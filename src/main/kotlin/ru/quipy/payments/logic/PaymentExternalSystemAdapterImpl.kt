package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Metrics
import kotlinx.coroutines.*
import kotlinx.coroutines.future.await
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.monitoring.MonitoringService
import ru.quipy.monitoring.RequestType
import ru.quipy.payments.api.PaymentAggregate
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpConnectTimeoutException
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.http.HttpTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.ThreadPoolExecutor
import kotlin.math.pow

// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val monitoringService: MonitoringService,
    private val ongoingWindow: OngoingWindow,
    private val rateLimiter: SlidingWindowRateLimiter,
    val esDispatcher: ExecutorCoroutineDispatcher
) : PaymentExternalSystemAdapter {

    private val scope = CoroutineScope(esDispatcher)

    companion object {
        val logger: Logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val mapper = ObjectMapper().registerKotlinModule()

        const val RETRY_DELAY_BASE = 2.0
        const val RETRY_DELAY_COEFF = 50
        const val MAX_DELAY_MS = 10L
        const val MAX_RETRIES = 2
        const val MAX_ATTEMPTS = MAX_RETRIES + 1
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName

    private val httpClientExecutor = Executors.newFixedThreadPool(15)

    private val client: HttpClient by lazy {
        HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_2)
            .executor(httpClientExecutor)
            .connectTimeout(monitoringService.get90thPercentileTimeout(accountName))
            .build()
    }

    init {
        Gauge.builder("http_client_active_connections", (httpClientExecutor as ThreadPoolExecutor)::getActiveCount)
            .description("Http client active connections")
            .register(Metrics.globalRegistry)
        Gauge.builder("http_client_total_connections", httpClientExecutor::getPoolSize)
            .description("Http client idle connections")
            .register(Metrics.globalRegistry)
    }

    override suspend fun performPayment(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long
    ) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        scope.launch {
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }
        }

        val request = HttpRequest.newBuilder()
            .uri(URI.create("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"))
            .POST(HttpRequest.BodyPublishers.noBody())
            .timeout(Duration.ofSeconds(40))
            .build()

        ongoingWindow.acquireAsync()
        try {
            sendRequest(request, paymentId, transactionId, deadline * 1000, esDispatcher)
        } finally {
            ongoingWindow.release()
        }
    }

    suspend fun sendRequest(
        request: HttpRequest,
        paymentId: UUID,
        transactionId: UUID,
        deadlineMs: Long,
        esDispatcher: CoroutineDispatcher
    ) {
        var lastResult: PaymentResult? = null

        for (attempt in 1..MAX_ATTEMPTS) {
            val retryNumber = attempt - 1
            val retryDelay = calculateDelay(retryNumber)

            if (retryNumber > 0) {
                monitoringService.increaseRetryCounter()
            }

            if (now() + retryDelay.toMillis() > deadlineMs) {
                logPaymentResult(paymentId, transactionId, false, "Deadline exceeded")
                monitoringService.increaseRequestsCounter(RequestType.PROCESSED_FAIL)
                return
            }

            if (retryDelay.toMillis() > 0) {
                delay(retryDelay.toMillis())
            }

            rateLimiter.tickAsync()
            val result = sendRequestReal(request, paymentId, transactionId, attempt)

            lastResult = result

            if (result.success) {
                logPaymentResult(paymentId, transactionId, result.paymentSucceeded, result.message)
                val requestType = if (result.paymentSucceeded) RequestType.PROCESSED_SUCCESS else RequestType.PROCESSED_FAIL
                monitoringService.increaseRequestsCounter(requestType)
                return
            }
        }

        // All attempts failed
        val reason = lastResult?.message ?: "All retry attempts failed"
        logPaymentResult(paymentId, transactionId, false, reason)
        monitoringService.increaseRequestsCounter(RequestType.PROCESSED_FAIL)
    }

    private fun logPaymentResult(
        paymentId: UUID,
        transactionId: UUID,
        succeeded: Boolean,
        reason: String?
    ) {
        scope.launch {
            paymentESService.update(paymentId) {
                it.logProcessing(succeeded, now(), transactionId, reason = reason)
            }
        }
    }

    private suspend fun sendRequestReal(
        request: HttpRequest,
        paymentId: UUID,
        transactionId: UUID,
        attempt: Int
    ): PaymentResult {
        try {
            val startTime = now()
            val response = client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).await()
            val duration = now() - startTime

            val body = try {
                mapper.readValue(response.body(), ExternalSysResponse::class.java)
            } catch (e: Exception) {
                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
            }

            monitoringService.increaseRequestsCounter(RequestType.OUTGOING)
            monitoringService.recordRequestDuration(duration, body.result)

            if (response.statusCode() in 200..299) {
                logger.info("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
                return PaymentResult(success = true, paymentSucceeded = body.result, message = body.message)
            }

            return PaymentResult(success = false, paymentSucceeded = false, message = "HTTP ${response.statusCode()}")
        } catch (e: HttpTimeoutException) {
            return PaymentResult(success = false, paymentSucceeded = false, message = "Request timeout")
        } catch (e: HttpConnectTimeoutException) {
            return PaymentResult(success = false, paymentSucceeded = false, message = "Connection timeout")
        } catch (e: Exception) {
            return PaymentResult(success = false, paymentSucceeded = false, message = e.message ?: "Unknown error")
        }
    }

    private fun calculateDelay(retryNumber: Int): Duration {
        val durationMs = if (retryNumber == 0) {
            0L
        } else {
            minOf((RETRY_DELAY_COEFF * RETRY_DELAY_BASE.pow(retryNumber - 1)).toLong(), MAX_DELAY_MS)
        }
        return Duration.ofMillis(durationMs)
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun rateLimitPerSec() = properties.rateLimitPerSec

    override fun parallelRequests() = properties.parallelRequests

    override fun name() = properties.accountName

    override fun averageProcessingTime() = properties.averageProcessingTime
}

data class PaymentResult(
    val success: Boolean,
    val paymentSucceeded: Boolean,
    val message: String?
)

fun now() = System.currentTimeMillis()
