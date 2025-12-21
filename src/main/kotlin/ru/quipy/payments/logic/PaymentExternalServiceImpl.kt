package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Metrics
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.monitoring.MonitoringService
import ru.quipy.monitoring.RequestType
import ru.quipy.payments.api.PaymentAggregate
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.http.HttpTimeoutException
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CompletionException
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
    private val rateLimiter: SlidingWindowRateLimiter
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val mapper = ObjectMapper().registerKotlinModule()

        const val RETRY_DELAY_BASE = 2.0
        const val RETRY_DELAY_COEFF = 0.1
        const val MAX_RETRIES = 3
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName

    private val httpClientExecutor = Executors.newFixedThreadPool(100)

    private val client: HttpClient by lazy {
        HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_2)
            .executor(httpClientExecutor)
            //.connectTimeout(monitoringService.get90thPercentileTimeout(accountName))
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
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val request = HttpRequest.newBuilder()
            .uri(URI.create("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"))
            .POST(HttpRequest.BodyPublishers.noBody())
            .timeout(Duration.ofSeconds(40))
            .build()

        // ongoingWindow.acquireAsync()
        try {
            sendRequest(request, paymentId, transactionId, deadline * 1000)
        } finally {
            // ongoingWindow.release()
        }
    }

    suspend fun sendRequest(
        request: HttpRequest,
        paymentId: UUID,
        transactionId: UUID,
        deadlineMs: Long
    ) {
        for (i in 1..MAX_RETRIES) {
            // rateLimiter.acquireAsync()
            val delayMs = if (i == 1) 0L else (RETRY_DELAY_COEFF * RETRY_DELAY_BASE.pow(i - 1) * 1000).toLong()
            if (i > 1) {
                monitoringService.increaseRetryCounter()
            }
            if (now() + delayMs > deadlineMs) {
                logger.error("[$accountName] [ERROR] Payment deadline exceeded for txId: $transactionId, payment: $paymentId")
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded")
                }
                monitoringService.increaseRequestsCounter(RequestType.PROCESSED_FAIL)
                return
            }

            if (delayMs > 0) {
                logger.warn("[$accountName] RETRY attempt $i after ${delayMs}ms delay")
                delay(delayMs)
            }

            try {
                val startTime = now()
                val response = client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).await()
                val duration = now() - startTime
                
                val body = try {
                    mapper.readValue(response.body(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Failed to parse response for txId: $transactionId, payment: $paymentId, result code: ${response.statusCode()}, reason: ${response.body()}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }
                
                monitoringService.increaseRequestsCounter(RequestType.OUTGOING)
                monitoringService.recordRequestDuration(duration, body.result)

                if (response.statusCode() in 200..299) {
                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                    val requestType = if (body.result) RequestType.PROCESSED_SUCCESS else RequestType.PROCESSED_FAIL
                    monitoringService.increaseRequestsCounter(requestType)
                    return
                }
                logger.warn("[$accountName] Non-success status ${response.statusCode()} for txId: $transactionId, attempt $i")
                
            } catch (e: Exception) {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                }
            }
        }

        logger.error("[$accountName] [ERROR] All retry attempts exhausted for txId: $transactionId, payment: $paymentId")
        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), transactionId, reason = "All retry attempts failed")
        }
        monitoringService.increaseRequestsCounter(RequestType.PROCESSED_FAIL)
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun rateLimitPerSec() = properties.rateLimitPerSec

    override fun parallelRequests() = properties.parallelRequests

    override fun name() = properties.accountName

    override fun averageProcessingTime() = properties.averageProcessingTime
}

fun now() = System.currentTimeMillis()
