package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Metrics
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.retry.annotation.Backoff
import org.springframework.retry.annotation.Recover
import org.springframework.retry.annotation.Retryable
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.monitoring.MonitoringService
import ru.quipy.monitoring.RequestType
import ru.quipy.payments.api.PaymentAggregate
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.ThreadPoolExecutor

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val monitoringService: MonitoringService,
    private val ongoingWindow: OngoingWindow,
    private val rateLimiter: SlidingWindowRateLimiter,
    esDispatcher: ExecutorCoroutineDispatcher
) : PaymentExternalSystemAdapter {

    private val scope = CoroutineScope(esDispatcher)

    companion object {
        val logger: Logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val mapper = ObjectMapper().registerKotlinModule()

        const val RETRY_DELAY_BASE = 2.0
        const val RETRY_DELAY_COEFF = 0.225
        const val MAX_RETRIES = 3
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
            sendRequest(request, paymentId, transactionId, deadline * 1000)
        } finally {
            ongoingWindow.release()
        }
    }

    @Retryable(
        value = [Exception::class],
        maxAttempts = MAX_RETRIES,
        backoff = Backoff(delay = (RETRY_DELAY_COEFF * 1000).toLong(), multiplier = RETRY_DELAY_BASE)
    )
    private suspend fun sendRequest(
        request: HttpRequest,
        paymentId: UUID,
        transactionId: UUID,
        deadlineMs: Long
    ): ExternalSysResponse {
        val startTime = now()

        if (startTime > deadlineMs) {
            logger.error("[$accountName] [ERROR] Payment deadline exceeded for txId: $transactionId, payment: $paymentId")
            scope.launch {
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded")
                }
            }
            monitoringService.increaseRequestsCounter(RequestType.PROCESSED_FAIL)
            return ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, "Deadline exceeded")
        }

        rateLimiter.tickAsync()
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
            scope.launch {
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
            }
            val requestType = if (body.result) RequestType.PROCESSED_SUCCESS else RequestType.PROCESSED_FAIL
            monitoringService.increaseRequestsCounter(requestType)
        } else {
            logger.warn("[$accountName] Non-success status ${response.statusCode()} for txId: $transactionId")
            throw RuntimeException("Non-success status")
        }

        return body
    }

    @Recover
    private fun recoverSendRequest(
        e: Exception,
        request: HttpRequest,
        paymentId: UUID,
        transactionId: UUID
    ): ExternalSysResponse {
        logger.error("[$accountName] [ERROR] All retry attempts exhausted for txId: $transactionId, payment: $paymentId")
        scope.launch {
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "All retry attempts failed")
            }
        }
        monitoringService.increaseRequestsCounter(RequestType.PROCESSED_FAIL)
        return ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, "All retry attempts failed")
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun rateLimitPerSec() = properties.rateLimitPerSec

    override fun parallelRequests() = properties.parallelRequests

    override fun name() = properties.accountName

    override fun averageProcessingTime() = properties.averageProcessingTime
}

fun now() = System.currentTimeMillis()
