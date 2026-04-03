package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
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
import java.net.http.*
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.coroutines.cancellation.CancellationException

// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val monitoringService: MonitoringService,
    private val ongoingWindow: OngoingWindow,
    private val rateLimiter: SlidingWindowRateLimiter,
    private val dbScope: CoroutineScope
) : PaymentExternalSystemAdapter {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName

    private val httpClientExecutor = Executors.newFixedThreadPool(15)

    private val circuitBreaker: CircuitBreaker by lazy {
        val config = CircuitBreakerConfig.custom()
            .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
            .slidingWindowSize(40)
            .failureRateThreshold(70f)
            .waitDurationInOpenState(Duration.ofSeconds(1))
            .build()

        CircuitBreaker.of("payment-service-$accountName", config).apply {
            eventPublisher.onSuccess { _ ->
                logger.debug("[$accountName] Circuit breaker recorded success")
            }
            eventPublisher.onError { event ->
                logger.warn("[$accountName] Circuit breaker recorded error: ${event.throwable.message}")
            }
            eventPublisher.onStateTransition { event ->
                logger.warn("[$accountName] Circuit breaker state transition: ${event.stateTransition}")
            }
        }
    }

    private val client: HttpClient by lazy {
        HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_2)
            .executor(httpClientExecutor)
            .connectTimeout(monitoringService.get90thPercentileTimeout(accountName))
            .build()
    }

    override suspend fun performPayment(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Instant
    ) = coroutineScope {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        val request = HttpRequest.newBuilder()
            .uri(URI.create("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"))
            .POST(HttpRequest.BodyPublishers.noBody())
            .timeout(Duration.ofSeconds(40))
            .header("x-idempotency-key", "$transactionId")
            .build()

        sendRequest(request, paymentId, transactionId, deadline, paymentStartedAt)
    }

    suspend fun sendRequest(
        request: HttpRequest,
        paymentId: UUID,
        transactionId: UUID,
        deadline: Instant,
        paymentStartedAt: Long
    ) {
        // Логируем каждую попытку отправки запроса
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования
        dbScope.launch {
            paymentESService.update(paymentId) {
                it.logSubmission(
                    success = true,
                    transactionId,
                    now().toEpochMilli(),
                    Duration.ofMillis(now().toEpochMilli() - paymentStartedAt)
                )
            }
        }

        while (circuitBreaker.state == CircuitBreaker.State.OPEN) {
            if (now() > deadline) {
                logPaymentResult(
                    paymentId,
                    transactionId,
                    false,
                    "Deadline exceeded while waiting for circuit breaker to close"
                )
                monitoringService.increaseRequestsCounter(RequestType.PROCESSED_FAIL)
                return
            }
            delay(10)
        }

        rateLimiter.tickAsync()
        ongoingWindow.acquireAsync()
        val result = try {
            sendRequestReal(request, paymentId, transactionId, paymentStartedAt)
        } finally {
            ongoingWindow.release()
        }

        val requestType = if (result.success && result.paymentSucceeded) {
            RequestType.PROCESSED_SUCCESS
        } else {
            RequestType.PROCESSED_FAIL
        }

        logPaymentResult(paymentId, transactionId, result.paymentSucceeded, result.message)
        monitoringService.increaseRequestsCounter(requestType)
    }

    private fun logPaymentResult(
        paymentId: UUID,
        transactionId: UUID,
        succeeded: Boolean,
        reason: String?
    ) {
        dbScope.launch {
            paymentESService.update(paymentId) {
                it.logProcessing(succeeded, now().toEpochMilli(), transactionId, reason = reason)
            }
        }
    }

    private suspend fun sendRequestReal(
        request: HttpRequest,
        paymentId: UUID,
        transactionId: UUID,
        paymentStartedAt: Long
    ): PaymentResult {
        // Check if circuit breaker allows the request
        if (!circuitBreaker.tryAcquirePermission()) {
            logger.warn("[$accountName] Circuit breaker is OPEN, rejecting request for payment $paymentId")
            return PaymentResult(success = false, paymentSucceeded = false, message = "Circuit breaker is open")
        }

        val startTime = now()

        try {
            val response = withContext(Dispatchers.IO) {
                client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).await()
            }

            val duration = Duration.between(startTime, now())

            val body = try {
                mapper.readValue(response.body(), ExternalSysResponse::class.java)
            } catch (e: Exception) {
                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
            }

            monitoringService.increaseRequestsCounter(RequestType.OUTGOING)
            monitoringService.recordRequestDuration(duration.toMillis(), body.result)

            if (response.statusCode() in 200..299) {
                logger.info("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
                // Record success in circuit breaker
                circuitBreaker.onSuccess(duration.toNanos(), TimeUnit.NANOSECONDS)
                return PaymentResult(success = true, paymentSucceeded = body.result, message = body.message)
            }

            // Record error for non-2xx responses (5xx errors)
            val error = Exception("HTTP ${response.statusCode()}")
            circuitBreaker.onError(duration.toNanos(), TimeUnit.NANOSECONDS, error)
            return PaymentResult(success = false, paymentSucceeded = false, message = "HTTP ${response.statusCode()}")
        } catch (e: HttpTimeoutException) {
            val duration = Duration.between(startTime, now())
            circuitBreaker.onError(duration.toNanos(), TimeUnit.NANOSECONDS, e)
            return PaymentResult(success = false, paymentSucceeded = false, message = "Request timeout")
        } catch (e: HttpConnectTimeoutException) {
            val duration = Duration.between(startTime, now())
            circuitBreaker.onError(duration.toNanos(), TimeUnit.NANOSECONDS, e)
            return PaymentResult(
                success = false,
                paymentSucceeded = false,
                message = "Connection timeout",
                shouldRetry = false
            )
        } catch (e: Exception) {
            if (e is CancellationException) throw e

            val duration = Duration.between(startTime, now())
            circuitBreaker.onError(duration.toNanos(), TimeUnit.NANOSECONDS, e)
            return PaymentResult(success = false, paymentSucceeded = false, message = e.message ?: "Unknown error")
        }
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
    val message: String?,
    val shouldRetry: Boolean = true
)

fun now() = Instant.now()
