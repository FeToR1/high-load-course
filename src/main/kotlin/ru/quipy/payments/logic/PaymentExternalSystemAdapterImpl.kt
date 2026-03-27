package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Metrics
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.withContext
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
import java.time.Instant
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
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
    private val dbScope: CoroutineScope
) : PaymentExternalSystemAdapter {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val mapper = ObjectMapper().registerKotlinModule()

        const val RETRY_DELAY_BASE = 2.0
        const val RETRY_DELAY_COEFF = 50
        const val MAX_DELAY_MS = 100000000L
        const val MAX_RETRIES = 4
        const val MAX_ATTEMPTS = MAX_RETRIES + 1
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName

    private val httpClientExecutor = Executors.newFixedThreadPool(15)

    private val circuitBreaker: CircuitBreaker by lazy {
        val config = CircuitBreakerConfig.custom()
            .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.TIME_BASED)
            .slidingWindowSize(3) // 3 seconds window
            .minimumNumberOfCalls(2) // Минимум 2 вызова - открываем при первой возможности
            .failureRateThreshold(30f) // 30% ошибок - очень чувствительно
            .waitDurationInOpenState(Duration.ofSeconds(10)) // 200ms - очень быстрое восстановление
            .permittedNumberOfCallsInHalfOpenState(1) // Только 1 тестовый запрос
            .automaticTransitionFromOpenToHalfOpenEnabled(true)
            .build()

        CircuitBreaker.of("payment-service-$accountName", config).apply {
            eventPublisher.onSuccess { event ->
                logger.debug("[$accountName] Circuit breaker recorded success")
            }
            eventPublisher.onError { event ->
                logger.warn("[$accountName] Circuit breaker recorded error: ${event.throwable?.message}")
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

        select {
            async { sendRequest(request, paymentId, transactionId, deadline, paymentStartedAt) }.onAwait {}
            // async { sendRequest(request, paymentId, transactionId, deadline, paymentStartedAt) }.onAwait {}
            // async { sendRequest(request, paymentId, transactionId, deadline, paymentStartedAt) }.onAwait {}
        }

        coroutineContext.cancelChildren()
    }

    suspend fun sendRequest(
        request: HttpRequest,
        paymentId: UUID,
        transactionId: UUID,
        deadline: Instant,
        paymentStartedAt: Long
    ) {
        var lastResult: PaymentResult? = null

        for (attempt in 1..MAX_ATTEMPTS) {
            val retryNumber = attempt - 1
            val retryDelay = calculateDelay(retryNumber)

            if (retryNumber > 0) {
                monitoringService.increaseRetryCounter()
            }

            if (now().plus(retryDelay) > deadline) {
                dbScope.launch {
                    paymentESService.update(paymentId) {
                        it.logSubmission(success = true, transactionId, now().toEpochMilli(), Duration.ofMillis(now().toEpochMilli() - paymentStartedAt))
                    }
                }
                logPaymentResult(paymentId, transactionId, false, "Deadline exceeded")
                monitoringService.increaseRequestsCounter(RequestType.PROCESSED_FAIL)
                return
            }

            if (retryDelay > Duration.ZERO) {
                delay(retryDelay.toMillis())
            }

            rateLimiter.tickAsync()
            ongoingWindow.acquireAsync()
            val result = try {
                sendRequestReal(request, paymentId, transactionId, paymentStartedAt)
            } finally {
                ongoingWindow.release()
            }

            lastResult = result

            if (result.success) {
                logPaymentResult(paymentId, transactionId, result.paymentSucceeded, result.message)
                val requestType = if (result.paymentSucceeded) RequestType.PROCESSED_SUCCESS else RequestType.PROCESSED_FAIL
                monitoringService.increaseRequestsCounter(requestType)
                return
            }

            if (!result.shouldRetry) {
                break
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
            return PaymentResult(success = false, paymentSucceeded = false, message = "Circuit breaker is open", shouldRetry = false)
        }

        // Логируем каждую попытку отправки запроса
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования
        dbScope.launch {
            paymentESService.update(paymentId) {
                it.logSubmission(
                    success = true,
                    transactionId,
                    now().toEpochMilli(),
                    Duration.ofMillis(now().toEpochMilli() - paymentStartedAt))
            }
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
            return PaymentResult(success = false, paymentSucceeded = false, message = "Connection timeout")
        } catch (e: Exception) {
            val duration = Duration.between(startTime, now())
            circuitBreaker.onError(duration.toNanos(), TimeUnit.NANOSECONDS, e)
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
    val message: String?,
    val shouldRetry: Boolean = true
)

fun now() = Instant.now()
