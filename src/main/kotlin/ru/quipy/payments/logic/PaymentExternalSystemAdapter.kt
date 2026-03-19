package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.kotlin.ratelimiter.executeSuspendFunction
import io.github.resilience4j.ratelimiter.RateLimiter
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.time.delay
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.monitoring.MonitoringService
import ru.quipy.monitoring.RequestType
import ru.quipy.payments.api.PaymentAggregate
import java.net.URI
import java.net.http.*
import java.time.Duration
import java.time.Instant
import java.time.Instant.now
import java.util.*
import kotlin.math.pow

class PaymentExternalSystemAdapter(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val monitoringService: MonitoringService,
    private val ongoingWindow: Semaphore,
    private val rateLimiter: RateLimiter,
    private val dbScope: CoroutineScope
) {
    companion object {
        val logger: Logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val mapper = ObjectMapper().registerKotlinModule()

        const val RETRY_DELAY_BASE = 2.0
        const val RETRY_DELAY_COEFF = 50
        const val MAX_DELAY_MS = 10L
        const val MAX_RETRIES = 0
        const val MAX_ATTEMPTS = MAX_RETRIES + 1
        const val MAX_LOG_COUNT = 500
        
        @Volatile
        private var logCounter = 0
        
        private fun shouldLog(): Boolean {
            return logCounter < MAX_LOG_COUNT
        }
        
        private fun incrementLogCounter() {
            if (logCounter < MAX_LOG_COUNT) {
                logCounter++
                if (logCounter == MAX_LOG_COUNT) {
                    logger.warn("Reached maximum log count ($MAX_LOG_COUNT). Suppressing further logs from PaymentExternalSystemAdapter.")
                }
            }
        }
    }

    private val client: HttpClient =
        HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_2)
            .connectTimeout(Duration.ofMillis(1000))
            .build()

    suspend fun performPayment(
        paymentId: UUID,
        amount: Int,
        deadline: Instant
    ) {
        val transactionId = UUID.randomUUID()

        val request = HttpRequest.newBuilder()
            .uri(URI.create("http://$paymentProviderHostPort/external/process?serviceName=${properties.serviceName}&token=$token&accountName=${properties.accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"))
            .POST(HttpRequest.BodyPublishers.noBody())
            .timeout(monitoringService.get90thPercentileTimeout(properties.accountName))
            .build()

        sendRequest(request, paymentId, transactionId, deadline)
    }

    suspend fun sendRequest(
        request: HttpRequest,
        paymentId: UUID,
        transactionId: UUID,
        deadline: Instant
    ) {
        var lastResult: PaymentResult? = null

        for (attempt in 1..MAX_ATTEMPTS) {
            val retryNumber = attempt - 1
            val retryDelay = calculateDelay(retryNumber)

            if (retryNumber > 0) {
                monitoringService.increaseRetryCounter()
            }

            val now = now().plus(retryDelay)

            if (now > deadline) {
                logPaymentResult(paymentId, transactionId, false, "Deadline exceeded $deadline, now $now, retry number $retryNumber, retry delay $retryDelay")
                monitoringService.increaseRequestsCounter(RequestType.PROCESSED_FAIL)
                return
            }

            if (retryDelay > Duration.ZERO) {
                delay(retryDelay)
            }

            val result = ongoingWindow.withPermit {
                rateLimiter.executeSuspendFunction {
                    sendRequestReal(request, paymentId, transactionId)
                }
            }

            lastResult = result
            
            if (result.success) {
                logPaymentResult(paymentId, transactionId, result.paymentSucceeded, result.message)
                val requestType = if (result.paymentSucceeded) RequestType.PROCESSED_SUCCESS else RequestType.PROCESSED_FAIL
                monitoringService.increaseRequestsCounter(requestType)
                return
            } else {
                if (shouldLog()) {
                    logger.warn("fail: ${result.message}")
                    incrementLogCounter()
                }
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
        if (reason != null && shouldLog()) {
            logger.warn("fail ${reason}")
            incrementLogCounter()
        }

        dbScope.launch {
            paymentESService.update(paymentId) {
                it.logProcessing(succeeded, now().toEpochMilli(), transactionId, reason = reason)
            }
        }
    }

    private suspend fun sendRequestReal(
        request: HttpRequest,
        paymentId: UUID,
        transactionId: UUID
    ): PaymentResult {
        val accountName = properties.accountName

        try {
            val startTime = now()
            val response = client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).await()
            val duration = Duration.between(startTime, now()).toMillis()

            val body = try {
                mapper.readValue(response.body(), ExternalSysResponse::class.java)
            } catch (e: Exception) {
                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
            }

            monitoringService.increaseRequestsCounter(RequestType.OUTGOING)
            monitoringService.recordRequestDuration(duration, body.result)

            if (response.statusCode() in 200..299) {
                //logger.info("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}")
                return PaymentResult(success = true, paymentSucceeded = body.result, message = body.message)
            }

            return PaymentResult.paymentFailed("HTTP ${response.statusCode()}")
        } catch (_: HttpTimeoutException) {
            return PaymentResult.paymentFailed("Request timeout")
        } catch (_: HttpConnectTimeoutException) {
            return PaymentResult.paymentFailed("Connection timeout")
        } catch (e: Exception) {
            return PaymentResult.paymentFailed(e.message ?: "Unknown error")
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

    fun rateLimitPerSec() = properties.rateLimitPerSec
    fun parallelRequests() = properties.parallelRequests
    fun averageProcessingTime() = properties.averageProcessingTime
}

data class PaymentResult(
    val success: Boolean,           // true if request completed successfully (got 2xx response)
    val paymentSucceeded: Boolean,  // true if payment was actually successful
    val message: String?            // reason/message from external system or error
) {
    companion object {
        fun paymentFailed(message: String?) = PaymentResult(false, paymentSucceeded = false, message = message)
    }
}
