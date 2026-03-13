package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.kotlin.ratelimiter.executeSuspendFunction
import io.github.resilience4j.ratelimiter.RateLimiter
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.time.delay
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

    private val dispatcherPayment = Executors.newFixedThreadPool(60).asCoroutineDispatcher()

    companion object {
        val logger: Logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val mapper = ObjectMapper().registerKotlinModule()

        const val RETRY_DELAY_BASE = 2.0
        const val RETRY_DELAY_COEFF = 50
        const val MAX_DELAY_MS = 10L
        const val MAX_RETRIES = 2
        const val MAX_ATTEMPTS = MAX_RETRIES + 1
    }

    private val client: HttpClient by lazy {
        HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_2)
            .connectTimeout(Duration.ofMillis(200))
            .build()
    }

    suspend fun performPayment(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
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
        val accountName = properties.accountName

        for (attempt in 1..MAX_ATTEMPTS) {
            val retryNumber = attempt - 1
            val retryDelay = calculateDelay(retryNumber)

            if (retryNumber > 0) {
                monitoringService.increaseRetryCounter()
            }

            if (now().plus(retryDelay) > deadline) {
                dbScope.launch {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now().toEpochMilli(), transactionId, reason = "Deadline exceeded")
                    }
                }
                monitoringService.increaseRequestsCounter(RequestType.PROCESSED_FAIL)
                return
            }

            if (retryDelay > Duration.ZERO) {
                delay(retryDelay)
            }

            ongoingWindow.withPermit {
                rateLimiter.executeSuspendFunction {
                    sendRequestReal(request, paymentId, transactionId, attempt)
                }
            }
        }

        dbScope.launch {
            paymentESService.update(paymentId) {
                it.logProcessing(false, now().toEpochMilli(), transactionId, reason = "All retry attempts failed")
            }
        }
        monitoringService.increaseRequestsCounter(RequestType.PROCESSED_FAIL)
    }

    private suspend fun sendRequestReal(
        request: HttpRequest,
        paymentId: UUID,
        transactionId: UUID,
        i: Int
    ) {
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
                logger.info("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
                dbScope.launch {
                    paymentESService.update(paymentId) {
                        it.logProcessing(
                            body.result,
                            now().toEpochMilli(),
                            transactionId,
                            reason = body.message
                        )
                    }
                }
                val requestType =
                    if (body.result) RequestType.PROCESSED_SUCCESS else RequestType.PROCESSED_FAIL
                monitoringService.increaseRequestsCounter(requestType)
                
                if (body.result || body.message != "Temporary error") {
                    return
                }
            }
        } catch (e: HttpTimeoutException) {
            // Timeout handled silently
        } catch (e: HttpConnectTimeoutException) {
            // Connection timeout handled silently
        } catch (e: Exception) {
            // Exception handled silently
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

fun now(): Instant = Instant.now()
