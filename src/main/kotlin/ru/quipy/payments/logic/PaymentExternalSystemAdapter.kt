package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
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
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.time.Instant
import java.util.*
import kotlin.math.pow

class PaymentExternalSystemAdapter(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val monitoringService: MonitoringService,
    private val ongoingWindow: OngoingWindow,
    private val rateLimiter: SlidingWindowRateLimiter,
    esDispatcher: ExecutorCoroutineDispatcher
) {

    private val scope = CoroutineScope(esDispatcher)

    companion object {
        val logger: Logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val mapper = ObjectMapper().registerKotlinModule()

        const val RETRY_DELAY_BASE = 2.0
        const val RETRY_DELAY_COEFF = 25
        const val MAX_RETRIES = 3
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName

    private val client: HttpClient by lazy {
        HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_2)
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
            .uri(URI.create("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"))
            .POST(HttpRequest.BodyPublishers.noBody())
            .timeout(Duration.ofSeconds(40))
            .build()

        sendRequest(request, paymentId, transactionId, deadline)
    }

    suspend fun sendRequest(
        request: HttpRequest,
        paymentId: UUID,
        transactionId: UUID,
        deadline: Instant
    ) {
        for (i in 1..MAX_RETRIES) {
            val requestDelay = calculateDelay(i)

            if (i > 1) {
                monitoringService.increaseRetryCounter()
            }
            if (now().plus(requestDelay) > deadline) {
                logger.error("[$accountName] Payment deadline exceeded for txId: $transactionId, payment: $paymentId")
                scope.launch {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now().toEpochMilli(), transactionId, reason = "Deadline exceeded")
                    }
                }
                monitoringService.increaseRequestsCounter(RequestType.PROCESSED_FAIL)
                return
            }

            if (requestDelay > Duration.ZERO) {
                logger.warn("[$accountName] RETRY attempt $i after ${requestDelay}ms delay")
                delay(requestDelay)
            }

            try {
                rateLimiter.tickAsync()
                ongoingWindow.acquireAsync()
                val startTime = now()
                val response = client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).await()
                val duration = Duration.between(startTime, now()).toMillis()

                val body = try {
                    mapper.readValue(response.body(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] Failed to parse response for txId: $transactionId, payment: $paymentId, result code: ${response.statusCode()}, reason: ${response.body()}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }

                monitoringService.increaseRequestsCounter(RequestType.OUTGOING)
                monitoringService.recordRequestDuration(duration, body.result)

                if (response.statusCode() in 200..299) {
                    scope.launch {
                        paymentESService.update(paymentId) {
                            it.logProcessing(body.result, now().toEpochMilli(), transactionId, reason = body.message)
                        }
                    }
                    val requestType = if (body.result) RequestType.PROCESSED_SUCCESS else RequestType.PROCESSED_FAIL
                    monitoringService.increaseRequestsCounter(requestType)
                    return
                }

                logger.warn("[$accountName] Non-success status ${response.statusCode()} for txId: $transactionId, attempt $i")

            } catch (e: Exception) {
                logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
            } finally {
                ongoingWindow.release()
            }
        }

        logger.error("[$accountName] All retry attempts exhausted for txId: $transactionId, payment: $paymentId")
        scope.launch {
            paymentESService.update(paymentId) {
                it.logProcessing(false, now().toEpochMilli(), transactionId, reason = "All retry attempts failed")
            }
        }
        monitoringService.increaseRequestsCounter(RequestType.PROCESSED_FAIL)
    }

    private fun calculateDelay(i: Int): Duration {
        val durationMs = if (i == 1) 0L else (RETRY_DELAY_COEFF * RETRY_DELAY_BASE.pow(i - 1)).toLong()
        return Duration.ofMillis(durationMs)
    }

    fun rateLimitPerSec() = properties.rateLimitPerSec

    fun parallelRequests() = properties.parallelRequests

    fun averageProcessingTime() = properties.averageProcessingTime
}

fun now(): Instant = Instant.now()
