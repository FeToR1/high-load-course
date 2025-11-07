package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.monitoring.MonitoringService
import ru.quipy.monitoring.RequestType
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import kotlin.math.pow


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val monitoringService: MonitoringService
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()

        const val RETRY_DELAY_BASE = 2.0
        const val RETRY_DELAY_COEFF = 0.225
        const val MAX_RETRIES = 5
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName

    private val client: OkHttpClient by lazy {
        val timeout = monitoringService.get90thPercentileTimeout()
        OkHttpClient.Builder()
            .callTimeout(timeout)
            .connectTimeout(timeout)
            .readTimeout(timeout)
            .writeTimeout(timeout)
            .build()
    }

    override fun performPayment(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val deadlineMs = deadline * 1000

        if (now() > deadlineMs) {
            monitoringService.increaseRequestsCounter(RequestType.PROCESSED_FAIL)
            return
        }

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        try {
            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()
            for (i in 1..MAX_RETRIES) {
                if (sendRequest(request, paymentId, transactionId)) {
                    break
                }

                if (i > 1) {
                    monitoringService.increaseRetryCounter()
                }

                val delay = (RETRY_DELAY_COEFF * RETRY_DELAY_BASE.pow(i)).toLong()

                if (deadlineMs < System.currentTimeMillis() + delay) {
                    monitoringService.increaseRequestsCounter(RequestType.PROCESSED_FAIL)
                    return
                }

                if (i < MAX_RETRIES) {
                    Thread.sleep(delay)
                }
            }
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
        }
    }

    fun sendRequest(request: Request, paymentId: UUID, transactionId: UUID): Boolean {
        val startTime = System.currentTimeMillis()
        
        client.newCall(request).execute().use { response ->
            monitoringService.increaseRequestsCounter(RequestType.OUTGOING)

            val body = try {
                mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
            } catch (e: Exception) {
                logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
            }

            logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

            val requestType = if (body.result) RequestType.PROCESSED_SUCCESS else RequestType.PROCESSED_FAIL
            monitoringService.increaseRequestsCounter(requestType)

            val duration = System.currentTimeMillis() - startTime
            monitoringService.recordRequestDuration(duration, body.result)

            // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
            // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
            paymentESService.update(paymentId) {
                it.logProcessing(body.result, now(), transactionId, reason = body.message)
            }

            return body.result
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun rateLimitPerSec() = properties.rateLimitPerSec

    override fun parallelRequests() = properties.parallelRequests

    override fun name() = properties.accountName

    override fun averageProcessingTime() = properties.averageProcessingTime

}

fun now() = System.currentTimeMillis()
