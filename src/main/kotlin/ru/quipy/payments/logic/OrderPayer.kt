package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private fun randomizeRetryAfter(minValue: Long, jitterFactor: Double = 2.0): Long {
        val jitter = (minValue * jitterFactor * Math.random()).toLong()
        return minValue + jitter
    }

    private val paymentExecutor = ThreadPoolExecutor(
        16,
        16,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(300),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()


        if (paymentExecutor.queue.remainingCapacity() == 0) {
            val a = randomizeRetryAfter(1000)
            throw TooManyRequestsWithRetryAfterException(a)
        }

        paymentExecutor.submit {
            val createdEvent = paymentESService.create {
                it.create(
                    paymentId,
                    orderId,
                    amount
                )
            }
            logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

            paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
        }
        return createdAt
    }

}

class TooManyRequestsWithRetryAfterException : Exception {
    private val retryAfter: Long
    constructor() : super() {
        retryAfter = 60000
    }
    constructor(retryAfter: Long) : super() {
        this.retryAfter = retryAfter
    }

    constructor(message: String, retryAfter: Long) : super(message) {
        this.retryAfter = retryAfter
    }

    constructor(message: String, cause: Throwable, retryAfter: Long) : super(message, cause) {
        this.retryAfter = retryAfter
    }

    constructor(cause: Throwable, retryAfter: Long) : super(cause) {
        this.retryAfter = retryAfter
    }

    fun getRetryAfter(): Long = this.retryAfter
}