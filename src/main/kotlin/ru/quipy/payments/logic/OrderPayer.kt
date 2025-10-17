package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.web.client.HttpClientErrorException
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.Future
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.RejectedExecutionHandler
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

    private val paymentExecutor = ThreadPoolExecutor(
        16,
        16,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(320),
        NamedThreadFactory("payment-submission-executor"),
        ThrowingDiscardOldestPolicy()
    )

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()
        val task = {
            val createdEvent = paymentESService.create {
                it.create(
                    paymentId,
                    orderId,
                    amount
                )
            }
            logger.trace("Payment {} for order {} created.", createdEvent.paymentId, orderId)

            paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
        }

        try {
            val future = paymentExecutor.submit(task)
            if (future.get()) {
                return createdAt
            }
        } catch (_: RejectedExecutionException) {
            throw HttpClientErrorException(HttpStatus.TOO_MANY_REQUESTS, "Payment queue full")
        }

        throw HttpClientErrorException(HttpStatus.TOO_MANY_REQUESTS)
    }
}

class ThrowingDiscardOldestPolicy : RejectedExecutionHandler {
    override fun rejectedExecution(r: Runnable, executor: ThreadPoolExecutor) {
        if (!executor.isShutdown) {
            val removed = executor.queue.poll()
            OrderPayer.logger.warn(
                "Queue full — dropped oldest payment task: {}. Rejecting new task.",
                removed?.javaClass?.simpleName ?: "unknown"
            )
        }

        throw RejectedExecutionException("Payment queue full — rejecting new task.")
    }
}