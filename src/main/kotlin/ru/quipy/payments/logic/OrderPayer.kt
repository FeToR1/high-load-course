package ru.quipy.payments.logic

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Metrics
import jakarta.annotation.PostConstruct
import kotlinx.coroutines.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.RateLimitExceededException
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Instant
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer(
    paymentAccounts: List<PaymentExternalSystemAdapter>,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentService: PaymentService,
    @Qualifier("eventSourcingDispatcher")
    private val esDispatcher: ExecutorCoroutineDispatcher
) {

    private val processTime = paymentAccounts[0].averageProcessingTime().toMillis()

    private val paymentExecutor = ThreadPoolExecutor(
        THREAD_POOL_SIZE,
        THREAD_POOL_SIZE,
        0,
        TimeUnit.SECONDS,
        LinkedBlockingQueue(4000),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    private val scope = CoroutineScope(paymentExecutor.asCoroutineDispatcher())

    @PostConstruct
    fun registerPoolSizeMetrics() {
        Gauge.builder("payment_executor_active_threads", paymentExecutor::getActiveCount)
            .description("Payment exec active threads")
            .register(Metrics.globalRegistry)
        Gauge.builder("payment_executor_total_threads", paymentExecutor::getPoolSize)
            .description("Payment exec total threads")
            .register(Metrics.globalRegistry)
    }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Instant): Long {
        val createdAt = System.currentTimeMillis()

        if (paymentExecutor.queue.remainingCapacity() == 0) {
            throw RateLimitExceededException(1000)
        }

        scope.launch {
            val createdEvent = withContext(esDispatcher) {
                paymentESService.create {
                    it.create(
                        paymentId,
                        orderId,
                        amount
                    )
                }
            }
            logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

            paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
        }

        return createdAt
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
        const val THREAD_POOL_SIZE = 64
    }
}
