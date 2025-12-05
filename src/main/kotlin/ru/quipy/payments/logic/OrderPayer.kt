package ru.quipy.payments.logic

import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Metrics
import jakarta.annotation.PostConstruct
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.RateLimitExceededException
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer(
    paymentAccounts: List<PaymentExternalSystemAdapter>
) {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val paymentExecutor = ThreadPoolExecutor(
        64,
        64,
        0,
        TimeUnit.SECONDS,
        LinkedBlockingQueue(1_000),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    private val scope = CoroutineScope(paymentExecutor.asCoroutineDispatcher() + SupervisorJob())

    @PostConstruct
    fun registerPoolSizeMetrics() {
        Gauge.builder("payment_executor_active_threads", paymentExecutor::getActiveCount)
            .description("Payment exec active threads")
            .register(Metrics.globalRegistry)
        Gauge.builder("payment_executor_total_threads", paymentExecutor::getPoolSize)
            .description("Payment exec total threads")
            .register(Metrics.globalRegistry)
    }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()

        if (paymentExecutor.queue.remainingCapacity() == 0) {
            throw RateLimitExceededException(1000)
        }

        scope.launch {
            val createdEvent = paymentESService.create {
                it.create(
                    paymentId,
                    orderId,
                    amount
                )
            }
            logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

            paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline, coroutineContext)
        }

        return createdAt
    }
}
