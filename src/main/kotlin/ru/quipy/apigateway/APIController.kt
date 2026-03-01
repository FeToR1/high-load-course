package ru.quipy.apigateway

import org.slf4j.LoggerFactory
import org.springframework.web.bind.annotation.*
import ru.quipy.common.utils.RateLimitExceededException
import ru.quipy.common.utils.RateLimiter
import ru.quipy.common.utils.RateLimiterFactory
import ru.quipy.monitoring.MonitoringService
import ru.quipy.monitoring.RequestType
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.logic.OrderPayer
import ru.quipy.payments.logic.PaymentExternalSystemAdapter
import java.time.Instant
import java.util.*

@RestController
class APIController(
    paymentAccounts: List<PaymentExternalSystemAdapter>,
    private val orderRepository: OrderRepository,
    private val orderPayer: OrderPayer,
    private val monitoringService: MonitoringService,
    private val rateLimiterFactory: RateLimiterFactory
) {
    @Volatile
    private var bucket: RateLimiter? = null
    private val account = paymentAccounts[0]
    private val bucketLock = Any()

    @PostMapping("/users")
    fun createUser(@RequestBody req: CreateUserRequest): User {
        return User(UUID.randomUUID(), req.name)
    }

    private val logger = LoggerFactory.getLogger(APIController::class.java)

    data class CreateUserRequest(val name: String, val password: String)

    data class User(val id: UUID, val name: String)

    @PostMapping("/orders")
    fun createOrder(@RequestParam userId: UUID, @RequestParam price: Int): Order {
        val order = Order(
            UUID.randomUUID(),
            userId,
            System.currentTimeMillis(),
            OrderStatus.COLLECTING,
            price,
        )
        return orderRepository.save(order)
    }

    data class Order(
        val id: UUID,
        val userId: UUID,
        val timeCreated: Long,
        val status: OrderStatus,
        val price: Int,
    )

    enum class OrderStatus {
        COLLECTING,
        PAYMENT_IN_PROGRESS,
        PAID,
    }

    @PostMapping("/orders/{orderId}/payment")
    fun payOrder(
        @PathVariable orderId: UUID,
        @RequestParam("deadline") deadlineTimestampMs: Long
    ): PaymentSubmissionDto {
        monitoringService.increaseRequestsCounter(RequestType.INCOMING)

        val deadline = Instant.ofEpochMilli(deadlineTimestampMs)

        initBucketOnce(deadline)

        if (!bucket!!.tick()) {
            val processTime = account.averageProcessingTime().toMillis()
            throw RateLimitExceededException(processTime * 5)
        }

        val paymentId = UUID.randomUUID()
        val order = orderRepository.findById(orderId)?.let {
            orderRepository.save(it.copy(status = OrderStatus.PAYMENT_IN_PROGRESS))
            it
        } ?: throw IllegalArgumentException("No such order $orderId")

        val createdAt = orderPayer.processPayment(orderId, order.price, paymentId, deadline)
        return PaymentSubmissionDto(createdAt, paymentId)
    }

    private fun initBucketOnce(deadline: Instant) {
        if (bucket != null) {
            return
        }

        synchronized(bucketLock) {
            if (bucket != null) {
                return
            }

            bucket = rateLimiterFactory.createForAccount(account, deadline)
        }
    }

    class PaymentSubmissionDto(
        val timestamp: Long,
        val transactionId: UUID
    )
}
