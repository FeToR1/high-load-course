package ru.quipy.apigateway

import org.slf4j.LoggerFactory
import org.springframework.web.bind.annotation.*
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.common.utils.RateLimitExceededException
import ru.quipy.monitoring.MonitoringService
import ru.quipy.monitoring.RequestType
import ru.quipy.orders.repository.OrderRepository
import ru.quipy.payments.logic.OrderPayer
import ru.quipy.payments.logic.PaymentExternalSystemAdapter
import java.time.Duration
import java.util.*
import kotlin.math.min

@RestController
class APIController(
    paymentAccounts: List<PaymentExternalSystemAdapter>,
    private val orderRepository: OrderRepository,
    private val orderPayer: OrderPayer,
    private val monitoringService: MonitoringService
) {
    @Volatile
    private var bucket: LeakingBucketRateLimiter? = null
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
    fun payOrder(@PathVariable orderId: UUID, @RequestParam deadline: Long): PaymentSubmissionDto {
        monitoringService.increaseRequestsCounter(RequestType.INCOMING)

        initBucketOnce(deadline)

        if (!bucket!!.tick()) {
            val processTime = account.averageProcessingTime().toMillis()
            throw RateLimitExceededException(5 * processTime)
        }

        val paymentId = UUID.randomUUID()
        val order = orderRepository.findById(orderId)?.let {
            orderRepository.save(it.copy(status = OrderStatus.PAYMENT_IN_PROGRESS))
            it
        } ?: throw IllegalArgumentException("No such order $orderId")

        val createdAt = orderPayer.processPayment(orderId, order.price, paymentId, deadline)
        return PaymentSubmissionDto(createdAt, paymentId)
    }

    private fun initBucketOnce(deadlineSeconds: Long) {
        if (bucket != null) {
            return
        }

        synchronized(bucketLock) {
            if (bucket != null) {
                return
            }

            val ttl: Double = (deadlineSeconds - System.currentTimeMillis()).toDouble() / 1000
            val averageProcessingTimeSeconds: Double = account.averageProcessingTime().toSeconds().toDouble() * 2

            val effectiveRps: Double = min(
                account.rateLimitPerSec().toDouble(),
                account.parallelRequests().toDouble() / averageProcessingTimeSeconds
            )

            val bucketSize: Int = (effectiveRps * (ttl - averageProcessingTimeSeconds)).toInt()

            logger.debug("Leaking bucket size: $bucketSize")

            bucket = LeakingBucketRateLimiter(
                account.rateLimitPerSec().toLong(),
                Duration.ofSeconds(1),
                bucketSize
            )
        }
    }

    class PaymentSubmissionDto(
        val timestamp: Long,
        val transactionId: UUID
    )
}
