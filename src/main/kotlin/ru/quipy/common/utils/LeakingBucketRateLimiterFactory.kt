package ru.quipy.common.utils

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import ru.quipy.payments.logic.PaymentExternalSystemAdapter
import java.time.Duration
import java.time.Instant
import kotlin.math.max
import kotlin.math.min

@Service
class LeakingBucketRateLimiterFactory : RateLimiterFactory {

    override fun createForAccount(
        account: PaymentExternalSystemAdapter,
        deadline: Instant
    ): LeakingBucketRateLimiter {
        val processingTime = calculateTotalProcessingTime(account)
        val ttl = Duration.ofMillis(1000)

        val bucketSize = if (ttl < processingTime) {
            DEFAULT_BUCKET_SIZE
        } else {
            calculateBucketSize(account, ttl, processingTime)
        }

        val effectiveRps = min(
            account.rateLimitPerSec().toDouble(),
            account.parallelRequests().toDouble() / processingTime.toMillis() * 1000
        )

        logger.info("Leaking bucket size: $bucketSize, Effective RPS: $effectiveRps")

        return LeakingBucketRateLimiter(
            effectiveRps.toLong(),
            Duration.ofSeconds(1),
            bucketSize
        )
    }

    private fun calculateTotalProcessingTime(account: PaymentExternalSystemAdapter): Duration {
        return account.averageProcessingTime().plus(APP_PROCESSING_TIME)
    }

    private fun calculateBucketSize(
        account: PaymentExternalSystemAdapter,
        ttl: Duration,
        processingTime: Duration
    ): Int {
        val processingWaitLimit = ttl.minus(processingTime)

        val effectiveRps = min(
            account.rateLimitPerSec().toDouble(),
            account.parallelRequests().toDouble() / processingTime.toMillis() * 1000
        )

        return max(1, (effectiveRps * processingWaitLimit.toMillis() / 1000).toInt())
    }

    companion object {
        private const val DEFAULT_BUCKET_SIZE = 100
        private val logger = LoggerFactory.getLogger(LeakingBucketRateLimiterFactory::class.java)
        private val APP_PROCESSING_TIME = Duration.ofMillis(350)
    }
}