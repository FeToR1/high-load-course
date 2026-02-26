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
        val ttl = Duration.between(Instant.now(), deadline)

        val bucketSize = if (ttl < processingTime) {
            logger.warn("Request TTL (${ttl.toMillis()} ms) < average processing time, using default bucket size")
            DEFAULT_BUCKET_SIZE
        } else {
            calculateBucketSize(account, ttl, processingTime)
        }

        logger.info("Leaking bucket size: $bucketSize")

        return LeakingBucketRateLimiter(
            account.rateLimitPerSec().toLong(),
            Duration.ofSeconds(1),
            bucketSize
        )
    }

    private fun calculateTotalProcessingTime(account: PaymentExternalSystemAdapter): Duration {
        var avgTime = account.averageProcessingTime().toMillis()
        avgTime += APP_PROCESSING_TIME.toMillis()
        return Duration.ofMillis(avgTime)
    }

    private fun calculateBucketSize(
        account: PaymentExternalSystemAdapter,
        ttl: Duration,
        totalProcessingTime: Duration
    ): Int {
        // max time a request can stay in the queue
        val processingWaitLimit = ttl.minus(totalProcessingTime)

        val effectiveRps = min(
            account.rateLimitPerSec().toDouble(),
            account.parallelRequests().toDouble() / totalProcessingTime.toMillis() * 1000
        )

        logger.info("Effective RPS: $effectiveRps")

        return max(1, (effectiveRps * processingWaitLimit.toMillis() / 1000).toInt())
    }

    companion object {
        private const val DEFAULT_BUCKET_SIZE = 100
        private val logger = LoggerFactory.getLogger(LeakingBucketRateLimiterFactory::class.java)
        private val APP_PROCESSING_TIME = Duration.ofMillis(350)
    }
}