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
        // Вычисляем максимальное время на ретраи (25 * 2^1 + 25 * 2^2 + 25 * 2^3 = 350ms)
        val maxRetryTime = calculateMaxRetryDelay()
        
        // Максимальное время ожидания в очереди = TTL - время обработки - запас на ретраи
        val maxQueueWaitTime = ttl.minus(processingTime).minus(maxRetryTime)

        if (maxQueueWaitTime.isNegative || maxQueueWaitTime.isZero) {
            logger.warn("No time for queue wait: ttl=${ttl.toMillis()}ms, processingTime=${processingTime.toMillis()}ms, maxRetryTime=${maxRetryTime.toMillis()}ms")
            return 1
        }

        val effectiveRps = min(
            account.rateLimitPerSec().toDouble(),
            account.parallelRequests().toDouble() / processingTime.toMillis() * 1000
        )

        // Размер ведра = сколько запросов можно накопить за maxQueueWaitTime
        val calculatedSize = (effectiveRps * maxQueueWaitTime.toMillis() / 1000).toInt()
        
        logger.info("Bucket calculation: effectiveRps=$effectiveRps, maxQueueWaitTime=${maxQueueWaitTime.toMillis()}ms, calculatedSize=$calculatedSize")
        
        return max(1, calculatedSize)
    }
    
    private fun calculateMaxRetryDelay(): Duration {
        // Формула из PaymentExternalSystemAdapter: 25 * 2^retryNumber
        // Для 3 ретраев: 25*2 + 25*4 + 25*8 = 50 + 100 + 200 = 350ms
        val totalDelayMs = (1..MAX_RETRIES).sumOf { retryNumber ->
            (RETRY_DELAY_COEFF * Math.pow(RETRY_DELAY_BASE, retryNumber.toDouble())).toLong()
        }
        return Duration.ofMillis(totalDelayMs)
    }

    companion object {
        private const val DEFAULT_BUCKET_SIZE = 100
        private val logger = LoggerFactory.getLogger(LeakingBucketRateLimiterFactory::class.java)
        private val APP_PROCESSING_TIME = Duration.ofMillis(350)
        
        // Константы из PaymentExternalSystemAdapter для расчета времени ретраев
        private const val RETRY_DELAY_BASE = 2.0
        private const val RETRY_DELAY_COEFF = 25
        private const val MAX_RETRIES = 3
    }
}