package ru.quipy.common.utils

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

class LeakingBucketRateLimiter(
    private val rate: Long,
    private val window: Duration,
    bucketSize: Int,
) : RateLimiter {
    private val rateLimiterScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())
    private val monitoringScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())

    private val queue = LinkedBlockingQueue<Int>(bucketSize)
    private val isFirstRequest = AtomicBoolean(true)
    private val requestsProcessed = AtomicLong(0)

    init {
        rateLimiterScope.launch {
            val delayBetweenRequests = window.toMillis().toDouble() / rate
            while (true) {
                if (queue.poll() != null) {
                    requestsProcessed.incrementAndGet()
                }
                delay(delayBetweenRequests.toLong())
            }
        }.invokeOnCompletion { th -> if (th != null) logger.error("Rate limiter release job completed", th) }

        // Поток для мониторинга RPS выхода из ведра каждые 5 секунд
        monitoringScope.launch {
            while (true) {
                delay(5000)
                val processed = requestsProcessed.getAndSet(0)
                val rps = processed / 5.0
                logger.info("Bucket outgoing RPS: $rps req/sec (${processed} requests in 5 seconds)")
            }
        }
    }

    override fun tick(): Boolean {
        if (isFirstRequest.compareAndSet(true, false)) {
            logger.info("First request received in LeakingBucketRateLimiter")
        }
        
        return queue.offer(1)
    }

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(LeakingBucketRateLimiter::class.java)
    }
}