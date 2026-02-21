package ru.quipy.common.utils

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.PriorityBlockingQueue

class SlidingWindowRateLimiter(
    private val rate: Long,
    window: Duration = Duration.ofSeconds(1)
) : RateLimiter {
    private val rateLimiterScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())

    private val semaphore = Semaphore(rate.toInt())
    private val queue = PriorityBlockingQueue<Measure>(10_000)
    private val windowNanos = window.toNanos()

    init {
        rateLimiterScope.launch {
            while (true) {
                val head = queue.peek()
                val winStart = System.nanoTime() - windowNanos
                if (head == null) {
                    delay(1L)
                    continue
                }
                if (head.timestamp > winStart) {
                    val remainingNanos = head.timestamp - winStart
                    val remainingMillis = remainingNanos / 1_000_000
                    delay(maxOf(1L, remainingMillis))
                    continue
                }
                queue.poll()
                semaphore.release()
            }
        }.invokeOnCompletion { th -> if (th != null) {} } // logger.error("Rate limiter release job completed", th) }
    }

    override fun tick(): Boolean {
        return if (semaphore.tryAcquire()) {
            queue.add(Measure(1, System.nanoTime()))
            true
        } else {
            false
        }
    }

    suspend fun tickAsync() {
        semaphore.acquire()
        queue.add(Measure(1, System.nanoTime()))
    }
    
    fun availablePermits(): Int = semaphore.availablePermits

    data class Measure(
        val value: Long,
        val timestamp: Long
    ) : Comparable<Measure> {
        override fun compareTo(other: Measure): Int {
            return timestamp.compareTo(other.timestamp)
        }
    }

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(SlidingWindowRateLimiter::class.java)
    }
}