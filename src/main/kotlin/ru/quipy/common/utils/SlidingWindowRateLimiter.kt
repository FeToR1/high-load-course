package ru.quipy.common.utils

import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.time.Duration
import kotlin.math.max


class SlidingWindowRateLimiter(
    private val rate: Long,
    window: Duration,
) {
    private val windowMillis = window.toMillis()
    private val mutex = Mutex()


    private val timestamps = ArrayDeque<Long>()

    fun tryAcquire(): Boolean {
        val now = System.currentTimeMillis()
        return synchronized(this) {
            evictExpired(now)
            if (timestamps.size < rate) {
                timestamps.addLast(now)
                true
            } else {
                false
            }
        }
    }

    suspend fun acquireAsync() {
        while (true) {

            val waitMillis = mutex.withLock {
                val now = System.currentTimeMillis()
                evictExpired(now)


                if (timestamps.size < rate) {
                    timestamps.addLast(now)
                    return
                }


                val oldest = timestamps.first()
                val elapsed = now - oldest
                val remaining = windowMillis - elapsed
                max(remaining, 0L)
            }

            if (waitMillis <= 0L) {
                continue
            }

            delay(waitMillis)
        }
    }

    private fun evictExpired(now: Long) {
        val threshold = now - windowMillis
        while (timestamps.isNotEmpty() && timestamps.first() <= threshold) {
            timestamps.removeFirst()
        }
    }
}
