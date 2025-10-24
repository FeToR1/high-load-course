package ru.quipy.common.utils

import kotlin.math.ceil
import kotlin.random.Random

class RateLimitExceededException(
    val retryAfterMs: Long,
    private val jitterPercent: Int = 20
) : RuntimeException("Rate limit exceeded. Retry after ${retryAfterMs}ms") {

    fun getRetryAfterWithJitter(): Long {
        val jitterRange = (retryAfterMs * jitterPercent / 100.0).toLong()
        val jitter = Random.nextLong(-jitterRange, jitterRange + 1)
        return (retryAfterMs + jitter).coerceAtLeast(0)
    }

    fun getRetryAfterSecondsWithJitter(): Long {
        return ceil(getRetryAfterWithJitter() / 1000.0).toLong()
    }

    fun getRetryAfterMs(): Long = retryAfterMs

    fun getRetryAfterSeconds(): Long = ceil(retryAfterMs / 1000.0).toLong()
}
