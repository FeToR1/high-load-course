package ru.quipy.monitoring

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Timer
import org.springframework.stereotype.Service
import java.time.Duration
import java.util.concurrent.TimeUnit

enum class RequestType {
    INCOMING,
    OUTGOING,
    PROCESSED_SUCCESS,
    PROCESSED_FAIL,
}

@Service
class MonitoringService() {

    fun increaseRequestsCounter(requestType: RequestType) = Counter
        .builder("http_requests_count")
        .description("Number of http requests by type")
        .tags("type", requestType.name)
        .register(Metrics.globalRegistry)
        .increment()

    fun increaseRetryCounter() = Counter
        .builder("http_retry_count")
        .description("Total number of retry attempts")
        .register(Metrics.globalRegistry)
        .increment()

    fun recordRequestDuration(durationMs: Long, success: Boolean) {
        Timer.builder("http_request_duration")
            .description("Request duration with percentiles (50th, 90th, 95th, 99th)")
            .tags("success", success.toString())
            .publishPercentiles(0.5, 0.9, 0.95, 0.99)
            .publishPercentileHistogram()
            .register(Metrics.globalRegistry)
            .record(durationMs, TimeUnit.MILLISECONDS)
    }

    fun get90thPercentileTimeout(accountName: String): Duration {
        val timeoutMs = ACCOUNT_TIMEOUTS[accountName] ?: DEFAULT_TIMEOUT_MS
        return Duration.ofMillis(timeoutMs)
    }

    companion object {
        private const val DEFAULT_TIMEOUT_MS = 15000L

        private val ACCOUNT_TIMEOUTS = mapOf(
            "acc-7" to 1070L,
            "acc-12" to 4900L,
        )
    }
}
