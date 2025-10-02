package ru.quipy.monitoring

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Timer
import org.springframework.stereotype.Service

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
}