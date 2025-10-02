package ru.quipy.payments.logic

import io.micrometer.core.instrument.Counter
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry

private val prometheusRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)

fun increaseRequestsCounter(requestType: String) = Counter
    .builder("http_requests_count")
    .description("Number of http requests by type (incoming, processed, outgoing)")
    .tags("type", requestType)
    .register(prometheusRegistry)
    .increment()