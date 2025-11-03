package ru.quipy.utils

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ControllerAdvice
import org.springframework.web.bind.annotation.ExceptionHandler
import ru.quipy.common.utils.RateLimitExceededException
import java.time.Instant

@ControllerAdvice
class MyControllerAdvice {
    private val logger: Logger = LoggerFactory.getLogger(MyControllerAdvice::class.java)

    @ExceptionHandler(RateLimitExceededException::class)
    fun handleTooManyRequestsException(e: RateLimitExceededException): ResponseEntity<Any> {
        logger.warn("Too many requests")

        val headers = HttpHeaders()
        headers.add("Retry-After", e.getRetryAfterSecondsWithJitter().toString())

        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
            .headers(headers)
            .body(
                mapOf(
                    "error" to "Too Many Requests",
                    "message" to "Rate limit exceeded. Please try again later.",
                    "retry_after_ms" to e.getRetryAfterWithJitter(),
                    "timestamp" to Instant.now()
                )
            )
    }
}
