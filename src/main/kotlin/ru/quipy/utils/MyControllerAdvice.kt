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

    val logger: Logger = LoggerFactory.getLogger(MyControllerAdvice::class.java)


    @ExceptionHandler(RateLimitExceededException::class)
    fun handleTooManyRequestsException(e: RateLimitExceededException): ResponseEntity<Any> {
        logger.warn("Too many requests: {}", e.message)

        val headers = HttpHeaders()
        val processTime = e.message?.toLongOrNull() ?: 1000L
        val retryAfter = processTime + (processTime * 2.0 * Math.random()).toLong()
        headers.add("Retry-After", retryAfter.toString())

        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
            .headers(headers)
            .body(
                mapOf(
                    "error" to "Too Many Requests",
                    "message" to "Rate limit exceeded. Please try again later.",
                    "retry_after" to retryAfter,
                    "timestamp" to Instant.now()
                )
            )
    }
}
