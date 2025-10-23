package ru.quipy.utils

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ControllerAdvice
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.client.HttpClientErrorException
import ru.quipy.apigateway.APIController
import java.time.Instant

@ControllerAdvice
class MyControllerAdvice {

    val logger: Logger = LoggerFactory.getLogger(APIController::class.java)

    @ExceptionHandler(HttpClientErrorException.TooManyRequests::class)
    fun handleTooManyRequestsException(e: HttpClientErrorException.TooManyRequests): ResponseEntity<Any> {
        logger.warn("Too many requests: {}", e.message)

        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
            .body(
                mapOf(
                    "error" to "Too Many Requests",
                    "message" to "Rate limit exceeded. Please try again later.",
                    "timestamp" to Instant.now()
                )
            )
    }
}
