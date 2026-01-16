package ru.quipy.common.utils

import ru.quipy.payments.logic.PaymentExternalSystemAdapter
import java.time.Instant

interface RateLimiterFactory {
    fun createForAccount(
        account: PaymentExternalSystemAdapter,
        deadline: Instant
    ): RateLimiter
}