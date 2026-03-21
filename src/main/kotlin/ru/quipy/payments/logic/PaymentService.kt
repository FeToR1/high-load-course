package ru.quipy.payments.logic

import org.springframework.stereotype.Service
import java.time.Instant
import java.util.*
import java.util.function.Supplier

@Service
class PaymentService(
    private val accountProvider: Supplier<PaymentExternalSystemAdapter>
) {

    suspend fun submitPaymentRequest(
        paymentId: UUID,
        amount: Int,
        deadline: Instant,
        paymentStartedAt: Long
    ) {
        val account = accountProvider.get()
        account.performPayment(paymentId, amount, deadline, paymentStartedAt)
    }
}
