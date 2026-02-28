package ru.quipy.payments.logic

import org.springframework.stereotype.Service
import java.util.*
import java.util.function.Supplier

@Service
class PaymentServiceImpl(
    private val accountProvider: Supplier<PaymentExternalSystemAdapter>
) {

    suspend fun submitPaymentRequest(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadlineTimestampMs: Long
    ) {
        val account = accountProvider.get()
        account.performPayment(paymentId, amount, paymentStartedAt, deadlineTimestampMs)
    }
}
