package ru.quipy.payments.logic

import org.springframework.stereotype.Service
import java.util.*

@Service
class PaymentSystemImpl(
    paymentAccounts: List<PaymentExternalSystemAdapter>
) : PaymentService {
    private val accountProvider = AccountProvider(paymentAccounts)

    override suspend fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        accountProvider.withAccountAsync {
            it.performPayment(paymentId, amount, paymentStartedAt, deadline)
        }
    }
}
