package ru.quipy.payments.logic

import kotlinx.coroutines.CoroutineContext
import org.springframework.stereotype.Service
import java.util.*
import kotlin.coroutines.EmptyCoroutineContext

@Service
class PaymentSystemImpl(
    paymentAccounts: List<PaymentExternalSystemAdapter>
) : PaymentService {
    private val accountProvider = AccountProvider(paymentAccounts)

    override suspend fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long, ioContext: CoroutineContext) {
        accountProvider.withAccountAsync {
            it.performPayment(paymentId, amount, paymentStartedAt, deadline, ioContext)
        }
    }
}
