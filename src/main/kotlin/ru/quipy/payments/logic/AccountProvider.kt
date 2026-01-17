package ru.quipy.payments.logic

import org.apache.logging.log4j.util.Supplier
import org.springframework.stereotype.Service

@Service
class AccountProvider(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>
) : Supplier<PaymentExternalSystemAdapter> {

    override fun get(): PaymentExternalSystemAdapter {
        return paymentAccounts[0]
    }
}
