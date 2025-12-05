package ru.quipy.payments.logic

class AccountProvider(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>
) {
    fun getAccount(): PaymentExternalSystemAdapter {
        return paymentAccounts[0]
    }
}

suspend fun AccountProvider.withAccountAsync(block: suspend (PaymentExternalSystemAdapter) -> Unit) {
    val account = getAccount()
    block.invoke(account)
}
