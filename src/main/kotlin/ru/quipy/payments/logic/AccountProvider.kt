package ru.quipy.payments.logic

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.time.Duration
import java.util.concurrent.Executors

class AccountProvider(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>
) {
    private val queueScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())

    private val rateLimiters: Map<String, SlidingWindowRateLimiter> = paymentAccounts.associateBy(
        { it.name() },
        { SlidingWindowRateLimiter(it.rateLimitPerSec().toLong(), Duration.ofSeconds(1)) }
    )

    private val ongoingWindows: Map<String, OngoingWindow> = paymentAccounts.associateBy(
        { it.name() },
        { OngoingWindow(it.parallelRequests()) }
    )

    suspend fun acquireAsync(): PaymentExternalSystemAdapter {
        val account = paymentAccounts[0]
        ongoingWindows.getValue(account.name()).acquireAsync()
        rateLimiters.getValue(account.name()).acquireAsync()
        return account
    }

    suspend fun release(accountName: String) {
        ongoingWindows.getValue(accountName).release()
    }
}

suspend fun AccountProvider.withAccountAsync(block: suspend (PaymentExternalSystemAdapter) -> Unit) {
    val account = acquireAsync()
    block.invoke(account)
    release(account.name())
}
