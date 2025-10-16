package ru.quipy.payments.logic

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.selects.select
import org.testcontainers.shaded.org.bouncycastle.util.Objects
import ru.quipy.common.utils.BlockingRateLimiter
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.time.Duration
import java.util.concurrent.Executors

class AccountProvider(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>
) {
    private val queueScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())

    private val rateLimiters: Map<String, BlockingRateLimiter> = paymentAccounts.associateBy(
        { it.name() },
        { SlidingWindowRateLimiter(it.rateLimitPerSec().toLong(), Duration.ofSeconds(1)) }
    )

    private val ongoingWindows: Map<String, OngoingWindow> = paymentAccounts.associateBy(
        { it.name() },
        { OngoingWindow(it.parallelRequests()) }
    )

    fun acquire(): PaymentExternalSystemAdapter {
        return runBlocking {
            select {
                paymentAccounts.forEach { account ->
                    queueScope.async {
                        ongoingWindows.getValue(account.name()).acquire()
                        rateLimiters.getValue(account.name()).tickBlocking()
                        return@async account
                    }.onAwait { it }
                }
            }
        }
    }

    fun release(accountName: String) {
        ongoingWindows.getValue(accountName).release()
    }
}

fun AccountProvider.withAccount(block: (PaymentExternalSystemAdapter) -> Unit) {
    var account: PaymentExternalSystemAdapter? = null
    try {
        account = acquire()
        block.invoke(account)
    } finally {
        if (!Objects.areEqual(account, null)) {
            release(account!!.name())
        }
    }
}
