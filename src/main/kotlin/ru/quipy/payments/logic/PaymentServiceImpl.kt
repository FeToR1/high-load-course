package ru.quipy.payments.logic

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.selects.select
import org.springframework.stereotype.Service
import ru.quipy.common.utils.BlockingRateLimiter
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors

class AccountProvider(
    val paymentAccounts: List<PaymentExternalSystemAdapter>
) {
    private val queueScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())

    private val rateLimiters: Map<String, BlockingRateLimiter> = paymentAccounts.associateBy(
        { it.name() },
        { SlidingWindowRateLimiter(it.rateLimitPerSec().toLong(), Duration.ofSeconds(1)) }
    )

    val ongoingWindows: Map<String, OngoingWindow> = paymentAccounts.associateBy(
        { it.name() },
        { OngoingWindow(it.parallelRequests()) }
    )

    fun acquire(): PaymentExternalSystemAdapter {
        // TODO: make this function fair
        return runBlocking {
            select {
                paymentAccounts.forEach { account ->
                    queueScope.async {
                        rateLimiters.getValue(account.name()).tickBlocking()
                        return@async account
                    }.onAwait { it }
                }
            }
        }
    }
}

@Service
class PaymentSystemImpl(
    paymentAccounts: List<PaymentExternalSystemAdapter>
) : PaymentService {
    private val accountProvider = AccountProvider(paymentAccounts)

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        accountProvider.paymentAccounts.forEach { account ->
            val ongoingWindow = accountProvider.ongoingWindows.getValue(account.name())
            try {
                ongoingWindow.acquire()
                accountProvider.acquire().performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
            } finally {
                ongoingWindow.release()
            }

        }

    }
}