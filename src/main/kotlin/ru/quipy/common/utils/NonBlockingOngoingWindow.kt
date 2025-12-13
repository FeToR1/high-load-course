package ru.quipy.common.utils

import kotlinx.coroutines.sync.Semaphore

class OngoingWindow(
    maxWinSize: Int
) {
    private val window = Semaphore(maxWinSize)

    suspend fun acquireAsync() {
        window.acquire()
    }

    fun release() {
        window.release()
    }

    fun awaitingQueueSize(): Int = window.availablePermits
}