package com.vydno.autocapture.capture

import android.content.Context
import androidx.work.CoroutineWorker
import androidx.work.WorkerParameters
import com.vydno.autocapture.security.CredentialStore
import com.vydno.autocapture.transport.CaptureTransport

class CaptureRetryWorker(appContext: Context, params: WorkerParameters) : CoroutineWorker(appContext, params) {
    override suspend fun doWork(): Result {
        val queue = EventQueue(applicationContext)
        val transport = CaptureTransport("https://vydno.capital", { CredentialStore(applicationContext).get() })
        return try {
            var retryNeeded = false
            queue.due().forEach { item -> when (transport.send(item.event)) {
                com.vydno.autocapture.transport.SendResult.Accepted -> queue.remove(item.notificationKey)
                com.vydno.autocapture.transport.SendResult.Conflict -> queue.markManualReview(item.notificationKey)
                com.vydno.autocapture.transport.SendResult.Revoked -> { queue.clear(); return Result.success() }
                is com.vydno.autocapture.transport.SendResult.Retry -> { queue.markRetry(item.notificationKey); retryNeeded = true }
            } }
            if (retryNeeded) Result.retry() else Result.success()
        } finally { transport.shutdown() }
    }
}
