package com.vydno.autocapture.capture

import android.content.Context
import androidx.work.Constraints
import androidx.work.ExistingWorkPolicy
import androidx.work.NetworkType
import androidx.work.OneTimeWorkRequestBuilder
import androidx.work.WorkManager
import java.util.concurrent.TimeUnit

object CaptureRetryScheduler {
    private const val NAME = "capture-delivery"
    fun enqueue(context: Context) {
        val request = OneTimeWorkRequestBuilder<CaptureRetryWorker>()
            .setConstraints(Constraints.Builder().setRequiredNetworkType(NetworkType.CONNECTED).build())
            .setBackoffCriteria(androidx.work.BackoffPolicy.EXPONENTIAL, 10, TimeUnit.SECONDS)
            .setInitialDelay(1, TimeUnit.SECONDS).build()
        // Append a successor when delivery work is already running. KEEP can lose a
        // service-side retry that is enqueued after the running worker took its snapshot.
        WorkManager.getInstance(context).enqueueUniqueWork(NAME, ExistingWorkPolicy.APPEND_OR_REPLACE, request)
    }
    fun cancel(context: Context) { WorkManager.getInstance(context).cancelUniqueWork(NAME) }
}
