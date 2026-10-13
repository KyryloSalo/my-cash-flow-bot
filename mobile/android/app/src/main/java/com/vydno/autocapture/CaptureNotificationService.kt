package com.vydno.autocapture

import android.content.ComponentName
import android.content.Context
import android.service.notification.NotificationListenerService
import android.service.notification.StatusBarNotification
import com.vydno.autocapture.capture.EventQueue
import com.vydno.autocapture.capture.NotificationParser
import com.vydno.autocapture.security.CredentialStore
import com.vydno.autocapture.transport.CaptureTransport

class CaptureNotificationService : NotificationListenerService() {
    private lateinit var queue: EventQueue
    private lateinit var transport: CaptureTransport
    private lateinit var parser: NotificationParser
    override fun onCreate() { super.onCreate(); queue=EventQueue(this); parser=NotificationParser(allowedPackages(this), packageBanks(this)); transport=CaptureTransport("https://vydno.capital", { CredentialStore(this).get() }); flush() }
    override fun onNotificationPosted(sbn: StatusBarNotification) { val text=sbn.notification.extras.getCharSequence("android.text")?.toString() ?: return; val event=parser.parse(sbn.packageName,text) ?: return; queue.upsert(sbn.key,event); flush() }
    override fun onListenerConnected() { flush() }
    override fun onListenerDisconnected() { requestRebind(ComponentName(this, javaClass)) }
    override fun onDestroy() { transport.shutdown(); super.onDestroy() }
    private fun flush() { queue.due().forEach { item -> transport.sendAsync(item.event) { result -> when (result) { is com.vydno.autocapture.transport.SendResult.Accepted -> queue.remove(item.notificationKey); is com.vydno.autocapture.transport.SendResult.Conflict -> queue.markManualReview(item.notificationKey); com.vydno.autocapture.transport.SendResult.Revoked -> Unit; is com.vydno.autocapture.transport.SendResult.Retry -> queue.markRetry(item.notificationKey) } } } }
    private fun allowedPackages(c: Context): Set<String> = c.getSharedPreferences("capture_config", Context.MODE_PRIVATE).getStringSet("allowed_packages", emptySet()) ?: emptySet()
    private fun packageBanks(c: Context): Map<String, String> = CredentialStore(c).packageBanks()
}
