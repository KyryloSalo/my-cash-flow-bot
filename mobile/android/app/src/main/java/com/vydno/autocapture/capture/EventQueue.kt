package com.vydno.autocapture.capture

import android.content.Context
import androidx.security.crypto.EncryptedSharedPreferences
import androidx.security.crypto.MasterKey
import org.json.JSONArray
import org.json.JSONObject
import java.time.Instant
import java.util.UUID

object StableEventIds { fun forNotificationKey(key: String): UUID { val b = java.security.MessageDigest.getInstance("SHA-256").digest(key.toByteArray()); return UUID.nameUUIDFromBytes(b) } }
data class QueueItem(val event: NormalizedEvent, val notificationKey: String, val attempts: Int = 0, val nextAttemptEpochMs: Long = 0, val createdEpochMs: Long = System.currentTimeMillis(), val manualReview: Boolean = false)

class EventQueue(context: Context, private val ttlMs: Long = 7 * 24 * 60 * 60 * 1000L) {
    private val prefs = EncryptedSharedPreferences.create(context, "capture_queue", MasterKey.Builder(context).setKeyScheme(MasterKey.KeyScheme.AES256_GCM).build(), EncryptedSharedPreferences.PrefKeyEncryptionScheme.AES256_SIV, EncryptedSharedPreferences.PrefValueEncryptionScheme.AES256_GCM)
    private val items = linkedMapOf<String, QueueItem>()
    init { load() }
    @Synchronized fun upsert(key: String, event: NormalizedEvent) {
        val stable = event.copy(eventId = StableEventIds.forNotificationKey(key))
        val prior = items[key]
        if (prior == null) items[key] = QueueItem(stable, key)
        else if (prior.event != stable) items[key] = prior.copy(event = stable, attempts = 0, nextAttemptEpochMs = 0)
        else return
        save()
    }
    @Synchronized fun due(now: Long = System.currentTimeMillis()): List<QueueItem> { val expired = items.filterValues { now - it.createdEpochMs > ttlMs }.keys; expired.forEach(items::remove); if (expired.isNotEmpty()) save(); return items.values.filter { !it.manualReview && it.nextAttemptEpochMs <= now } }
    @Synchronized fun markManualReview(key: String) { items[key]?.let { items[key] = it.copy(manualReview = true); save() } }
    @Synchronized fun markRetry(key: String, now: Long = System.currentTimeMillis()) { items[key]?.let { val n=it.attempts+1; items[key]=it.copy(attempts=n,nextAttemptEpochMs=now + (1000L shl minOf(n,8))); save() } }
    @Synchronized fun remove(key: String) { items.remove(key); save() }
    @Synchronized fun clear() { items.clear(); save() }
    private fun save() { val a=JSONArray(); items.values.forEach { i -> a.put(JSONObject().apply { put("key",i.notificationKey); put("id",i.event.eventId.toString()); put("bank",i.event.bank); put("observed",i.event.observedAt.toString()); put("amount",i.event.amount); put("currency",i.event.currency); put("merchant",i.event.merchant); put("hint",i.event.operationHint); put("version",i.event.parserVersion); put("attempts",i.attempts); put("next",i.nextAttemptEpochMs); put("created",i.createdEpochMs); put("manual",i.manualReview) }) }; prefs.edit().putString("items",a.toString()).commit() }
    private fun load() { val a=JSONArray(prefs.getString("items","[]")); for (n in 0 until a.length()) { val o=a.getJSONObject(n); val e=NormalizedEvent(UUID.fromString(o.getString("id")),o.getString("bank"),Instant.parse(o.getString("observed")),if(o.isNull("amount")) null else o.getString("amount"),if(o.isNull("currency")) null else o.getString("currency"),if(o.isNull("merchant")) null else o.getString("merchant"),o.getString("hint"),o.getString("version")); val i=QueueItem(e,o.getString("key"),o.getInt("attempts"),o.getLong("next"),o.getLong("created"),o.optBoolean("manual", false)); items[i.notificationKey]=i } }
}
