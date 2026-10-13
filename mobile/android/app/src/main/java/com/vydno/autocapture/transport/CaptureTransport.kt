package com.vydno.autocapture.transport

import com.vydno.autocapture.capture.NormalizedEvent
import java.net.HttpURLConnection
import java.net.URL
import java.util.concurrent.Executors
import java.util.concurrent.Future

sealed class SendResult { data object Accepted: SendResult(); data object Conflict: SendResult(); data class Retry(val code: Int): SendResult(); data object Revoked: SendResult() }
class CaptureTransport(private val baseUrl: String, private val credential: () -> String?, private val timeoutMs: Int = 15000) {
    private val executor = Executors.newSingleThreadExecutor()
    fun sendAsync(event: NormalizedEvent, callback: (SendResult) -> Unit): Future<*> = executor.submit { callback(send(event)) }
    fun shutdown() = executor.shutdownNow()
    fun send(event: NormalizedEvent): SendResult {
        val token = credential() ?: return SendResult.Revoked
        val payload = JSONObjectBuilder.event(event)
        val c = (URL("${baseUrl.trimEnd('/')}/app/api/capture/v1/events").openConnection() as HttpURLConnection)
        return try { c.connectTimeout=timeoutMs; c.readTimeout=timeoutMs; c.requestMethod="POST"; c.doOutput=true; c.setRequestProperty("Authorization","Bearer $token"); c.setRequestProperty("Content-Type","application/json"); c.outputStream.use { it.write(payload.toByteArray()) }; when(c.responseCode) { 200,201,202 -> SendResult.Accepted; 409 -> SendResult.Conflict; 401,403 -> SendResult.Revoked; else -> SendResult.Retry(c.responseCode) } } catch (_: java.net.SocketTimeoutException) { SendResult.Retry(408) } catch (_: java.io.IOException) { SendResult.Retry(0) } finally { c.disconnect() }
    }
}
private object JSONObjectBuilder { fun event(e:NormalizedEvent)=org.json.JSONObject().apply { put("schema_version",1); put("event_id",e.eventId.toString()); put("source","android_notification"); put("bank",e.bank); put("observed_at",e.observedAt.toString()); put("occurred_at",org.json.JSONObject.NULL); put("amount",e.amount ?: org.json.JSONObject.NULL); put("currency",e.currency ?: org.json.JSONObject.NULL); put("merchant",e.merchant ?: org.json.JSONObject.NULL); put("operation_hint",e.operationHint); put("parser_version",e.parserVersion) }.toString() }
