package com.vydno.autocapture

import android.app.Activity
import android.content.ComponentName
import android.content.Intent
import android.os.Bundle
import android.provider.Settings
import android.text.InputType
import android.view.ViewGroup
import android.webkit.WebResourceRequest
import android.webkit.WebView
import android.webkit.WebViewClient
import androidx.webkit.WebMessageCompat
import androidx.webkit.JavaScriptReplyProxy
import androidx.webkit.WebViewCompat
import androidx.webkit.WebViewFeature
import android.net.Uri
import org.json.JSONObject

import android.widget.Button
import android.widget.EditText
import android.widget.LinearLayout
import android.widget.TextView
import com.vydno.autocapture.security.CaptureConfig
import com.vydno.autocapture.security.CredentialStore

class MainActivity : Activity() {
    private val config by lazy { getSharedPreferences(CaptureConfig.PREFS, MODE_PRIVATE) }
    private val credentials by lazy { CredentialStore(this) }
    private lateinit var status: TextView
    private lateinit var packageInput: EditText
    private lateinit var bankInput: EditText
    private lateinit var webView: WebView

    override fun onCreate(state: Bundle?) {
        super.onCreate(state)
        val root = LinearLayout(this).apply { orientation = LinearLayout.VERTICAL; setPadding(24, 24, 24, 24) }
        status = TextView(this)
        root.addView(status)
        root.addView(Button(this).apply { text = "Grant notification access"; setOnClickListener { openNotificationAccess() } })
        packageInput = EditText(this).apply { hint = "Verified Android package ID (e.g. from device evidence)"; inputType = InputType.TYPE_CLASS_TEXT }
        bankInput = EditText(this).apply { hint = "Canonical bank key"; inputType = InputType.TYPE_CLASS_TEXT }
        root.addView(packageInput)
        root.addView(bankInput)
        root.addView(Button(this).apply { text = "Save verified bank mapping"; setOnClickListener { saveMapping() } })
        root.addView(Button(this).apply { text = "Revoke capture and clear mappings"; setOnClickListener { revokeCapture() } })
        webView = WebView(this).apply {
            settings.javaScriptEnabled = true
            settings.allowFileAccess = false
            settings.allowContentAccess = false
            webViewClient = object : WebViewClient() {
                @Suppress("DEPRECATION")
                override fun shouldOverrideUrlLoading(v: WebView, url: String): Boolean = !isTrustedUrl(url)
                override fun shouldOverrideUrlLoading(v: WebView, request: WebResourceRequest): Boolean = !request.isForMainFrame || !isTrustedUrl(request.url.toString())
                override fun onPageFinished(v: WebView, url: String) { if (!isTrustedUrl(url)) v.stopLoading(); super.onPageFinished(v, url) }
            }
            if (WebViewFeature.isFeatureSupported(WebViewFeature.WEB_MESSAGE_LISTENER)) {
                WebViewCompat.addWebMessageListener(this, "AndroidCapture", setOf(CaptureConfig.TRUSTED_ORIGIN), object : WebViewCompat.WebMessageListener {
                    override fun onPostMessage(view: WebView, message: WebMessageCompat, sourceOrigin: Uri, isMainFrame: Boolean, replyProxy: JavaScriptReplyProxy) {
                        if (!isMainFrame || sourceOrigin.toString() != CaptureConfig.TRUSTED_ORIGIN) return
                        runCatching { JSONObject(message.data ?: return).let { msg ->
                            when (msg.optString("action")) {
                                "handoff" -> msg.optString("credential").trim().let { token -> if (token.isNotEmpty() && token.length <= 4096 && token.none { it.isWhitespace() }) { credentials.put(token); refreshStatus() } }
                                "revoke" -> revokeCapture()
                            }
                        } }
                    }
                })
            }
            if (WebViewFeature.isFeatureSupported(WebViewFeature.WEB_MESSAGE_LISTENER)) {
                loadUrl("https://vydno.capital/app/")
            } else {
                status.text = "Unsupported Android WebView: update WebView before using capture.\nSign in through the canonical Vydno PWA with Telegram; this app does not create a separate identity."
            }
        }
        root.addView(webView, LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, 0, 1f))
        setContentView(root)
        refreshStatus()
    }

    fun openNotificationAccess() { startActivity(Intent(Settings.ACTION_NOTIFICATION_LISTENER_SETTINGS)) }

    private fun listenerEnabled(): Boolean {
        val enabled = Settings.Secure.getString(contentResolver, "enabled_notification_listeners").orEmpty()
        return enabled.split(":").any { it == ComponentName(this, CaptureNotificationService::class.java).flattenToString() }
    }

    private fun saveMapping() {
        val packageName = CaptureConfig.normalizePackageName(packageInput.text.toString())
        val bank = CaptureConfig.normalizeBank(bankInput.text.toString())
        if (packageName == null || bank == null) { status.text = "Enter a verified package ID and canonical bank key."; return }
        credentials.putPackageBank(packageName, bank)
        val packages = config.getStringSet(CaptureConfig.ALLOWED_PACKAGES, emptySet()).orEmpty().toMutableSet().apply { add(packageName) }
        config.edit().putStringSet(CaptureConfig.ALLOWED_PACKAGES, packages).apply()
        packageInput.text.clear(); bankInput.text.clear(); refreshStatus()
    }

    private fun revokeCapture() {
        if (::webView.isInitialized) webView.evaluateJavascript("window.__androidCaptureRevoke?.()", null)
        com.vydno.autocapture.capture.CaptureRetryScheduler.cancel(this)
            config.edit().remove(CaptureConfig.ALLOWED_PACKAGES).apply()
            credentials.packageBanks().keys.forEach(credentials::removePackageBank)
            credentials.clear()
            refreshStatus()
        }

    private fun refreshStatus() {
        val packages = config.getStringSet(CaptureConfig.ALLOWED_PACKAGES, emptySet()).orEmpty()
        status.text = "Notification access: ${if (listenerEnabled()) "granted" else "not granted"}\nConfigured verified banks: ${packages.size}\nCapture is fail-closed until both access and a mapping are configured."
    }

    override fun onResume() { super.onResume(); if (::status.isInitialized) refreshStatus() }

    private fun isTrustedUrl(value: String): Boolean = trustedOrigin(value) == CaptureConfig.TRUSTED_ORIGIN

    private fun trustedOrigin(value: String): String? = runCatching {
        val uri = android.net.Uri.parse(value)
        if (uri.scheme == "https" && uri.host == "vydno.capital" && (uri.port == -1 || uri.port == 443)) {
            "https://vydno.capital"
        } else null
    }.getOrNull()
}
