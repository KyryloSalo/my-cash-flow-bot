package com.vydno.autocapture

import android.app.Activity
import android.content.Intent
import android.os.Bundle
import android.provider.Settings
import android.webkit.WebResourceRequest
import android.webkit.WebView
import android.webkit.WebViewClient
import androidx.webkit.WebViewCompat
import androidx.webkit.WebViewFeature
import com.vydno.autocapture.security.CredentialStore

class MainActivity : Activity() {
    override fun onCreate(state: Bundle?) {
        super.onCreate(state)
        val webView = WebView(this)
        webView.settings.javaScriptEnabled = true
        webView.webViewClient = object : WebViewClient() {
            override fun shouldOverrideUrlLoading(view: WebView, request: WebResourceRequest): Boolean =
                !TrustedUrl.isAllowed(request.url.toString())
        }
        if (WebViewFeature.isFeatureSupported(WebViewFeature.WEB_MESSAGE_LISTENER)) {
            WebViewCompat.addWebMessageListener(
                webView,
                "AndroidCapture",
                setOf("https://vydno.capital"),
            ) { _, message, sourceOrigin, isMainFrame, replyProxy ->
                if (!isMainFrame || sourceOrigin.toString() != "https://vydno.capital") return@addWebMessageListener
                val value = message.data ?: return@addWebMessageListener
                when {
                    PairingCommand.opensNotificationSettings(value) -> {
                        startActivity(Intent(Settings.ACTION_NOTIFICATION_LISTENER_SETTINGS))
                        replyProxy.postMessage("notification-settings-opened")
                    }
                    else -> PairingCommand.parse(value)?.let { credential ->
                        CredentialStore(this).put(credential)
                        replyProxy.postMessage("credential-stored")
                    }
                }
            }
        }
        webView.loadUrl("https://vydno.capital/app/")
        setContentView(webView)
    }
}
