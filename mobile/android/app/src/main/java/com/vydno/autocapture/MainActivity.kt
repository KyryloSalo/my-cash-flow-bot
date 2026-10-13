package com.vydno.autocapture

import android.app.Activity
import android.os.Bundle
import android.content.Intent
import android.provider.Settings
import android.webkit.WebView
import android.webkit.WebViewClient

class MainActivity : Activity() {
    override fun onCreate(state: Bundle?) { super.onCreate(state); val w=WebView(this); w.settings.javaScriptEnabled=true; w.webViewClient=object: WebViewClient(){ override fun shouldOverrideUrlLoading(v: WebView, url: String): Boolean = !url.startsWith("https://vydno.capital/") }; w.loadUrl("https://vydno.capital/app/"); setContentView(w) }
    fun openNotificationAccess() { startActivity(Intent(Settings.ACTION_NOTIFICATION_LISTENER_SETTINGS)) }
}
