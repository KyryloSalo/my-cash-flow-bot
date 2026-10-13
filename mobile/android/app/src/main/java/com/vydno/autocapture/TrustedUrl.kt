package com.vydno.autocapture

import java.net.URI

object TrustedUrl {
    fun isAllowed(value: String): Boolean = try {
        val uri = URI(value)
        uri.scheme == "https" && uri.host == "vydno.capital" && uri.port == -1
    } catch (_: Exception) {
        false
    }
}
