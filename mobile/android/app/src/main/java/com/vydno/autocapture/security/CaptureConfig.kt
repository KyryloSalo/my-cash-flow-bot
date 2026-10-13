package com.vydno.autocapture.security

object CaptureConfig {
    const val PREFS = "capture_config"
    const val ALLOWED_PACKAGES = "allowed_packages"
    const val TRUSTED_ORIGIN = "https://vydno.capital"
    val SUPPORTED_BANKS: Set<String> = setOf("mono", "privat24", "unknown")

    fun normalizePackageName(value: String): String? {
        val normalized = value.trim()
        return normalized.takeIf { it.matches(Regex("[A-Za-z][A-Za-z0-9_]*(\\.[A-Za-z0-9_]+)+")) }
    }

    fun normalizeBank(value: String): String? {
        val normalized = value.trim().lowercase()
        return normalized.takeIf { it in SUPPORTED_BANKS }
    }
}
