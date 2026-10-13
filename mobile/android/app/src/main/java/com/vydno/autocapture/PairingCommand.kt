package com.vydno.autocapture

object PairingCommand {
    private val credentialPattern = Regex("^[A-Za-z0-9_-]{43,64}$")

    fun parse(message: String): String? {
        val credential = message.removePrefix("credential:")
        if (credential == message || !credentialPattern.matches(credential)) return null
        return credential
    }

    fun opensNotificationSettings(message: String): Boolean = message == "open-notification-settings"
}
