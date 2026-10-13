package com.vydno.autocapture.security

import android.content.Context
import androidx.security.crypto.EncryptedSharedPreferences
import androidx.security.crypto.MasterKey

class CredentialStore(context: Context) {
    private val prefs = EncryptedSharedPreferences.create(context, "capture_credentials", MasterKey.Builder(context).setKeyScheme(MasterKey.KeyScheme.AES256_GCM).build(), EncryptedSharedPreferences.PrefKeyEncryptionScheme.AES256_SIV, EncryptedSharedPreferences.PrefValueEncryptionScheme.AES256_GCM)
    fun put(value: String) = prefs.edit().putString("submit_credential", value).apply()
    fun get(): String? = prefs.getString("submit_credential", null)
    fun clear() = prefs.edit().clear().apply()
    fun putPackageBank(packageName: String, bank: String) = prefs.edit().putString("bank:$packageName", bank).apply()
    fun packageBanks(): Map<String, String> = prefs.all.filterKeys { it.startsWith("bank:") }.mapKeys { it.key.removePrefix("bank:") }.mapValues { it.value.toString() }
}
