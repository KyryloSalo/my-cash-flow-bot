package com.vydno.autocapture.capture

import java.time.Instant
import java.util.UUID

data class NormalizedEvent(val eventId: UUID, val bank: String, val observedAt: Instant, val amount: String?, val currency: String?, val merchant: String?, val operationHint: String, val parserVersion: String = "android-1")

class NotificationParser(private val allowedPackages: Set<String>, private val packageBanks: Map<String, String> = emptyMap()) {
    fun parse(packageName: String, text: String, observedAt: Instant = Instant.now()): NormalizedEvent? {
        if (packageName !in allowedPackages || text.length > 1000) return null
        val lower = text.lowercase()
        if (listOf("otp", "one-time", "verification", "transfer", "refund", "income", "salary", "received").any(lower::contains)) return null
        val amountMatch = Regex("(?<!\\d)(\\d+(?:[.,]\\d{1,2})?)(?:\\s*)(UAH|грн|USD|EUR)?", RegexOption.IGNORE_CASE).find(text)
        val amount = amountMatch?.groupValues?.get(1)?.replace(',', '.')
        val currency = amountMatch?.groupValues?.get(2)?.uppercase()?.let { if (it == "ГРН") "UAH" else it }
        val merchant = Regex("(?:at|у|в)\\s+([\\p{L}\\p{N} .&'-]{2,80})", RegexOption.IGNORE_CASE).find(text)?.groupValues?.get(1)?.trim()
        val purchaseWords = listOf("purchase", "купів", "оплат", "card", "карт")
        val hint = if (amount != null && currency != null && purchaseWords.any(lower::contains)) "purchase" else "unknown"
        return NormalizedEvent(UUID.nameUUIDFromBytes("$packageName|$text".toByteArray()), packageBanks[packageName] ?: "unknown", observedAt, amount, currency, merchant, hint)
    }
}
