package com.vydno.autocapture.capture

import org.junit.Assert.*
import org.junit.Test

class NotificationParserTest {
    private val parser = NotificationParser(setOf("bank.verified"))
    @Test fun purchaseIsNormalizedWithoutRawText() { val r = parser.parse("bank.verified", "Card payment 125.50 UAH at Coffee Bar")!!; assertEquals("125.50", r.amount); assertEquals("UAH", r.currency); assertEquals("purchase", r.operationHint) }
    @Test fun unknownPackageIsRejectedBeforeParsing() { assertNull(parser.parse("bank.unknown", "Card payment 1 UAH")) }
    @Test fun otpAndIncomeAreRejected() { listOf("OTP 123456", "Income 10 UAH").forEach { assertNull(parser.parse("bank.verified", it)) } }
    @Test fun transferAndRefundAreCapturedForReview() {
        assertEquals("transfer", parser.parse("bank.verified", "Transfer 20 UAH")!!.operationHint)
        assertEquals("refund", parser.parse("bank.verified", "Refund 10 UAH")!!.operationHint)
    }
    @Test fun incompletePurchaseNeedsReview() { assertEquals("unknown", parser.parse("bank.verified", "Card payment at Shop")!!.operationHint) }
    @Test fun configuredPackageBankMappingIsApplied() { assertEquals("mono", NotificationParser(setOf("bank.verified"), mapOf("bank.verified" to "mono")).parse("bank.verified", "Card payment 1 UAH")!!.bank) }
    @Test fun identicalNotificationsWithDifferentKeysHaveDifferentEventIds() {
        val first = parser.parse("bank.verified", "Card payment 10 UAH at Store", notificationKey = "bank.verified|1")!!
        val second = parser.parse("bank.verified", "Card payment 10 UAH at Store", notificationKey = "bank.verified|2")!!
        assertNotEquals(first.eventId, second.eventId)
    }
}
