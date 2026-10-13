package com.vydno.autocapture.capture

import org.junit.Assert.*
import org.junit.Test

class NotificationParserTest {
    private val parser = NotificationParser(setOf("bank.verified"))
    @Test fun purchaseIsNormalizedWithoutRawText() { val r = parser.parse("bank.verified", "Card payment 125.50 UAH at Coffee Bar")!!; assertEquals("125.50", r.amount); assertEquals("UAH", r.currency); assertEquals("purchase", r.operationHint) }
    @Test fun unknownPackageIsRejectedBeforeParsing() { assertNull(parser.parse("bank.unknown", "Card payment 1 UAH")) }
    @Test fun otpTransferRefundIncomeAreRejected() { listOf("OTP 123456", "Transfer 20 UAH", "Refund 10 UAH", "Income 10 UAH").forEach { assertNull(parser.parse("bank.verified", it)) } }
    @Test fun incompletePurchaseNeedsReview() { assertEquals("unknown", parser.parse("bank.verified", "Card payment at Shop")!!.operationHint) }
    @Test fun configuredPackageBankMappingIsApplied() { assertEquals("mono", NotificationParser(setOf("bank.verified"), mapOf("bank.verified" to "mono")).parse("bank.verified", "Card payment 1 UAH")!!.bank) }
}
