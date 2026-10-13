package com.vydno.autocapture.transport

import org.junit.Assert.assertEquals
import org.junit.Test

class DeliveryDispositionTest {
    @Test fun revokedCredentialStopsRetryAndRequiresRepair() {
        assertEquals(DeliveryDisposition.REPAIR_REQUIRED, SendResult.Revoked.disposition())
    }
}
