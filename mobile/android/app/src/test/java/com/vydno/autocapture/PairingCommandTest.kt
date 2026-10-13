package com.vydno.autocapture

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class PairingCommandTest {
    @Test
    fun acceptsOnlyWellFormedCredentialCommand() {
        val credential = "A".repeat(43)

        assertEquals(credential, PairingCommand.parse("credential:$credential"))
        assertNull(PairingCommand.parse("credential:short"))
        assertNull(PairingCommand.parse("other:$credential"))
        assertNull(PairingCommand.parse("credential:${"A".repeat(42)}!"))
    }

    @Test
    fun acceptsOnlyExactNotificationSettingsCommand() {
        assertTrue(PairingCommand.opensNotificationSettings("open-notification-settings"))
        assertFalse(PairingCommand.opensNotificationSettings("open-notification-settings-now"))
    }
}
