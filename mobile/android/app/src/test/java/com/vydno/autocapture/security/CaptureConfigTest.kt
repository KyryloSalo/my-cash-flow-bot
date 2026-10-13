package com.vydno.autocapture.security

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class CaptureConfigTest {
    @Test fun packageAndBankInputsAreNormalized() {
        assertEquals("com.example.bank", CaptureConfig.normalizePackageName("  com.example.bank "))
        assertEquals("mono", CaptureConfig.normalizeBank(" MONO "))
        assertEquals("privat24", CaptureConfig.normalizeBank(" Privat24 "))
    }

    @Test fun malformedInputsAreRejected() {
        assertNull(CaptureConfig.normalizePackageName("bank app"))
        assertNull(CaptureConfig.normalizeBank("Mono Bank"))
        assertNull(CaptureConfig.normalizeBank("raiffeisen"))
        assertNull(CaptureConfig.normalizeBank(""))
    }
}
