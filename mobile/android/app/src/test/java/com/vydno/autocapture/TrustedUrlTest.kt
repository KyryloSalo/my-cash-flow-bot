package com.vydno.autocapture

import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class TrustedUrlTest {
    @Test
    fun permitsOnlyHttpsProductionOrigin() {
        assertTrue(TrustedUrl.isAllowed("https://vydno.capital/app/"))
        assertFalse(TrustedUrl.isAllowed("http://vydno.capital/app/"))
        assertFalse(TrustedUrl.isAllowed("https://vydno.capital.evil.example/app/"))
        assertFalse(TrustedUrl.isAllowed("https://admin.vydno.capital/app/"))
        assertFalse(TrustedUrl.isAllowed("not-a-url"))
    }
}
