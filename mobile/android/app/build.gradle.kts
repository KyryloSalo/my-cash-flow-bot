plugins { id("com.android.application"); id("org.jetbrains.kotlin.android") }

android { namespace = "com.vydno.autocapture"; compileSdk = 35
    defaultConfig { applicationId = "com.vydno.autocapture"; minSdk = 26; targetSdk = 35; versionCode = 1; versionName = "0.1.0"; testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner" }
    compileOptions { sourceCompatibility = JavaVersion.VERSION_17; targetCompatibility = JavaVersion.VERSION_17 }
}

dependencies { implementation("androidx.core:core-ktx:1.15.0"); implementation("androidx.webkit:webkit:1.12.1"); implementation("androidx.security:security-crypto:1.1.0-alpha06"); testImplementation("junit:junit:4.13.2") }
