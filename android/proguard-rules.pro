# --- HiveMQ MQTT Client ---
-keep class com.hivemq.client.** { *; }
-dontwarn com.hivemq.client.**

# --- Netty (Core dependency for HiveMQ) ---
-keep class io.netty.** { *; }
-dontwarn io.netty.**
-dontwarn sun.misc.Unsafe
-dontwarn sun.nio.ch.DirectBuffer
-dontwarn java.nio.file.**
-dontwarn java.lang.ClassValue
-dontwarn io.netty.internal.tcnative.**

# --- JCTools (Used by Netty/HiveMQ) ---
-keep class org.jctools.** { *; }
-dontwarn org.jctools.**

# --- RxJava (Used by HiveMQ's reactive API) ---
-keep class io.reactivex.** { *; }
-dontwarn io.reactivex.**

# --- SLF4J (Logging API often pulled in) ---
-dontwarn org.slf4j.**
