package expo.modules.nativemqtt

import expo.modules.kotlin.modules.Module
import expo.modules.kotlin.modules.ModuleDefinition
import expo.modules.kotlin.Promise
import com.hivemq.client.mqtt.mqtt3.Mqtt3AsyncClient
import com.hivemq.client.mqtt.mqtt3.Mqtt3Client
import com.hivemq.client.mqtt.datatypes.MqttQos
import com.hivemq.client.mqtt.MqttGlobalPublishFilter
import com.hivemq.client.mqtt.mqtt3.message.connect.connack.Mqtt3ConnAck
import android.os.Handler
import android.os.Looper
import android.util.Log
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.Arrays
import kotlin.math.min
import kotlin.math.pow
import kotlin.random.Random

data class MqttWillOption(
    val topic: String,
    val payload: ByteArray,
    val qos: Int,
    val retained: Boolean
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        other as MqttWillOption
        if (topic != other.topic) return false
        if (!payload.contentEquals(other.payload)) return false
        if (qos != other.qos) return false
        if (retained != other.retained) return false
        return true
    }

    override fun hashCode(): Int {
        var result = topic.hashCode()
        result = 31 * result + payload.contentHashCode()
        result = 31 * result + qos
        result = 31 * result + retained.hashCode()
        return result
    }
}

data class MqttConnectOptions(
    val clientId: String,
    val cleanSession: Boolean,
    val keepAlive: Int,
    val allowUntrustedCA: Boolean,
    val maxReconnectAttempts: Int,
    val reconnectDelay: Double,
    val autoReconnect: Boolean,
    val maxPayloadSize: Int,
    val will: MqttWillOption?
)

class ExpoNativeMqttModule : Module() {
    private var client: Mqtt3AsyncClient? = null
    private var storedOptions: MqttConnectOptions? = null
    private val subscribedTopics = ConcurrentHashMap<String, Int>() // topic -> qos
    private var isDisconnectingManually = false
    private var reconnectExecutor: ScheduledExecutorService? = null
    private var storedUsername: String? = null
    private var storedPassword: CharArray? = null
    private var reconnectAttempts = 0
    private var isReplacingClient = false
    private val lock = Any()

    override fun definition() = ModuleDefinition {
        Name("ExpoNativeMqtt")

        Events(
            "onMqttConnected",
            "onMqttDisconnected",
            "onMqttError",
            "onMqttMessageReceived",
            "onMqttSubscribed",
            "onMqttUnsubscribed",
            "onMqttReconnecting"
        )

        OnDestroy {
            synchronized(lock) {
                reconnectExecutor?.shutdownNow()
                reconnectExecutor = null
                storedPassword?.let { Arrays.fill(it, '\u0000') }
                storedPassword = null
                storedUsername = null
                client?.disconnect()
                client = null
            }
        }

        AsyncFunction("connect") { brokerUrl: String, username: String?, password: String?, options: Map<String, Any>, willPayload: ByteArray?, promise: Promise ->
            try {
                // Parse options securely
                val parsedOptions = MqttConnectOptions(
                    clientId = options["clientId"] as? String ?: "expo-mqtt-${UUID.randomUUID()}",
                    cleanSession = options["cleanSession"] as? Boolean ?: false,
                    keepAlive = (options["keepAlive"] as? Number)?.toInt() ?: 60,
                    allowUntrustedCA = options["allowUntrustedCA"] as? Boolean ?: false,
                    maxReconnectAttempts = (options["maxReconnectAttempts"] as? Number)?.toInt() ?: 10,
                    reconnectDelay = (options["reconnectDelay"] as? Number)?.toDouble() ?: 5000.0,
                    autoReconnect = options["autoReconnect"] as? Boolean ?: true,
                    maxPayloadSize = (options["maxPayloadSize"] as? Number)?.toInt() ?: 262144,
                    will = (options["will"] as? Map<String, Any>)?.let { willMap ->
                        MqttWillOption(
                            topic = willMap["topic"] as? String ?: "",
                            payload = willPayload ?: ByteArray(0),
                            qos = (willMap["qos"] as? Number)?.toInt() ?: 0,
                            retained = willMap["retained"] as? Boolean ?: false
                        )
                    }
                )

                synchronized(lock) {
                    storedOptions = parsedOptions
                    reconnectAttempts = 0
                    storedUsername = username
                    storedPassword?.let { Arrays.fill(it, '\u0000') }
                    storedPassword = password?.toCharArray()

                    if (reconnectExecutor == null || reconnectExecutor!!.isShutdown) {
                        reconnectExecutor = Executors.newSingleThreadScheduledExecutor()
                    }

                    // Clean up existing client to prevent resource leaks
                    isReplacingClient = true
                    client?.let { existingClient ->
                        try { 
                            existingClient.disconnect().whenComplete { _, _ ->
                                synchronized(lock) {
                                    isReplacingClient = false
                                }
                            }
                        } catch (_: Exception) {
                            isReplacingClient = false
                        }
                    } ?: run {
                        isReplacingClient = false
                    }
                    client = null
                    isDisconnectingManually = false
                }

                // Parse Broker URL
                var host = brokerUrl
                var port = 1883
                try {
                    val uriString = if (!brokerUrl.contains("://")) "tcp://$brokerUrl" else brokerUrl
                    val uri = java.net.URI(uriString)
                    host = uri.host ?: brokerUrl
                    if (uri.port != -1) {
                        port = uri.port
                    }
                } catch (e: Exception) {
                    val cleanUrl = brokerUrl.replace("tcp://", "").replace("ssl://", "").replace("wss://", "").replace("ws://", "")
                    val parts = cleanUrl.split(":")
                    host = parts.getOrNull(0) ?: brokerUrl
                    port = parts.getOrNull(1)?.toIntOrNull() ?: 1883
                }

                val builder = Mqtt3Client.builder()
                    .identifier(parsedOptions.clientId)
                    .serverHost(host)
                    .serverPort(port)
                    .addDisconnectedListener { context ->
                        val error = context.cause
                        sendEvent("onMqttDisconnected", mapOf("error" to (error?.message ?: "Disconnected")))
                        
                        var shouldReconnect = false
                        synchronized(lock) {
                            shouldReconnect = !isDisconnectingManually && !isReplacingClient
                        }
                        if (shouldReconnect) {
                            scheduleReconnect()
                        }
                    }

                // SSL Setup
                if (brokerUrl.startsWith("ssl://") || brokerUrl.startsWith("wss://")) {
                    if (parsedOptions.allowUntrustedCA) {
                        val isDebug = (appContext.reactContext?.applicationInfo?.flags?.and(android.content.pm.ApplicationInfo.FLAG_DEBUGGABLE) ?: 0) != 0
                        if (!isDebug) {
                            Log.w("ExpoNativeMqtt", "allowUntrustedCA is enabled in release build. Use caution.")
                        }
                        
                        val trustAllTmf = object : javax.net.ssl.TrustManagerFactory(
                            object : javax.net.ssl.TrustManagerFactorySpi() {
                                override fun engineInit(ks: java.security.KeyStore?) {}
                                override fun engineInit(spec: javax.net.ssl.ManagerFactoryParameters?) {}
                                override fun engineGetTrustManagers(): Array<javax.net.ssl.TrustManager> = arrayOf(
                                    object : javax.net.ssl.X509TrustManager {
                                        override fun checkClientTrusted(chain: Array<out java.security.cert.X509Certificate>?, authType: String?) {}
                                        override fun checkServerTrusted(chain: Array<out java.security.cert.X509Certificate>?, authType: String?) {}
                                        override fun getAcceptedIssuers(): Array<java.security.cert.X509Certificate> = arrayOf()
                                    }
                                )
                            }, null, "TrustAll"
                        ) {}
                        
                        // TODO: Implement certificate pinning in a future build.
                        // The code below does certificate pinning (when implemented)
                        builder.sslConfig()
                            .trustManagerFactory(trustAllTmf)
                            // We purposefully DO NOT disable hostnameVerifier here anymore for security
                            .applySslConfig()
                    } else {
                        // TODO: Implement certificate pinning in a future build.
                        // The code below does certificate pinning (when implemented)
                        builder.sslWithDefaultConfig()
                    }
                }

                val newClient = builder.buildAsync()
                synchronized(lock) {
                    client = newClient
                }

                // Global Publish listener
                newClient.publishes(MqttGlobalPublishFilter.ALL) { publish ->
                    val payloadBytes = publish.payloadAsBytes
                    
                    var maxSize = 262144
                    synchronized(lock) {
                        maxSize = storedOptions?.maxPayloadSize ?: 262144
                    }
                    
                    if (payloadBytes.size > maxSize) {
                        sendEvent("onMqttError", mapOf("error" to "Message dropped: payload size (${payloadBytes.size} bytes) exceeds maximum (${maxSize} bytes)"))
                        return@publishes
                    }

                    sendEvent("onMqttMessageReceived", mapOf(
                        "topic" to publish.topic.toString(),
                        "payload" to payloadBytes,
                        "qos" to publish.qos.code,
                        "retained" to publish.isRetain
                    ))
                }

                // Connect builder
                val connectBuilder = newClient.connectWith()
                    .cleanSession(parsedOptions.cleanSession)
                    .keepAlive(parsedOptions.keepAlive)

                synchronized(lock) {
                    if (!storedUsername.isNullOrEmpty() && storedPassword != null) {
                        connectBuilder.simpleAuth()
                            .username(storedUsername!!)
                            .password(String(storedPassword!!).toByteArray())
                            .applySimpleAuth()
                    }
                }

                parsedOptions.will?.let { willOpt ->
                    try {
                        val willQos = MqttQos.fromCode(willOpt.qos) ?: MqttQos.AT_MOST_ONCE
                        connectBuilder.willPublish()
                            .topic(willOpt.topic)
                            .payload(willOpt.payload)
                            .qos(willQos)
                            .retain(willOpt.retained)
                            .applyWillPublish()
                    } catch (e: Exception) {
                        promise.reject("CONNECT_ERROR", "Failed to configure will message: ${e.message}", e)
                        return@AsyncFunction
                    }
                }

                connectBuilder.send().whenComplete { ack: Mqtt3ConnAck?, throwable: Throwable? ->
                    if (throwable != null) {
                        promise.reject("CONNECT_FAILED", throwable.message, throwable)
                        sendEvent("onMqttError", mapOf("error" to throwable.message))
                    } else {
                        synchronized(lock) {
                            reconnectAttempts = 0
                        }
                        promise.resolve("Connected")
                        sendEvent("onMqttConnected", mapOf("status" to "connected"))
                        resubscribeAll()
                    }
                }
            } catch (e: Exception) {
                promise.reject("CONNECT_ERROR", e.message, e)
            }
        }

        AsyncFunction("disconnect") { promise: Promise ->
            val c: Mqtt3AsyncClient?
            synchronized(lock) {
                isDisconnectingManually = true
                reconnectExecutor?.shutdownNow()
                reconnectExecutor = null
                storedUsername = null
                storedPassword?.let { Arrays.fill(it, '\u0000') }
                storedPassword = null
                storedOptions = null
                reconnectAttempts = 0
                c = client
            }
            
            c?.disconnect()?.whenComplete { _, _ ->
                synchronized(lock) {
                    if (client === c) {
                        client = null
                    }
                }
                promise.resolve("Disconnected")
            } ?: promise.resolve("No active client")
        }

        AsyncFunction("subscribe") { topic: String, qos: Int, promise: Promise ->
            val c: Mqtt3AsyncClient?
            synchronized(lock) {
                subscribedTopics[topic] = qos
                c = client
            }
            
            c ?: run {
                promise.reject("NOT_CONNECTED", "No active MQTT connection", null)
                return@AsyncFunction
            }

            val mqttQos = MqttQos.fromCode(qos) ?: MqttQos.AT_MOST_ONCE
            
            c.subscribeWith()
                ?.topicFilter(topic)
                ?.qos(mqttQos)
                ?.send()
                ?.whenComplete { _, throwable ->
                    if (throwable != null) {
                        promise.reject("SUBSCRIBE_FAILED", throwable.message, throwable)
                    } else {
                        promise.resolve("Subscribed")
                        sendEvent("onMqttSubscribed", mapOf("topics" to listOf(topic)))
                    }
                }
        }

        AsyncFunction("unsubscribe") { topic: String, promise: Promise ->
            val c: Mqtt3AsyncClient?
            synchronized(lock) {
                subscribedTopics.remove(topic)
                c = client
            }
            
            c ?: run {
                promise.reject("NOT_CONNECTED", "No active MQTT connection", null)
                return@AsyncFunction
            }
            
            c.unsubscribeWith()
                ?.topicFilter(topic)
                ?.send()
                ?.whenComplete { _, throwable ->
                    if (throwable != null) {
                        promise.reject("UNSUBSCRIBE_FAILED", throwable.message, throwable)
                    } else {
                        promise.resolve("Unsubscribed")
                        sendEvent("onMqttUnsubscribed", mapOf("topic" to topic))
                    }
                }
        }

        AsyncFunction("publish") { topic: String, payload: ByteArray, qos: Int, retained: Boolean, promise: Promise ->
            val c: Mqtt3AsyncClient?
            synchronized(lock) {
                c = client
            }
            
            c ?: run {
                promise.reject("NOT_CONNECTED", "No active MQTT connection", null)
                return@AsyncFunction
            }
            
            try {
                val mqttQos = MqttQos.fromCode(qos) ?: MqttQos.AT_MOST_ONCE

                c.publishWith()
                    ?.topic(topic)
                    ?.payload(payload)
                    ?.qos(mqttQos)
                    ?.retain(retained)
                    ?.send()
                    ?.whenComplete { _, throwable ->
                        if (throwable != null) {
                            promise.reject("PUBLISH_FAILED", throwable.message, throwable)
                        } else {
                            promise.resolve("Published")
                        }
                    }
            } catch (e: Exception) {
                promise.reject("PUBLISH_ERROR", e.message, e)
            }
        }
    }

    private fun scheduleReconnect() {
        var optionsToUse: MqttConnectOptions?
        var isManual: Boolean
        synchronized(lock) {
            optionsToUse = storedOptions
            isManual = isDisconnectingManually
        }
        
        val autoReconnect = optionsToUse?.autoReconnect ?: true
        if (!autoReconnect || isManual) return

        val maxAttempts = optionsToUse?.maxReconnectAttempts ?: 10
        
        var currentAttempt: Int
        synchronized(lock) {
            if (maxAttempts > 0 && reconnectAttempts >= maxAttempts) {
                sendEvent("onMqttError", mapOf("error" to "Max reconnect attempts reached ($maxAttempts)"))
                return
            }
            reconnectAttempts++
            currentAttempt = reconnectAttempts
        }

        val baseDelay = optionsToUse?.reconnectDelay ?: 5000.0
        val attemptFactor = 2.0.pow(min(currentAttempt.toDouble(), 6.0))
        val calculatedDelay = min(baseDelay * attemptFactor, 60000.0)
        val jitter = Random.nextDouble(0.75, 1.25)
        val delayMs = (calculatedDelay * jitter).toLong()

        synchronized(lock) {
            if (isDisconnectingManually) return
            
            if (reconnectExecutor == null || reconnectExecutor!!.isShutdown) {
                reconnectExecutor = Executors.newSingleThreadScheduledExecutor()
            }
            
            reconnectExecutor?.schedule({
                var checkManual: Boolean
                var c: Mqtt3AsyncClient?
                synchronized(lock) {
                    checkManual = isDisconnectingManually
                    c = client
                }
                if (checkManual) return@schedule

                c?.let {
                    sendEvent("onMqttReconnecting", mapOf("status" to "reconnecting", "attempt" to currentAttempt))
                    val cleanSession = optionsToUse?.cleanSession ?: false
                    val keepAlive = optionsToUse?.keepAlive ?: 60
                    
                    val connectBuilder = it.connectWith()
                        .cleanSession(cleanSession)
                        .keepAlive(keepAlive)
                    
                    synchronized(lock) {
                        if (!storedUsername.isNullOrEmpty() && storedPassword != null) {
                            connectBuilder.simpleAuth()
                                .username(storedUsername!!)
                                .password(String(storedPassword!!).toByteArray())
                                .applySimpleAuth()
                        }
                    }
                    
                    // Re-apply will message on reconnect
                    optionsToUse?.will?.let { willOpt ->
                        try {
                            val willQos = MqttQos.fromCode(willOpt.qos) ?: MqttQos.AT_MOST_ONCE
                            connectBuilder.willPublish()
                                .topic(willOpt.topic)
                                .payload(willOpt.payload)
                                .qos(willQos)
                                .retain(willOpt.retained)
                                .applyWillPublish()
                        } catch (e: Exception) {
                            // Ignore exception during reconnect will parsing
                        }
                    }
                    
                    connectBuilder.send().whenComplete { _, throwable ->
                        if (throwable == null) {
                            synchronized(lock) {
                                reconnectAttempts = 0
                            }
                            sendEvent("onMqttConnected", mapOf("status" to "connected"))
                            resubscribeAll()
                        }
                    }
                }
            }, delayMs, TimeUnit.MILLISECONDS)
        }
    }

    private fun resubscribeAll() {
        val subscriptions = mutableMapOf<String, Int>()
        val c: Mqtt3AsyncClient?
        synchronized(lock) {
            subscriptions.putAll(subscribedTopics)
            c = client
        }
        
        for ((topic, qos) in subscriptions) {
            val mqttQos = MqttQos.fromCode(qos) ?: MqttQos.AT_MOST_ONCE
            c?.subscribeWith()
                ?.topicFilter(topic)
                ?.qos(mqttQos)
                ?.send()
        }
    }
}
