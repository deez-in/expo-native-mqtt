import ExpoModulesCore
import CocoaMQTT
import Foundation

struct MqttWillOption {
  let topic: String
  let payloadBase64: String
  let qos: Int
  let retained: Bool
}

struct MqttConnectOptions {
  let clientId: String
  let cleanSession: Bool
  let keepAlive: UInt16
  let allowUntrustedCA: Bool
  let maxReconnectAttempts: Int
  let reconnectDelay: Double
  let autoReconnect: Bool
  let maxPayloadSize: Int
  let will: MqttWillOption?
}

public class ExpoNativeMqttModule: Module {
  private var mqtt: CocoaMQTT?
  private var storedOptions: MqttConnectOptions?
  private var subscribedTopics: [String: Int] = [:] // topic -> qos
  private var reconnectTimer: Timer?
  private var isDisconnectingManually = false
  private var reconnectAttempts = 0
  private let lock = NSLock()

  private var connectPromise: Promise?
  private var disconnectPromise: Promise?
  private var pendingSubscribePromises: [String: Promise] = [:]
  private var pendingUnsubscribePromises: [String: Promise] = [:]
  private var pendingPublishPromises: [UInt16: Promise] = [:]
  
  deinit {
    lock.lock()
    reconnectTimer?.invalidate()
    reconnectTimer = nil
    mqtt?.username = nil
    mqtt?.password = nil
    mqtt = nil
    lock.unlock()
  }

  public func definition() -> ModuleDefinition {
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

    AsyncFunction("connect") { (brokerUrl: String, username: String?, password: String?, options: [String: Any], promise: Promise) in
      guard let url = URL(string: brokerUrl) else {
        promise.reject("INVALID_URL", "Invalid broker URL")
        return
      }

      let host = url.host ?? brokerUrl
      let port = UInt16(url.port ?? 1883)
      
      let willOpt: MqttWillOption?
      if let willDict = options["will"] as? [String: Any],
         let topic = willDict["topic"] as? String,
         let payloadBase64 = willDict["payloadBase64"] as? String {
        willOpt = MqttWillOption(
          topic: topic,
          payloadBase64: payloadBase64,
          qos: willDict["qos"] as? Int ?? 0,
          retained: willDict["retained"] as? Bool ?? false
        )
      } else {
        willOpt = nil
      }

      let parsedOptions = MqttConnectOptions(
        clientId: options["clientId"] as? String ?? "expo-mqtt-\(UUID().uuidString)",
        cleanSession: options["cleanSession"] as? Bool ?? false,
        keepAlive: options["keepAlive"] as? UInt16 ?? 60,
        allowUntrustedCA: options["allowUntrustedCA"] as? Bool ?? false,
        maxReconnectAttempts: options["maxReconnectAttempts"] as? Int ?? 10,
        reconnectDelay: options["reconnectDelay"] as? Double ?? 5000.0,
        autoReconnect: options["autoReconnect"] as? Bool ?? true,
        maxPayloadSize: options["maxPayloadSize"] as? Int ?? 262144,
        will: willOpt
      )

      self.lock.lock()
      self.storedOptions = parsedOptions
      self.isDisconnectingManually = false
      self.reconnectAttempts = 0
      self.reconnectTimer?.invalidate()
      self.reconnectTimer = nil
      self.connectPromise = promise
      self.lock.unlock()

      // Disconnect existing client to prevent resource leaks
      if let existingClient = self.mqtt {
        existingClient.delegate = nil
        existingClient.disconnect()
        self.mqtt = nil
      }

      self.mqtt = CocoaMQTT(clientID: parsedOptions.clientId, host: host, port: port)
      self.mqtt?.username = username
      self.mqtt?.password = password
      self.mqtt?.keepAlive = parsedOptions.keepAlive
      self.mqtt?.cleanSession = parsedOptions.cleanSession
      self.mqtt?.delegate = self
      
      if let will = parsedOptions.will, let willData = Data(base64Encoded: will.payloadBase64) {
        let cocoaQos = CocoaMQTTQoS(rawValue: UInt8(will.qos)) ?? .qos0
        let willMessage = CocoaMQTTMessage(topic: will.topic, payload: [UInt8](willData), qos: cocoaQos, retained: will.retained)
        self.mqtt?.willMessage = willMessage
      }

      // SSL Setup
      if brokerUrl.hasPrefix("ssl://") || brokerUrl.hasPrefix("wss://") {
        self.mqtt?.enableSSL = true
        if parsedOptions.allowUntrustedCA {
          #if !DEBUG
          self.lock.lock()
          let capturedPromise = self.connectPromise
          self.connectPromise = nil
          self.lock.unlock()
          capturedPromise?.reject("SECURITY_ERROR", "allowUntrustedCA is not permitted in release builds due to security risks.")
          return
          #endif
          self.mqtt?.allowUntrustCACertificate = true
        }
        // TODO: Implement certificate pinning in a future build.
        // The code below does certificate pinning (when implemented)
      }

      let success = self.mqtt?.connect() ?? false
      if !success {
        self.lock.lock()
        let capturedPromise = self.connectPromise
        self.connectPromise = nil
        self.lock.unlock()
        capturedPromise?.reject("CONNECT_FAILED", "Failed to initiate connection")
      }
    }

    AsyncFunction("disconnect") { (promise: Promise) in
      self.lock.lock()
      self.isDisconnectingManually = true
      self.reconnectTimer?.invalidate()
      self.reconnectTimer = nil
      self.storedOptions = nil
      self.reconnectAttempts = 0
      self.disconnectPromise = promise
      self.lock.unlock()

      guard let client = self.mqtt else {
        self.lock.lock()
        self.disconnectPromise = nil
        self.lock.unlock()
        promise.resolve("Disconnected")
        return
      }

      client.disconnect()
    }

    AsyncFunction("subscribe") { (topic: String, qos: Int, promise: Promise) in
      self.lock.lock()
      self.subscribedTopics[topic] = qos
      self.pendingSubscribePromises[topic] = promise
      let client = self.mqtt
      self.lock.unlock()
      
      guard let mqttClient = client else {
        promise.reject("NOT_CONNECTED", "No active MQTT connection")
        return
      }

      let cocoaQos = CocoaMQTTQoS(rawValue: UInt8(qos)) ?? .qos0
      mqttClient.subscribe(topic, qos: cocoaQos)
    }

    AsyncFunction("unsubscribe") { (topic: String, promise: Promise) in
      self.lock.lock()
      self.subscribedTopics.removeValue(forKey: topic)
      self.pendingUnsubscribePromises[topic] = promise
      let client = self.mqtt
      self.lock.unlock()
      
      guard let mqttClient = client else {
        promise.reject("NOT_CONNECTED", "No active MQTT connection")
        return
      }

      mqttClient.unsubscribe(topic)
    }

    AsyncFunction("publish") { (topic: String, base64Payload: String, qos: Int, retained: Bool, promise: Promise) in
      self.lock.lock()
      let client = self.mqtt
      self.lock.unlock()
      
      guard let mqttClient = client else {
        promise.reject("NOT_CONNECTED", "No active MQTT connection")
        return
      }
      
      guard let data = Data(base64Encoded: base64Payload) else {
        promise.reject("INVALID_PAYLOAD", "Invalid base64 payload")
        return
      }
      let cocoaQos = CocoaMQTTQoS(rawValue: UInt8(qos)) ?? .qos0
      let bytes = [UInt8](data)
      let msgId = mqttClient.publish(topic, withBytes: bytes, qos: cocoaQos, retained: retained)
      
      if qos == 0 || msgId == nil {
        promise.resolve("Published")
      } else {
        self.lock.lock()
        self.pendingPublishPromises[msgId!] = promise
        self.lock.unlock()
      }
    }
  }

  fileprivate func scheduleReconnect() {
    lock.lock()
    guard !isDisconnectingManually else {
      lock.unlock()
      return
    }
    guard let optionsToUse = storedOptions, optionsToUse.autoReconnect else {
      lock.unlock()
      return
    }

    let maxAttempts = optionsToUse.maxReconnectAttempts
    if maxAttempts > 0 && reconnectAttempts >= maxAttempts {
      lock.unlock()
      sendEvent("onMqttError", ["error": "Max reconnect attempts reached (\(maxAttempts))"])
      return
    }

    let baseDelay = optionsToUse.reconnectDelay
    let factor = pow(2.0, Double(min(reconnectAttempts, 6)))
    let cappedDelay = min(baseDelay * factor, 60000.0)
    let jitter = Double.random(in: 0.75...1.25)
    let delaySeconds = (cappedDelay * jitter) / 1000.0

    reconnectAttempts += 1
    let currentAttempt = reconnectAttempts
    lock.unlock()

    DispatchQueue.main.async { [weak self] in
      guard let self = self else { return }
      self.lock.lock()
      self.reconnectTimer?.invalidate()
      self.reconnectTimer = Timer.scheduledTimer(withTimeInterval: delaySeconds, repeats: false) { [weak self] _ in
        guard let self = self else { return }
        self.lock.lock()
        let isManual = self.isDisconnectingManually
        self.lock.unlock()
        
        if !isManual {
          self.sendEvent("onMqttReconnecting", ["status": "reconnecting", "attempt": currentAttempt])
          DispatchQueue.global(qos: .userInitiated).async { [weak self] in
            guard let self = self else { return }
            self.lock.lock()
            let client = self.mqtt
            self.lock.unlock()
            _ = client?.connect()
          }
        }
      }
      self.lock.unlock()
    }
  }

  fileprivate func resubscribeAll() {
    lock.lock()
    let topicsCopy = subscribedTopics
    let client = mqtt
    lock.unlock()

    for (topic, qos) in topicsCopy {
      let cocoaQos = CocoaMQTTQoS(rawValue: UInt8(qos)) ?? .qos0
      client?.subscribe(topic, qos: cocoaQos)
    }
  }
}

extension ExpoNativeMqttModule: CocoaMQTTDelegate {
  public func mqtt(_ mqtt: CocoaMQTT, didConnectAck ack: CocoaMQTTConnAck) {
    lock.lock()
    let promise = connectPromise
    connectPromise = nil
    reconnectAttempts = 0
    lock.unlock()

    if ack == .accept {
      promise?.resolve("Connected")
      sendEvent("onMqttConnected", ["status": "connected"])
      resubscribeAll()
    } else {
      promise?.reject("CONNECT_REFUSED", "Connection refused: \(ack)")
      sendEvent("onMqttError", ["error": "Connection refused: \(ack)"])
    }
  }

  public func mqttDidDisconnect(_ mqtt: CocoaMQTT, withError error: Error?) {
    lock.lock()
    let promise = disconnectPromise
    disconnectPromise = nil
    let isManual = isDisconnectingManually
    let publishPromises = pendingPublishPromises
    pendingPublishPromises.removeAll()
    let subscribePromises = pendingSubscribePromises
    pendingSubscribePromises.removeAll()
    let unsubscribePromises = pendingUnsubscribePromises
    pendingUnsubscribePromises.removeAll()
    lock.unlock()

    // Reject any pending publish promises that will never be acked
    for (_, p) in publishPromises {
      p.reject("DISCONNECTED", "Disconnected before publish was acknowledged")
    }
    for (_, p) in subscribePromises {
      p.reject("DISCONNECTED", "Disconnected before subscribe was acknowledged")
    }
    for (_, p) in unsubscribePromises {
      p.reject("DISCONNECTED", "Disconnected before unsubscribe was acknowledged")
    }

    promise?.resolve("Disconnected")
    sendEvent("onMqttDisconnected", [
      "error": error?.localizedDescription ?? "clean disconnect"
    ])

    if isManual {
      self.mqtt?.username = nil
      self.mqtt?.password = nil
      self.mqtt = nil
    }

    scheduleReconnect()
  }

  public func mqtt(_ mqtt: CocoaMQTT, didReceiveMessage message: CocoaMQTTMessage, id: UInt16) {
    let payloadBytes = message.payload
    
    lock.lock()
    let maxSize = storedOptions?.maxPayloadSize ?? 262144
    lock.unlock()
    
    if payloadBytes.count > maxSize {
      sendEvent("onMqttError", ["error": "Message dropped: payload size (\(payloadBytes.count) bytes) exceeds maximum (\(maxSize) bytes)"])
      return
    }

    let payloadBase64 = Data(payloadBytes).base64EncodedString()

    sendEvent("onMqttMessageReceived", [
      "topic": message.topic,
      "payloadBase64": payloadBase64,
      "qos": Int(message.qos.rawValue),
      "retained": message.retained
    ])
  }

  public func mqtt(_ mqtt: CocoaMQTT, didSubscribeTopics success: NSDictionary, failed: [String]) {
    let topics = success.allKeys.compactMap { $0 as? String }
    lock.lock()
    for topic in topics {
      pendingSubscribePromises.removeValue(forKey: topic)?.resolve("Subscribed")
    }
    for topic in failed {
      pendingSubscribePromises.removeValue(forKey: topic)?.reject("SUBSCRIBE_FAILED", "Failed to subscribe to \(topic)")
    }
    lock.unlock()

    sendEvent("onMqttSubscribed", ["topics": topics])
  }

  public func mqtt(_ mqtt: CocoaMQTT, didUnsubscribeTopics topics: [String]) {
    lock.lock()
    for topic in topics {
      pendingUnsubscribePromises.removeValue(forKey: topic)?.resolve("Unsubscribed")
    }
    lock.unlock()

    for topic in topics {
      sendEvent("onMqttUnsubscribed", ["topic": topic])
    }
  }

  public func mqtt(_ mqtt: CocoaMQTT, didPublishMessage message: CocoaMQTTMessage, id: UInt16) { }

  public func mqtt(_ mqtt: CocoaMQTT, didPublishAck id: UInt16) {
    lock.lock()
    let promise = pendingPublishPromises.removeValue(forKey: id)
    lock.unlock()
    promise?.resolve("Published")
  }

  public func mqttDidPing(_ mqtt: CocoaMQTT) { }

  public func mqttDidReceivePong(_ mqtt: CocoaMQTT) { }
}
