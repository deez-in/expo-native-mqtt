# Changelog

All notable changes to this project will be documented in this file.

## [0.3.0] - 2026-08-23

### Breaking Changes & Binary Core
- **API**: Migrated to a "Binary-Only Core" design. The native modules no longer attempt to decode incoming bytes to UTF-8 strings. The `MqttMessage.payload` field has been removed and replaced entirely by `payloadBase64`.
- **API**: Consolidated `publish` and `publishBinary` into a single `publish(topic, base64Payload, qos, retained)` method that exclusively accepts Base64 strings.
- **API**: Added strict validation rejecting `+` and `#` wildcards in publish topics.
- **API**: Added validation for correct wildcard placement in subscribe topics.

### Security & Performance
- **Android**: Prevented data races by synchronizing access to mutable state (`subscribedTopics`, `reconnectAttempts`, options).
- **Security**: Added explicit credential clearing from memory on module destruction using `CharArray` (Android) and `deinit` (iOS).
- **Performance**: Moved reconnect logic off the main thread to background executors/queues (Android/iOS).
- **Performance**: Fixed dual-allocation performance issue by exclusively handling Base64 payloads over the bridge.
- **Security**: Log a warning in release builds if `allowUntrustedCA` is enabled.
- **Reliability**: Fixed a race condition between `disconnect()` and background reconnect logic.
- **Options**: Added `maxPayloadSize` limit (default 256KB) to drop oversized messages natively and prevent OOM.
- **Options**: Added support for MQTT Last Will and Testament (`will` object).

## [0.2.0] - 2026-08-23

### Security & Critical Fixes
- **iOS**: Upgraded `CocoaMQTT` to `~> 2.4` to fix CVE-2026-30867 (DoS via malformed retained packet).
- **Android**: Upgraded `HiveMQ MQTT Client` to `1.3.17` (patched Netty CVEs and updated toolchain).
- **Security**: Defaulted TLS certificate verification to strict on iOS; added opt-in `allowUntrustedCA` connect option.
- **Android**: Fixed reconnect dropping authentication credentials upon auto-reconnect.
- **Security**: Cleared stored credentials and session options in native memory upon `disconnect()`.

### Improvements & Hardening
- **iOS**: Rearchitected promise resolution to wait for broker `ConnAck` / `SubAck` / `UnsubAck` / `Disconnect` instead of premature return.
- **Reliability**: Added exponential backoff with random jitter and `maxReconnectAttempts` limit to prevent mobile battery drain.
- **Thread Safety**: Protected internal topic subscriptions across threads (`ConcurrentHashMap` on Android, `NSLock` on iOS).
- **Features**: Added `publishBinary` and incoming `payloadBase64` support for binary data.
- **Parity**: Normalized `onMqttSubscribed` event payload format across platforms to `{ topics: string[] }`.
- **Parity**: Fixed iOS default publish QoS to `0` matching Android.
- **Type Safety**: Strongly typed `MqttEventMap` for `addListener`.
- **DX**: Added input validation, TypeScript build configuration, and full `README.md` documentation.
- **Web**: Safe non-crashing web stubs for SSR and web runtime safety.

## [0.1.0] - 2026-03-25

### Added
- Initial release of `expo-native-mqtt`
- Android implementation using HiveMQ MQTT Client
- iOS implementation using CocoaMQTT
- Support for persistent sessions (`cleanSession: false`) for offline message delivery
- Automatic background reconnection and resubscription handled at native layer
