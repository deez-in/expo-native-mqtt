# expo-native-mqtt

High-performance native MQTT client for Expo and React Native apps on iOS and Android. Built on top of **CocoaMQTT** (iOS) and **HiveMQ MQTT Client** (Android), featuring automatic background reconnection, persistent sessions for offline message delivery, TLS support, and binary payload handling.

---

## Features

- ⚡ **Native Performance**: Uses production-tested native engines (CocoaMQTT ~>2.4 & HiveMQ 1.3.17).
- 🔄 **Auto-Reconnect with Backoff**: Native-level exponential backoff with jitter and configurable retry limits.
- 📦 **Offline Message Queuing**: Persistent session support (`cleanSession: false`) for receiving queued messages after reconnecting.
- 🔒 **TLS / SSL**: Secure MQTT connections with strict certificate validation by default. *(TODO: Implement Certificate Pinning in a future build).*
- 🧬 **Binary-Only Core**: Lightning-fast message passing. The bridge only sends and receives Base64 encoded strings, eliminating duplicate UTF-8 decoding on the native thread.
- 🎯 **Type-Safe**: Full TypeScript definitions with typed event maps.

---

## Installation

```bash
npm install expo-native-mqtt
# or
yarn add expo-native-mqtt
# or
npx expo install expo-native-mqtt
```

### Configure Expo Config Plugin

Add `expo-native-mqtt` to your plugins in `app.json` or `app.config.js`:

```json
{
  "expo": {
    "plugins": [
      "expo-native-mqtt"
    ]
  }
}
```

Then rebuild your native development client:

```bash
npx expo prebuild
npx expo run:ios
npx expo run:android
```

---

## Quick Start

```typescript
import React, { useEffect } from 'react';
import { View, Button } from 'react-native';
import ExpoNativeMqtt from 'expo-native-mqtt';
// Use a library like 'base64-js' or react-native's atob/btoa for encoding/decoding

export default function App() {
  useEffect(() => {
    // 1. Listen for connection events
    const subConnected = ExpoNativeMqtt.addListener('onMqttConnected', (event) => {
      console.log('MQTT Connected:', event.status);
      // Subscribe to topics
      ExpoNativeMqtt.subscribe('sensors/temperature', 1);
    });

    // 2. Listen for incoming messages (All messages arrive as Base64)
    const subMessage = ExpoNativeMqtt.addListener('onMqttMessageReceived', (msg) => {
      // If expecting a string, you must decode the Base64 payload in JS
      // e.g., using Buffer.from(msg.payloadBase64, 'base64').toString('utf8')
      console.log(`[${msg.topic}] (QoS ${msg.qos}): ${msg.payloadBase64}`);
    });

    // 3. Listen for errors and reconnects
    const subError = ExpoNativeMqtt.addListener('onMqttError', (err) => {
      console.warn('MQTT Error:', err.error);
    });

    const subReconnecting = ExpoNativeMqtt.addListener('onMqttReconnecting', (status) => {
      console.log(`Reconnecting attempt #${status.attempt}...`);
    });

    // 4. Connect to broker
    ExpoNativeMqtt.connect('ssl://broker.emqx.io:8883', 'username', 'password', {
      clientId: 'my-mobile-app-client',
      cleanSession: false, // enables offline delivery
      autoReconnect: true,
      reconnectDelay: 3000,
      maxReconnectAttempts: 10
    }).catch((err) => {
      console.error('Initial connection failed:', err);
    });

    return () => {
      subConnected.remove();
      subMessage.remove();
      subError.remove();
      subReconnecting.remove();
      ExpoNativeMqtt.disconnect();
    };
  }, []);

  const sendMessage = async () => {
    try {
      // You must encode your string payload to Base64 before publishing
      // e.g., 'eyB0ZW1wOiAyNC41IH0=' is Base64 for '{ temp: 24.5 }'
      await ExpoNativeMqtt.publish('sensors/temperature', 'eyB0ZW1wOiAyNC41IH0=', 1);
    } catch (err) {
      console.error('Publish failed:', err);
    }
  };

  return (
    <View style={{ flex: 1, justifyContent: 'center', alignItems: 'center' }}>
      <Button title="Publish Message" onPress={sendMessage} />
    </View>
  );
}
```

---

## API Reference

### Methods

#### `connect(brokerUrl, username?, password?, options?): Promise<string>`
Initiates connection to the specified broker URL. Resolves when the broker acknowledges the connection (`ConnAck`).

- `brokerUrl`: `tcp://host:port` or `ssl://host:port` (or `wss://host:port`)
- `username`: Optional username string
- `password`: Optional password string
- `options`: Optional `MqttConnectOptions` configuration object

#### `disconnect(): Promise<string>`
Gracefully disconnects from the broker, cancels pending auto-reconnects, and securely clears credentials from native memory.

#### `subscribe(topic, qos?): Promise<string>`
Subscribes to an MQTT topic filter. Resolves once acknowledged by the broker.
- `topic`: Topic string (supports wildcards `+` and `#`)
- `qos`: `0`, `1`, or `2` (default `0`)

#### `unsubscribe(topic): Promise<string>`
Unsubscribes from an MQTT topic filter. Resolves once acknowledged by the broker.

#### `publish(topic, base64Payload, qos?, retained?): Promise<string>`
Publishes a message to a topic. To maximize native performance, this method exclusively accepts **Base64** strings. You must convert your strings or binary data to Base64 in JavaScript before calling this method.
- `topic`: Destination topic (cannot contain wildcards)
- `base64Payload`: Base64 string representation of your payload
- `qos`: `0`, `1`, or `2` (default `0`)
- `retained`: Boolean flag (default `false`)

#### `addListener(eventName, listener): EventSubscription`
Registers a strongly-typed event listener. Returns an `EventSubscription` object with a `.remove()` method.

---

### `MqttConnectOptions`

| Option | Type | Default | Description |
|---|---|---|---|
| `clientId` | `string` | `expo-mqtt-<UUID>` | Unique client identifier |
| `cleanSession` | `boolean` | `false` | Set `false` for persistent sessions (offline delivery), `true` to discard state |
| `autoReconnect` | `boolean` | `true` | Automatically reconnect on unexpected disconnects |
| `reconnectDelay` | `number` | `5000` | Base reconnect backoff delay in milliseconds |
| `maxReconnectAttempts` | `number` | `10` | Maximum reconnection attempts (`0` for unlimited) |
| `keepAlive` | `number` | `60` | Ping interval in seconds |
| `allowUntrustedCA` | `boolean` | `false` | Opt-in for development with self-signed TLS certificates |
| `maxPayloadSize` | `number` | `262144` | Maximum incoming payload size in bytes (default 256KB). Larger payloads are dropped to prevent OOM. |
| `will` | `object` | `undefined` | MQTT Last Will and Testament configuration. |

**Last Will Object (`will`):**
- `topic` (`string`): The topic to publish the will message to.
- `payloadBase64` (`string`): The Base64 encoded payload of the will message.
- `qos` (`number`): Quality of Service level (0, 1, or 2).
- `retained` (`boolean`): Whether the will message should be retained.

---

### Events & Payloads

| Event Name | Payload Shape | Description |
|---|---|---|
| `onMqttConnected` | `{ status: string }` | Successfully connected / reconnected and resubscribed |
| `onMqttDisconnected` | `{ error: string }` | Disconnected (cleanly or unexpectedly) |
| `onMqttMessageReceived` | `MqttMessage` | Incoming message arrived |
| `onMqttError` | `{ error: string }` | Connection refused, subscribe failure, or max retries reached |
| `onMqttSubscribed` | `{ topics: string[] }` | Topics successfully acknowledged by broker |
| `onMqttUnsubscribed` | `{ topic: string }` | Topic unsubscription completed |
| `onMqttReconnecting` | `{ status: string, attempt: number }` | Reconnection attempt in progress |

#### `MqttMessage` Object

```typescript
interface MqttMessage {
  topic: string;
  payloadBase64: string; // Raw bytes encoded as Base64. You must decode this in JS if you expect a string.
  qos: number; // 0, 1, or 2
  retained: boolean;
}
```

---

## License

MIT
