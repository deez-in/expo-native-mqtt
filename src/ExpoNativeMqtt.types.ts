export type MqttEventName =
  | 'onMqttConnected'
  | 'onMqttDisconnected'
  | 'onMqttMessageReceived'
  | 'onMqttError'
  | 'onMqttSubscribed'
  | 'onMqttUnsubscribed'
  | 'onMqttReconnecting';

export interface MqttConnectOptions {
  clientId?: string;
  cleanSession?: boolean; // default: false (enables offline delivery)
  autoReconnect?: boolean; // default: true
  reconnectDelay?: number; // ms, default: 5000
  keepAlive?: number; // seconds, default: 60
  allowUntrustedCA?: boolean; // default: false (opt-in for self-signed certificates in dev)
  maxReconnectAttempts?: number; // default: 10 (0 for unlimited)
  maxPayloadSize?: number; // bytes, default: 262144 (256KB)
  will?: {
    topic: string;
    payloadBase64: string;
    qos: number;
    retained: boolean;
  };
}

export interface MqttMessage {
  topic: string;
  payloadBase64: string;
  qos: number;
  retained: boolean;
}

export interface MqttEventMap {
  onMqttConnected: { status: string };
  onMqttDisconnected: { error: string };
  onMqttMessageReceived: MqttMessage;
  onMqttError: { error: string };
  onMqttSubscribed: { topics: string[] };
  onMqttUnsubscribed: { topic: string };
  onMqttReconnecting: { status: string; attempt: number };
}
