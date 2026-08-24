import { requireNativeModule, EventEmitter, type EventSubscription } from 'expo-modules-core';
import { MqttConnectOptions, MqttEventName, MqttEventMap } from './ExpoNativeMqtt.types';

type MqttModuleEventsMap = {
  [K in MqttEventName]: (event: MqttEventMap[K]) => void;
};

const ExpoNativeMqtt = requireNativeModule('ExpoNativeMqtt');
const emitter = new EventEmitter<MqttModuleEventsMap>(ExpoNativeMqtt);

export default {
  // Methods
  connect(
    brokerUrl: string,
    username?: string,
    password?: string,
    options?: MqttConnectOptions
  ): Promise<string> {
    if (!brokerUrl || typeof brokerUrl !== 'string') {
      return Promise.reject(new Error('brokerUrl must be a valid non-empty string'));
    }
    return ExpoNativeMqtt.connect(brokerUrl, username || null, password || null, options || {});
  },

  disconnect(): Promise<string> {
    return ExpoNativeMqtt.disconnect();
  },

  subscribe(topic: string, qos: number = 0): Promise<string> {
    if (!topic || typeof topic !== 'string') {
      return Promise.reject(new Error('topic must be a valid non-empty string'));
    }

    if (topic.includes('+')) {
      const parts = topic.split('/');
      for (const part of parts) {
        if (part.includes('+') && part !== '+') {
          return Promise.reject(new Error('Invalid wildcard (+): Must occupy an entire topic level'));
        }
      }
    }
    if (topic.includes('#')) {
      if (!topic.endsWith('#') || (topic.length > 1 && topic[topic.length - 2] !== '/')) {
        return Promise.reject(new Error('Invalid wildcard (#): Must be the last character and occupy an entire topic level'));
      }
    }

    if (qos < 0 || qos > 2 || !Number.isInteger(qos)) {
      return Promise.reject(new Error('QoS must be an integer between 0 and 2'));
    }
    return ExpoNativeMqtt.subscribe(topic, qos);
  },

  unsubscribe(topic: string): Promise<string> {
    if (!topic || typeof topic !== 'string') {
      return Promise.reject(new Error('topic must be a valid non-empty string'));
    }
    return ExpoNativeMqtt.unsubscribe(topic);
  },

  publish(
    topic: string,
    base64Payload: string,
    qos: number = 0,
    retained: boolean = false
  ): Promise<string> {
    if (!topic || typeof topic !== 'string') {
      return Promise.reject(new Error('topic must be a valid non-empty string'));
    }
    if (topic.includes('+') || topic.includes('#')) {
      return Promise.reject(new Error('Publish topics cannot contain wildcards (+ or #)'));
    }
    if (!base64Payload || typeof base64Payload !== 'string') {
      return Promise.reject(new Error('base64Payload must be a valid non-empty base64 string'));
    }
    if (qos < 0 || qos > 2 || !Number.isInteger(qos)) {
      return Promise.reject(new Error('QoS must be an integer between 0 and 2'));
    }
    return ExpoNativeMqtt.publish(topic, base64Payload, qos, retained);
  },

  // Typed Events
  addListener<E extends MqttEventName>(
    eventName: E,
    listener: (event: MqttEventMap[E]) => void
  ): EventSubscription {
    return (emitter.addListener as (name: string, fn: (data: any) => void) => EventSubscription)(
      eventName,
      listener
    );
  }
};
