import type { EventSubscription } from 'expo-modules-core';
import { MqttConnectOptions, MqttEventName, MqttEventMap } from './ExpoNativeMqtt.types';

const webNotSupported = () =>
  Promise.reject(new Error('ExpoNativeMqtt is only supported on native iOS and Android.'));

export default {
  connect(
    _brokerUrl: string,
    _username?: string,
    _password?: string,
    _options?: MqttConnectOptions
  ): Promise<string> {
    return webNotSupported();
  },

  disconnect(): Promise<string> {
    return Promise.resolve('Disconnected');
  },

  subscribe(_topic: string, _qos: number = 0): Promise<string> {
    return webNotSupported();
  },

  unsubscribe(_topic: string): Promise<string> {
    return webNotSupported();
  },

  publish(
    _topic: string,
    _payload: Uint8Array,
    _qos: number = 0,
    _retained: boolean = false
  ): Promise<string> {
    return webNotSupported();
  },

  addListener<E extends MqttEventName>(
    _eventName: E,
    _listener: (event: MqttEventMap[E]) => void
  ): EventSubscription {
    return {
      remove: () => {}
    };
  }
};
