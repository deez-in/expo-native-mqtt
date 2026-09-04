import ExpoNativeMqtt from '../ExpoNativeMqtt';
import ExpoNativeMqttWeb from '../ExpoNativeMqtt.web';

// Mock expo-modules-core
jest.mock('expo-modules-core', () => {
  const listeners: Record<string, Function[]> = {};
  const mockNativeModule = {
    connect: jest.fn().mockResolvedValue('Connected'),
    disconnect: jest.fn().mockResolvedValue('Disconnected'),
    subscribe: jest.fn().mockResolvedValue('Subscribed'),
    unsubscribe: jest.fn().mockResolvedValue('Unsubscribed'),
    publish: jest.fn().mockResolvedValue('Published'),
  };

  return {
    requireNativeModule: jest.fn(() => mockNativeModule),
    EventEmitter: jest.fn().mockImplementation(() => ({
      addListener: jest.fn((event: string, listener: Function) => {
        if (!listeners[event]) listeners[event] = [];
        listeners[event].push(listener);
        return {
          remove: () => {
            listeners[event] = listeners[event].filter((l) => l !== listener);
          },
        };
      }),
      emit: (event: string, data: any) => {
        (listeners[event] || []).forEach((l) => l(data));
      },
    })),
  };
});

describe('ExpoNativeMqtt (TypeScript wrapper & input validation)', () => {
  it('rejects invalid brokerUrl on connect', async () => {
    await expect(ExpoNativeMqtt.connect('')).rejects.toThrow('brokerUrl must be a valid non-empty string');
    // @ts-expect-error testing null at runtime
    await expect(ExpoNativeMqtt.connect(null)).rejects.toThrow('brokerUrl must be a valid non-empty string');
  });

  it('rejects invalid topic and qos on subscribe', async () => {
    await expect(ExpoNativeMqtt.subscribe('')).rejects.toThrow('topic must be a valid non-empty string');
    await expect(ExpoNativeMqtt.subscribe('test', 5)).rejects.toThrow('QoS must be an integer between 0 and 2');
    await expect(ExpoNativeMqtt.subscribe('test', -1)).rejects.toThrow('QoS must be an integer between 0 and 2');

    // Wildcard validation
    await expect(ExpoNativeMqtt.subscribe('test/+/wrong+')).rejects.toThrow('Invalid wildcard (+): Must occupy an entire topic level');
    await expect(ExpoNativeMqtt.subscribe('test/#/wrong')).rejects.toThrow('Invalid wildcard (#): Must be the last character and occupy an entire topic level');
  });

  it('rejects invalid publish arguments', async () => {
    const validBytes = new Uint8Array([1, 2, 3]);
    await expect(ExpoNativeMqtt.publish('', validBytes)).rejects.toThrow('topic must be a valid non-empty string');
    // @ts-expect-error testing invalid type at runtime
    await expect(ExpoNativeMqtt.publish('test', null)).rejects.toThrow('payload must be a valid Uint8Array');
    // @ts-expect-error testing string type at runtime
    await expect(ExpoNativeMqtt.publish('test', 'SGVsbG8=')).rejects.toThrow('payload must be a valid Uint8Array');
    await expect(ExpoNativeMqtt.publish('test', validBytes, 4)).rejects.toThrow('QoS must be an integer between 0 and 2');

    // Publish wildcard validation
    await expect(ExpoNativeMqtt.publish('test/+', validBytes)).rejects.toThrow('Publish topics cannot contain wildcards (+ or #)');
    await expect(ExpoNativeMqtt.publish('test/#', validBytes)).rejects.toThrow('Publish topics cannot contain wildcards (+ or #)');
  });

  it('publishes valid binary Uint8Array payload', async () => {
    const payload = new Uint8Array([10, 20, 30]);
    await expect(ExpoNativeMqtt.publish('test/binary', payload, 1, true)).resolves.toBe('Published');
  });

  it('validates will options on connect', async () => {
    await expect(ExpoNativeMqtt.connect('tcp://localhost:1883', undefined, undefined, { will: { topic: '', payload: new Uint8Array([1]), qos: 0, retained: false } })).rejects.toThrow('will.topic must be a valid non-empty string');
    // @ts-expect-error testing invalid will.payload
    await expect(ExpoNativeMqtt.connect('tcp://localhost:1883', undefined, undefined, { will: { topic: 'status', payload: 'not-bytes', qos: 0, retained: false } })).rejects.toThrow('will.payload must be a valid Uint8Array');
    await expect(ExpoNativeMqtt.connect('tcp://localhost:1883', undefined, undefined, { will: { topic: 'status', payload: new Uint8Array([1]), qos: 5, retained: false } })).rejects.toThrow('will.qos must be an integer between 0 and 2');
    await expect(ExpoNativeMqtt.connect('tcp://localhost:1883', undefined, undefined, { will: { topic: 'status', payload: new Uint8Array([1]), qos: 1, retained: false } })).resolves.toBe('Connected');
  });

  it('allows adding and removing typed listeners', () => {
    const fn = jest.fn();
    const sub = ExpoNativeMqtt.addListener('onMqttConnected', fn);
    expect(sub).toBeDefined();
    expect(typeof sub.remove).toBe('function');
    sub.remove();
  });
});

describe('ExpoNativeMqtt.web (Web fallback safety)', () => {
  it('returns clean rejected promises without unhandled crash', async () => {
    await expect(ExpoNativeMqttWeb.connect('tcp://localhost:1883')).rejects.toThrow('ExpoNativeMqtt is only supported on native iOS and Android.');
    await expect(ExpoNativeMqttWeb.subscribe('test')).rejects.toThrow('ExpoNativeMqtt is only supported on native iOS and Android.');
    await expect(ExpoNativeMqttWeb.publish('test', new Uint8Array([1, 2]))).rejects.toThrow('ExpoNativeMqtt is only supported on native iOS and Android.');
  });

  it('provides safe no-op disconnect and subscription on web', async () => {
    await expect(ExpoNativeMqttWeb.disconnect()).resolves.toBe('Disconnected');
    const sub = ExpoNativeMqttWeb.addListener('onMqttMessageReceived', () => {});
    expect(sub).toBeDefined();
    expect(typeof sub.remove).toBe('function');
    expect(() => sub.remove()).not.toThrow();
  });
});
