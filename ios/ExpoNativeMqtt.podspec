Pod::Spec.new do |s|
  s.name           = 'ExpoNativeMqtt'
  s.version        = '0.6.0'
  s.summary        = 'Native MQTT client for Expo with auto-reconnect and offline delivery'
  s.description    = 'Native MQTT client for Expo using CocoaMQTT internally'
  s.author         = 'expo-native-mqtt contributors'
  s.homepage       = 'https://github.com/deez-in/expo-native-mqtt'
  s.license        = { :type => 'MIT' }
  s.platforms      = {
    :ios => '15.1'
  }
  s.source         = { :git => 'https://github.com/deez-in/expo-native-mqtt.git', :tag => s.version.to_s }
  s.static_framework = true

  s.dependency 'ExpoModulesCore'
  s.dependency 'CocoaMQTT', '~> 2.4'

  s.source_files = "**/*.{h,m,mm,swift}"
end
