const { withPodfile, withAppBuildGradle } = require("expo/config-plugins");

function withMqttModules(config) {
  // 1) iOS podfile update
  config = withPodfile(config, (podfileConfig) => {
    let podfile = podfileConfig.modResults.contents;

    const snippet = `\n  # [expo-native-mqtt] Enable modular headers for CocoaMQTT dependency\n  pod 'MqttCocoaAsyncSocket', :modular_headers => true`;

    const expoModulesRegex = /(use_expo_modules!)/;
    if (!podfile.includes('MqttCocoaAsyncSocket') && expoModulesRegex.test(podfile)) {
      podfile = podfile.replace(expoModulesRegex, `$1${snippet}`);
    }

    podfileConfig.modResults.contents = podfile;
    return podfileConfig;
  });

  // 2) Android build.gradle update
  config = withAppBuildGradle(config, (gradleConfig) => {
    let buildGradle = gradleConfig.modResults.contents;

    const packagingSnippet = `
    packaging {
        resources {
            excludes += "**/INDEX.LIST"
            excludes += "**/io.netty.versions.properties"
            excludes += "META-INF/io.netty.versions.properties"
        }
    }
`;

    const androidRegex = /(android\s*\{)/;
    if (!buildGradle.includes('excludes += "**/INDEX.LIST"') && androidRegex.test(buildGradle)) {
      buildGradle = buildGradle.replace(androidRegex, `$1${packagingSnippet}`);
    }

    gradleConfig.modResults.contents = buildGradle;
    return gradleConfig;
  });

  return config;
}

module.exports = withMqttModules;
