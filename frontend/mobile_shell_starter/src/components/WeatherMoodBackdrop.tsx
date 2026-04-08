import React, { useEffect, useMemo, useRef } from "react";
import { Animated, Easing, StyleSheet, View } from "react-native";

import type { WeatherMood } from "./visuals";
import { colors, radius } from "../theme/tokens";

type Props = {
  mood: WeatherMood;
};

export function WeatherMoodBackdrop({ mood }: Props) {
  const rainDrops = useMemo(() => (mood === "storm" ? 14 : 8), [mood]);
  const rainValuesRef = useRef<Animated.Value[]>([]);
  const sunPulse = useRef(new Animated.Value(0)).current;
  const cloudDrift = useRef(new Animated.Value(0)).current;

  if (rainValuesRef.current.length !== rainDrops) {
    rainValuesRef.current = Array.from({ length: rainDrops }, () => new Animated.Value(-1));
  }

  useEffect(() => {
    const running: Animated.CompositeAnimation[] = [];

    if (mood === "rain" || mood === "storm") {
      rainValuesRef.current.forEach((value, index) => {
        value.setValue(-1);
        const animation = Animated.loop(
          Animated.sequence([
            Animated.delay(index * 60),
            Animated.timing(value, {
              toValue: 1,
              duration: mood === "storm" ? 860 + index * 22 : 1220 + index * 28,
              easing: Easing.linear,
              useNativeDriver: true,
            }),
            Animated.timing(value, {
              toValue: -1,
              duration: 0,
              useNativeDriver: true,
            }),
          ])
        );
        animation.start();
        running.push(animation);
      });
    }

    if (mood === "sunny") {
      sunPulse.setValue(0);
      const animation = Animated.loop(
        Animated.sequence([
          Animated.timing(sunPulse, {
            toValue: 1,
            duration: 2600,
            easing: Easing.inOut(Easing.quad),
            useNativeDriver: true,
          }),
          Animated.timing(sunPulse, {
            toValue: 0,
            duration: 2600,
            easing: Easing.inOut(Easing.quad),
            useNativeDriver: true,
          }),
        ])
      );
      animation.start();
      running.push(animation);
    }

    if (mood === "cloudy") {
      cloudDrift.setValue(0);
      const animation = Animated.loop(
        Animated.sequence([
          Animated.timing(cloudDrift, {
            toValue: 1,
            duration: 6400,
            easing: Easing.inOut(Easing.quad),
            useNativeDriver: true,
          }),
          Animated.timing(cloudDrift, {
            toValue: 0,
            duration: 6400,
            easing: Easing.inOut(Easing.quad),
            useNativeDriver: true,
          }),
        ])
      );
      animation.start();
      running.push(animation);
    }

    return () => {
      running.forEach((animation) => animation.stop());
    };
  }, [cloudDrift, mood, sunPulse]);

  return (
    <View pointerEvents="none" style={styles.container}>
      {mood === "sunny" ? (
        <View style={styles.sunLayer}>
          <Animated.View
            style={[
              styles.sunHalo,
              {
                opacity: sunPulse.interpolate({
                  inputRange: [0, 1],
                  outputRange: [0.26, 0.5],
                }),
                transform: [
                  {
                    scale: sunPulse.interpolate({
                      inputRange: [0, 1],
                      outputRange: [0.96, 1.08],
                    }),
                  },
                ],
              },
            ]}
          />
          <Animated.View
            style={[
              styles.sunCore,
              {
                opacity: sunPulse.interpolate({
                  inputRange: [0, 1],
                  outputRange: [0.55, 0.82],
                }),
              },
            ]}
          />
        </View>
      ) : null}

      {mood === "cloudy" ? (
        <View style={styles.cloudLayer}>
          <Animated.View
            style={[
              styles.cloudBlob,
              styles.cloudBlobLarge,
              {
                transform: [
                  {
                    translateX: cloudDrift.interpolate({
                      inputRange: [0, 1],
                      outputRange: [-8, 12],
                    }),
                  },
                ],
              },
            ]}
          />
          <Animated.View
            style={[
              styles.cloudBlob,
              styles.cloudBlobSmall,
              {
                transform: [
                  {
                    translateX: cloudDrift.interpolate({
                      inputRange: [0, 1],
                      outputRange: [10, -10],
                    }),
                  },
                ],
              },
            ]}
          />
        </View>
      ) : null}

      {mood === "neutral" ? (
        <View style={styles.neutralLayer}>
          <View style={[styles.neutralGlow, styles.neutralGlowLarge]} />
          <View style={[styles.neutralGlow, styles.neutralGlowSmall]} />
        </View>
      ) : null}

      {mood === "snow" ? (
        <View style={styles.snowLayer}>
          {Array.from({ length: 16 }).map((_, index) => (
            <View
              key={`snow-${index}`}
              style={[
                styles.snowDot,
                {
                  left: `${(index * 13) % 100}%`,
                  top: `${(index * 19) % 100}%`,
                  opacity: 0.12 + (index % 5) * 0.06,
                },
              ]}
            />
          ))}
        </View>
      ) : null}

      {mood === "rain" || mood === "storm" ? (
        <View style={styles.rainLayer}>
          <View style={styles.rainSheen} />
          {rainValuesRef.current.map((value, index) => (
            <Animated.View
              key={`drop-${index}`}
              style={[
                styles.rainDrop,
                mood === "storm" ? styles.rainDropStorm : null,
                {
                  left: `${(index * 7 + (index % 3) * 18) % 100}%`,
                  opacity: mood === "storm" ? 0.44 : 0.28,
                  transform: [
                    {
                      translateY: value.interpolate({
                        inputRange: [-1, 1],
                        outputRange: [-60, 240],
                      }),
                    },
                  ],
                },
              ]}
            />
          ))}
        </View>
      ) : null}

      {mood === "storm" ? <View style={styles.stormTint} /> : null}
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    ...StyleSheet.absoluteFillObject,
    borderRadius: radius.lg,
    overflow: "hidden",
  },
  rainLayer: {
    ...StyleSheet.absoluteFillObject,
  },
  rainDrop: {
    position: "absolute",
    top: -50,
    width: 1.8,
    height: 24,
    borderRadius: radius.pill,
    backgroundColor: "rgba(130,218,255,0.72)",
  },
  rainDropStorm: {
    width: 2.2,
    backgroundColor: "rgba(186,230,255,0.88)",
  },
  rainSheen: {
    ...StyleSheet.absoluteFillObject,
    backgroundColor: "rgba(35, 138, 214, 0.08)",
  },
  sunLayer: {
    ...StyleSheet.absoluteFillObject,
    justifyContent: "flex-start",
    alignItems: "flex-end",
  },
  sunHalo: {
    width: 230,
    height: 230,
    borderRadius: radius.pill,
    backgroundColor: "rgba(255,204,98,0.30)",
    marginTop: -120,
    marginRight: -84,
  },
  sunCore: {
    position: "absolute",
    width: 84,
    height: 84,
    borderRadius: radius.pill,
    backgroundColor: "rgba(255,220,136,0.58)",
    top: 10,
    right: 16,
  },
  cloudLayer: {
    ...StyleSheet.absoluteFillObject,
    justifyContent: "center",
  },
  cloudBlob: {
    position: "absolute",
    borderRadius: radius.pill,
    backgroundColor: "rgba(156,220,255,0.30)",
  },
  cloudBlobLarge: {
    width: 180,
    height: 86,
    top: 38,
    left: -34,
  },
  cloudBlobSmall: {
    width: 132,
    height: 70,
    bottom: 18,
    right: -26,
  },
  neutralLayer: {
    ...StyleSheet.absoluteFillObject,
  },
  neutralGlow: {
    position: "absolute",
    borderRadius: radius.pill,
    backgroundColor: "rgba(94, 189, 255, 0.22)",
  },
  neutralGlowLarge: {
    width: 220,
    height: 120,
    top: -28,
    right: -34,
  },
  neutralGlowSmall: {
    width: 180,
    height: 90,
    bottom: -20,
    left: -24,
    backgroundColor: "rgba(70, 165, 240, 0.18)",
  },
  snowLayer: {
    ...StyleSheet.absoluteFillObject,
  },
  snowDot: {
    position: "absolute",
    width: 5,
    height: 5,
    borderRadius: radius.pill,
    backgroundColor: "rgba(215,235,255,0.72)",
  },
  stormTint: {
    ...StyleSheet.absoluteFillObject,
    backgroundColor: "rgba(8,18,30,0.24)",
  },
});
