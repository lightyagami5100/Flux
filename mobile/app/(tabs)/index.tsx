import React, { useState, useRef, useEffect } from 'react';
import { StyleSheet, Text, View, TouchableOpacity, Alert } from 'react-native';
import { CameraView, useCameraPermissions } from 'expo-camera';
import * as Location from 'expo-location';
import { Accelerometer } from 'expo-sensors';

export default function TabOneScreen() {
  const [permission, requestPermission] = useCameraPermissions();
  const [isPatrolling, setIsPatrolling] = useState(false);
  const [bumpCount, setBumpCount] = useState(0);
  const [lastBumpTime, setLastBumpTime] = useState(0);
  
  const cameraRef = useRef<CameraView>(null);
  const subscriptionRef = useRef<any>(null);
  
  // Configurable backend URL via environment variable with localhost default
  const BACKEND_URL = process.env.EXPO_PUBLIC_API_URL || 'http://localhost:8000/v1/ingest/upload';

  useEffect(() => {
    (async () => {
      const { status } = await Location.requestForegroundPermissionsAsync();
      if (status !== 'granted') {
        Alert.alert('Location permission needed for bump mapping.');
      }
    })();
    return () => {
      _unsubscribe();
    };
  }, []);

  const _subscribe = () => {
    // 100ms update interval for accelerometer
    Accelerometer.setUpdateInterval(100);
    subscriptionRef.current = Accelerometer.addListener(accelerometerData => {
      detectBump(accelerometerData);
    });
  };

  const _unsubscribe = () => {
    if (subscriptionRef.current) {
      subscriptionRef.current.remove();
      subscriptionRef.current = null;
    }
  };

  const detectBump = async ({ x, y, z }: { x: number, y: number, z: number }) => {
    // Calculate magnitude of acceleration vector
    const gForce = Math.sqrt(x * x + y * y + z * z);
    
    // 1g is normal gravity. A spike > 2.0g usually indicates a significant bump
    if (gForce > 2.0) {
      const now = Date.now();
      // Debounce bumps (only trigger once every 3 seconds)
      if (now - lastBumpTime > 3000) {
        setLastBumpTime(now);
        setBumpCount(prev => prev + 1);
        handleBumpDetected();
      }
    }
  };

  const handleBumpDetected = async () => {
    console.log("💥 BUMP DETECTED! Capturing frame...");
    try {
      // 1. Get current location
      const loc = await Location.getCurrentPositionAsync({ accuracy: Location.Accuracy.Balanced });
      
      // 2. Capture a single photo frame (much smaller than video)
      if (cameraRef.current) {
        const photo = await cameraRef.current.takePictureAsync({ quality: 0.5 });
        if (photo && photo.uri) {
          uploadFrame(photo.uri, loc.coords.latitude, loc.coords.longitude);
        }
      }
    } catch (e) {
      console.error("Error capturing bump:", e);
    }
  };

  const uploadFrame = async (uri: string, lat: number, lon: number) => {
    try {
      const formData = new FormData();
      
      // We still name the field 'video' so the backend FastAPI UploadFile logic doesn't break
      // but we're actually sending a much smaller JPEG image. Roboflow handles images natively!
      formData.append('video', {
        uri,
        name: 'frame.jpg',
        type: 'image/jpeg',
      } as any);
      
      formData.append('lat', lat.toString());
      formData.append('lon', lon.toString());

      console.log('Uploading bump frame...');
      
      const response = await fetch(BACKEND_URL, {
        method: 'POST',
        body: formData,
        headers: { 'Accept': 'application/json' },
      });
      
      if (response.ok) {
        console.log('✅ Frame uploaded successfully');
      }
    } catch (error) {
      console.error('❌ Upload failed:', error);
    }
  };

  const togglePatrol = () => {
    if (isPatrolling) {
      setIsPatrolling(false);
      _unsubscribe();
    } else {
      setIsPatrolling(true);
      _subscribe();
    }
  };

  if (!permission) return <View />;
  if (!permission.granted) {
    return (
      <View style={styles.container}>
        <Text style={{ color: 'white', textAlign: 'center', marginBottom: 20 }}>Camera permission is required.</Text>
        <TouchableOpacity style={styles.button} onPress={requestPermission}>
          <Text style={styles.buttonText}>Grant Permission</Text>
        </TouchableOpacity>
      </View>
    );
  }

  return (
    <View style={styles.container}>
      <CameraView style={styles.camera} facing="back" ref={cameraRef}>
        <View style={styles.overlay}>
          <Text style={styles.title}>Smart Patrol (سمارٹ پٹرول)</Text>
          <Text style={styles.subtitle}>
            {isPatrolling ? '🟢 Active (نگرانی جاری ہے)' : 'Paused (رک گیا)'}
          </Text>
          
          {isPatrolling && (
            <View style={styles.statsCard}>
              <Text style={styles.statsText}>Bumps Detected: {bumpCount}</Text>
              <Text style={styles.statsSub}>Only uploading on impact</Text>
            </View>
          )}
        </View>

        <View style={styles.buttonContainer}>
          <TouchableOpacity style={[styles.button, isPatrolling && styles.buttonRecording]} onPress={togglePatrol}>
            <Text style={styles.buttonText}>{isPatrolling ? 'Stop Patrol (روکیں)' : 'Start Patrol (شروع کریں)'}</Text>
          </TouchableOpacity>
        </View>
      </CameraView>
    </View>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: 'black' },
  camera: { flex: 1 },
  overlay: { position: 'absolute', top: 60, width: '100%', alignItems: 'center' },
  title: { fontSize: 24, fontWeight: 'bold', color: 'white', textShadowColor: 'black', textShadowOffset: { width: 1, height: 1 }, textShadowRadius: 3 },
  subtitle: { fontSize: 16, color: 'white', marginTop: 5, fontWeight: '500' },
  statsCard: { marginTop: 20, backgroundColor: 'rgba(0,0,0,0.6)', padding: 15, borderRadius: 12, alignItems: 'center', borderWidth: 1, borderColor: 'rgba(255,255,255,0.2)' },
  statsText: { color: '#30d158', fontSize: 18, fontWeight: 'bold' },
  statsSub: { color: 'rgba(255,255,255,0.7)', fontSize: 12, marginTop: 4 },
  buttonContainer: { flex: 1, flexDirection: 'row', backgroundColor: 'transparent', margin: 40, marginBottom: 60 },
  button: { flex: 1, alignSelf: 'flex-end', alignItems: 'center', backgroundColor: '#007AFF', padding: 16, borderRadius: 12 },
  buttonRecording: { backgroundColor: '#FF3B30' },
  buttonText: { fontSize: 18, fontWeight: 'bold', color: 'white' },
});
