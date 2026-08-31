import React, { useState, useRef, useEffect } from 'react';
import { StyleSheet, Text, View, TouchableOpacity, Alert, Platform } from 'react-native';
import { CameraView, useCameraPermissions } from 'expo-camera';
import * as Location from 'expo-location';
import { Accelerometer } from 'expo-sensors';

export default function TabOneScreen() {
  const [permission, requestPermission] = useCameraPermissions();
  const [isPatrolling, setIsPatrolling] = useState(false);
  const [bumpCount, setBumpCount] = useState(0);
  const [lastBumpTime, setLastBumpTime] = useState(0);
  const [statusMsg, setStatusMsg] = useState<string>('');
  
  const cameraRef = useRef<CameraView>(null);
  const subscriptionRef = useRef<any>(null);
  
  // Configurable backend URL via environment variable with localhost default
  const BACKEND_URL = process.env.EXPO_PUBLIC_API_URL || 'http://localhost:8000/v1/ingest/upload';

  useEffect(() => {
    (async () => {
      try {
        const { status } = await Location.requestForegroundPermissionsAsync();
        if (status !== 'granted') {
          console.warn('Location permission not granted; using fallback coordinates.');
        }
      } catch (e) {
        console.warn('Location permission check error:', e);
      }
    })();
    return () => {
      _unsubscribe();
    };
  }, []);

  const _subscribe = async () => {
    try {
      if (Platform.OS !== 'web') {
        const isAvailable = await Accelerometer.isAvailableAsync();
        if (isAvailable) {
          Accelerometer.setUpdateInterval(100);
          subscriptionRef.current = Accelerometer.addListener(accelerometerData => {
            detectBump(accelerometerData);
          });
        }
      } else {
        console.log('Running on Web: accelerometer listener disabled; use Trigger Bump button.');
      }
    } catch (e) {
      console.warn('Accelerometer initialization notice:', e);
    }
  };

  const _unsubscribe = () => {
    if (subscriptionRef.current) {
      try {
        subscriptionRef.current.remove();
      } catch (_) {}
      subscriptionRef.current = null;
    }
  };

  const detectBump = async ({ x, y, z }: { x: number; y: number; z: number }) => {
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
    console.log('💥 BUMP DETECTED! Capturing frame...');
    setStatusMsg('💥 Bump detected! Uploading...');
    try {
      // 1. Get current location (fallback to default if unavailable)
      let lat = 33.6844;
      let lon = 73.0479;
      try {
        const loc = await Location.getCurrentPositionAsync({ accuracy: Location.Accuracy.Balanced });
        if (loc?.coords) {
          lat = loc.coords.latitude;
          lon = loc.coords.longitude;
        }
      } catch (err) {
        console.warn('Could not get GPS, using default location:', err);
      }
      
      // 2. Capture a single photo frame if camera is available
      if (cameraRef.current) {
        try {
          const photo = await cameraRef.current.takePictureAsync({ quality: 0.5 });
          if (photo && photo.uri) {
            await uploadFrame(photo.uri, lat, lon);
            return;
          }
        } catch (camErr) {
          console.warn('Camera snapshot not available, falling back to synthetic sample:', camErr);
        }
      }

      // Fallback: Upload a 1x1 test JPEG if camera is unavailable (e.g. desktop web)
      await uploadSyntheticFrame(lat, lon);
    } catch (e: any) {
      console.error('Error capturing bump:', e);
      setStatusMsg(`❌ Error: ${e.message || e}`);
    }
  };

  const uploadSyntheticFrame = async (lat: number, lon: number) => {
    try {
      // Minimal valid 1x1 JPEG base64
      const dummyJpegBase64 = '/9j/4AAQSkZJRgABAQEASABIAAD/2wBDAP//////////////////////////////////////////////////////////////////////////////////////wgALCAABAAEBAREA/8QAFBABAAAAAAAAAAAAAAAAAAAAAP/aAAgBAQABPxA=';
      const byteCharacters = atob(dummyJpegBase64);
      const byteNumbers = new Array(byteCharacters.length);
      for (let i = 0; i < byteCharacters.length; i++) {
        byteNumbers[i] = byteCharacters.charCodeAt(i);
      }
      const byteArray = new Uint8Array(byteNumbers);
      const blob = new Blob([byteArray], { type: 'image/jpeg' });

      const formData = new FormData();
      formData.append('video', blob, 'sample_frame.jpg');
      formData.append('lat', lat.toString());
      formData.append('lon', lon.toString());

      console.log('Uploading simulated frame to:', BACKEND_URL);
      const response = await fetch(BACKEND_URL, {
        method: 'POST',
        body: formData,
        headers: { 'Accept': 'application/json' },
      });
      if (response.ok) {
        setStatusMsg('✅ Bump uploaded & sent to AI worker!');
      } else {
        setStatusMsg(`❌ Server returned ${response.status}`);
      }
    } catch (err: any) {
      console.error('Synthetic upload failed:', err);
      setStatusMsg(`❌ Upload failed: ${err.message || err}`);
    }
  };

  const uploadFrame = async (uri: string, lat: number, lon: number) => {
    try {
      const formData = new FormData();
      
      if (Platform.OS === 'web') {
        const res = await fetch(uri);
        const blob = await res.blob();
        formData.append('video', blob, 'frame.jpg');
      } else {
        formData.append('video', {
          uri,
          name: 'frame.jpg',
          type: 'image/jpeg',
        } as any);
      }
      
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
        setStatusMsg('✅ Frame uploaded to worker!');
      } else {
        setStatusMsg(`❌ Upload failed with status: ${response.status}`);
      }
    } catch (error: any) {
      console.error('❌ Upload failed:', error);
      setStatusMsg(`❌ Upload error: ${error.message || error}`);
    }
  };

  const togglePatrol = () => {
    if (isPatrolling) {
      setIsPatrolling(false);
      _unsubscribe();
      setStatusMsg('Patrol stopped');
    } else {
      setIsPatrolling(true);
      _subscribe();
      setStatusMsg('Patrol active — monitoring road bumps');
    }
  };

  const renderCameraOrFallback = () => {
    if (permission?.granted) {
      return (
        <CameraView style={styles.camera} facing="back" ref={cameraRef}>
          {renderOverlayContent()}
        </CameraView>
      );
    }

    return (
      <View style={[styles.camera, styles.webFallback]}>
        <Text style={styles.webFallbackTitle}>📷 Camera Standby</Text>
        <Text style={styles.webFallbackText}>
          {Platform.OS === 'web'
            ? 'Web test mode active. You can trigger bumps manually below.'
            : 'Camera permission is required.'}
        </Text>
        {!permission?.granted && (
          <TouchableOpacity style={styles.permissionBtn} onPress={requestPermission}>
            <Text style={styles.buttonText}>Request Permission</Text>
          </TouchableOpacity>
        )}
        {renderOverlayContent()}
      </View>
    );
  };

  const renderOverlayContent = () => (
    <>
      <View style={styles.overlay}>
        <Text style={styles.title}>Smart Patrol (سمارٹ پٹرول)</Text>
        <Text style={styles.subtitle}>
          {isPatrolling ? '🟢 Active (نگرانی جاری ہے)' : 'Paused (رک گیا)'}
        </Text>
        
        {isPatrolling && (
          <View style={styles.statsCard}>
            <Text style={styles.statsText}>Bumps Detected: {bumpCount}</Text>
            <Text style={styles.statsSub}>Auto-uploading snapshot on bump</Text>
            {statusMsg ? <Text style={styles.statusMsg}>{statusMsg}</Text> : null}
          </View>
        )}
      </View>

      <View style={styles.buttonContainer}>
        <TouchableOpacity
          style={[styles.button, isPatrolling && styles.buttonRecording]}
          onPress={togglePatrol}
        >
          <Text style={styles.buttonText}>
            {isPatrolling ? 'Stop Patrol (روکیں)' : 'Start Patrol (شروع کریں)'}
          </Text>
        </TouchableOpacity>

        {isPatrolling && (
          <TouchableOpacity
            style={[styles.button, styles.triggerButton]}
            onPress={() => {
              setBumpCount(prev => prev + 1);
              handleBumpDetected();
            }}
          >
            <Text style={styles.buttonText}>💥 Trigger Bump (Test)</Text>
          </TouchableOpacity>
        )}
      </View>
    </>
  );

  return (
    <View style={styles.container}>
      {renderCameraOrFallback()}
    </View>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: 'black' },
  camera: { flex: 1 },
  webFallback: { justifyContent: 'center', alignItems: 'center', backgroundColor: '#151515' },
  webFallbackTitle: { color: 'white', fontSize: 20, fontWeight: 'bold', marginTop: 40 },
  webFallbackText: { color: 'rgba(255,255,255,0.7)', fontSize: 14, textAlign: 'center', marginHorizontal: 20, marginTop: 8 },
  permissionBtn: { marginTop: 12, backgroundColor: '#007AFF', paddingHorizontal: 20, paddingVertical: 10, borderRadius: 8 },
  overlay: { position: 'absolute', top: 50, width: '100%', alignItems: 'center' },
  title: { fontSize: 24, fontWeight: 'bold', color: 'white', textShadowColor: 'black', textShadowOffset: { width: 1, height: 1 }, textShadowRadius: 3 },
  subtitle: { fontSize: 16, color: 'white', marginTop: 5, fontWeight: '500' },
  statsCard: { marginTop: 15, backgroundColor: 'rgba(0,0,0,0.75)', padding: 12, borderRadius: 12, alignItems: 'center', borderWidth: 1, borderColor: 'rgba(255,255,255,0.2)', width: '85%' },
  statsText: { color: '#30d158', fontSize: 18, fontWeight: 'bold' },
  statsSub: { color: 'rgba(255,255,255,0.7)', fontSize: 12, marginTop: 2 },
  statusMsg: { color: '#ffd60a', fontSize: 12, marginTop: 6, fontWeight: '600', textAlign: 'center' },
  buttonContainer: { position: 'absolute', bottom: 40, left: 20, right: 20, flexDirection: 'row', gap: 12 },
  button: { flex: 1, alignItems: 'center', justifyContent: 'center', backgroundColor: '#007AFF', padding: 16, borderRadius: 12, minHeight: 54 },
  buttonRecording: { backgroundColor: '#FF3B30' },
  triggerButton: { backgroundColor: '#ff9500' },
  buttonText: { fontSize: 16, fontWeight: 'bold', color: 'white', textAlign: 'center' },
});
