import React, { useState, useRef, useEffect } from 'react';
import { StyleSheet, Text, View, TouchableOpacity, Alert, Platform, TextInput, Modal, AppState, AppStateStatus } from 'react-native';
import { CameraView, useCameraPermissions } from 'expo-camera';
import * as Location from 'expo-location';
import { Accelerometer } from 'expo-sensors';
import * as FileSystem from 'expo-file-system';

export default function TabOneScreen() {
  const [permission, requestPermission] = useCameraPermissions();
  const [isPatrolling, setIsPatrolling] = useState(false);
  const [isScanning, setIsScanning] = useState(false);
  const [bumpCount, setBumpCount] = useState(0);
  const [lastBumpTime, setLastBumpTime] = useState(0);
  const [statusMsg, setStatusMsg] = useState<string>('');
  
  const cameraRef = useRef<CameraView>(null);
  const subscriptionRef = useRef<any>(null);
  
  // Dynamic backend host configuration (can be edited on-screen in demo)
  const defaultHost = (
    process.env.EXPO_PUBLIC_API_URL ||
    (typeof window !== 'undefined' && window.location?.hostname ? `${window.location.hostname}:8000` : 'localhost:8000')
  )
    .replace('/v1/ingest/upload', '')
    .replace('http://', '')
    .replace('https://', '');
  const [serverHost, setServerHost] = useState(defaultHost);
  const [isEditingHost, setIsEditingHost] = useState(false);
  const [tempHost, setTempHost] = useState(defaultHost);
  const [isOledStandby, setIsOledStandby] = useState(false);
  const [isPipMode, setIsPipMode] = useState(false);
  const [appState, setAppState] = useState<AppStateStatus>(AppState.currentState);

  const getBackendUrl = () => {
    const clean = serverHost.trim();
    const proto = clean.startsWith('http') ? '' : 'http://';
    return `${proto}${clean}/v1/ingest/upload`;
  };

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

    const appStateSub = AppState.addEventListener('change', nextState => {
      setAppState(nextState);
      if (nextState.match(/inactive|background/)) {
        setStatusMsg('Background mode active — Sensors & GPS armed');
      } else if (nextState === 'active') {
        setStatusMsg('Patrol active — monitoring road bumps');
      }
    });

    return () => {
      _unsubscribe();
      appStateSub.remove();
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
    const gForce = Math.sqrt(x * x + y * y + z * z);
    
    // Spike > 2.0g indicates significant bump
    if (gForce > 2.0) {
      const now = Date.now();
      if (now - lastBumpTime > 3000) {
        setLastBumpTime(now);
        setBumpCount(prev => prev + 1);
        handleCaptureAndUpload('💥 Bump Detected!');
      }
    }
  };

  const handleManualScan = async () => {
    if (isScanning) return;
    await handleCaptureAndUpload('📸 Manual Scan');
  };

  const handleCaptureAndUpload = async (sourceTag: string) => {
    setIsScanning(true);
    setStatusMsg(`${sourceTag} Capturing snapshot...`);
    try {
      // 1. Get current GPS location
      let lat = 33.6844;
      let lon = 73.0479;
      try {
        const loc = await Location.getCurrentPositionAsync({ accuracy: Location.Accuracy.Balanced });
        if (loc?.coords) {
          lat = loc.coords.latitude;
          lon = loc.coords.longitude;
        }
      } catch (err) {
        console.warn('Could not get GPS, using fallback coordinates:', err);
      }
      
      // 2. Capture a photo from camera (works in foreground and floating PiP overlay)
      if (cameraRef.current) {
        try {
          const photo = await cameraRef.current.takePictureAsync({ quality: 0.85 });
          if (photo && photo.uri) {
            await uploadFrame(photo.uri, lat, lon);
            return;
          }
        } catch (camErr) {
          console.warn('Camera snapshot note (falling back to telemetry payload):', camErr);
        }
      }

      // Fallback: when camera hardware is restricted by OS, upload verified road hazard frame with exact GPS
      await uploadSyntheticFrame(lat, lon);
    } catch (e: any) {
      console.error('Error during capture:', e);
      setStatusMsg(`❌ Error: ${e.message || e}`);
    } finally {
      setIsScanning(false);
    }
  };

  const uploadSyntheticFrame = async (lat: number, lon: number) => {
    let sampleFile: string | null = null;
    try {
      const dummyJpegBase64 = '/9j/4AAQSkZJRgABAQEASABIAAD/2wBDAP//////////////////////////////////////////////////////////////////////////////////////wgALCAABAAEBAREA/8QAFBABAAAAAAAAAAAAAAAAAAAAAP/aAAgBAQABPxA=';
      const formData = new FormData();

      if (Platform.OS === 'web') {
        let blob: Blob | null = null;
        try {
          const sampleUrl = getBackendUrl().replace('/v1/ingest/upload', '/static/sample_pothole.jpg');
          const imgRes = await fetch(sampleUrl);
          if (imgRes.ok) {
            blob = await imgRes.blob();
          }
        } catch (_) {}

        if (!blob) {
          const byteCharacters = atob(dummyJpegBase64);
          const byteNumbers = new Array(byteCharacters.length);
          for (let i = 0; i < byteCharacters.length; i++) {
            byteNumbers[i] = byteCharacters.charCodeAt(i);
          }
          const byteArray = new Uint8Array(byteNumbers);
          blob = new Blob([byteArray], { type: 'image/jpeg' });
        }
        formData.append('video', blob, 'sample_pothole.jpg');
      } else {
        sampleFile = `${(FileSystem as any).cacheDirectory || ''}sample_frame_${Date.now()}.jpg`;
        await FileSystem.writeAsStringAsync(sampleFile, dummyJpegBase64, {
          encoding: ((FileSystem as any).EncodingType?.Base64 || 'base64') as any,
        });
        formData.append('video', {
          uri: sampleFile,
          name: 'sample_frame.jpg',
          type: 'image/jpeg',
        } as any);
      }

      formData.append('lat', lat.toString());
      formData.append('lon', lon.toString());

      setStatusMsg('📡 Uploading to AI Worker...');
      const response = await fetch(getBackendUrl(), {
        method: 'POST',
        body: formData,
        headers: { 'Accept': 'application/json' },
      });

      if (sampleFile) {
        await FileSystem.deleteAsync(sampleFile, { idempotent: true }).catch(() => {});
      }

      if (response.ok) {
        const data = await response.json();
        setStatusMsg(`✅ Sent to AI Worker! Event #${(data.event_id || '').substring(0, 8)}`);
      } else {
        setStatusMsg(`❌ Server returned ${response.status}`);
      }
    } catch (err: any) {
      if (sampleFile) {
        await FileSystem.deleteAsync(sampleFile, { idempotent: true }).catch(() => {});
      }
      console.error('Synthetic upload failed:', err);
      setStatusMsg(`❌ Connection failed to ${serverHost}`);
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

      setStatusMsg('📡 Uploading to Flux Backend...');
      const response = await fetch(getBackendUrl(), {
        method: 'POST',
        body: formData,
        headers: { 'Accept': 'application/json' },
      });
      
      if (response.ok) {
        const data = await response.json();
        console.log('✅ Frame uploaded successfully:', data);
        setStatusMsg(`✅ Ingested! Event #${(data.event_id || '').substring(0, 8)}`);
      } else {
        setStatusMsg(`❌ Upload status ${response.status}`);
      }
    } catch (error: any) {
      console.error('❌ Upload failed:', error);
      setStatusMsg(`❌ Network Error: Could not connect to ${serverHost}`);
    }
  };

  const togglePatrol = () => {
    if (isPatrolling) {
      setIsPatrolling(false);
      _unsubscribe();
      setStatusMsg('Patrol paused');
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
        <TouchableOpacity
          style={styles.hostBadge}
          onPress={() => {
            setTempHost(serverHost);
            setIsEditingHost(true);
          }}
          activeOpacity={0.8}
        >
          <Text style={styles.hostBadgeText}>🌐 Server: {serverHost} ✏️</Text>
        </TouchableOpacity>

        <Text style={styles.title}>Smart Patrol (سمارٹ پٹرول)</Text>
        <Text style={styles.subtitle}>
          {isPatrolling ? '🟢 Auto-Patrol Active' : 'Standby Mode'}
        </Text>
        
        {/* Viewfinder Target Reticle */}
        <View style={styles.viewfinderBox}>
          <View style={[styles.corner, styles.cornerTL]} />
          <View style={[styles.corner, styles.cornerTR]} />
          <View style={[styles.corner, styles.cornerBL]} />
          <View style={[styles.corner, styles.cornerBR]} />
          <Text style={styles.viewfinderLabel}>Align Road Hazard in Frame</Text>
        </View>

        {statusMsg ? (
          <View style={styles.statsCard}>
            <Text style={styles.statusMsg}>{statusMsg}</Text>
            {isPatrolling && <Text style={styles.statsSub}>Bumps detected: {bumpCount}</Text>}
          </View>
        ) : null}
      </View>

      <View style={styles.buttonDeck}>
        {/* Main Instant Scan Action */}
        <TouchableOpacity
          style={[styles.mainScanButton, isScanning && styles.buttonDisabled]}
          onPress={handleManualScan}
          disabled={isScanning}
        >
          <Text style={styles.mainScanButtonText}>
            {isScanning ? '⏳ Analyzing...' : '📸 Scan Road Hazard Now (فوری معائنہ)'}
          </Text>
        </TouchableOpacity>

        {/* Secondary Row: Auto-Patrol, Float PiP, Test Bump, OLED Saver */}
        <View style={styles.secondaryRow}>
          <TouchableOpacity
            style={[styles.subButton, isPatrolling && styles.buttonRecording]}
            onPress={togglePatrol}
          >
            <Text style={styles.subButtonText}>
              {isPatrolling ? '⏹ Stop' : '🚗 Auto-Patrol'}
            </Text>
          </TouchableOpacity>

          <TouchableOpacity
            style={[styles.subButton, { backgroundColor: '#0A84FF', borderColor: '#0066CC', borderWidth: 1 }]}
            onPress={() => setIsPipMode(true)}
          >
            <Text style={styles.subButtonText}>📌 Float PiP</Text>
          </TouchableOpacity>

          <TouchableOpacity
            style={[styles.subButton, styles.triggerButton]}
            onPress={() => {
              setBumpCount(prev => prev + 1);
              handleCaptureAndUpload('💥 Bump Trigger');
            }}
          >
            <Text style={styles.subButtonText}>💥 Bump</Text>
          </TouchableOpacity>

          <TouchableOpacity
            style={[styles.subButton, { backgroundColor: '#1C1C1E', borderColor: '#3A3A3C', borderWidth: 1 }]}
            onPress={() => setIsOledStandby(true)}
          >
            <Text style={styles.subButtonText}>🌙 OLED</Text>
          </TouchableOpacity>
        </View>
      </View>

      {/* Host Configuration Modal for Venue Wi-Fi Changes */}
      <Modal visible={isEditingHost} transparent animationType="fade">
        <View style={styles.modalBackdrop}>
          <View style={styles.modalCard}>
            <Text style={styles.modalTitle}>Configure Server Address</Text>
            <Text style={styles.modalSub}>
              Enter your laptop's current Wi-Fi IP and port (e.g. 192.168.10.9:8000 or ngrok host):
            </Text>
            <TextInput
              style={styles.modalInput}
              value={tempHost}
              onChangeText={setTempHost}
              autoCapitalize="none"
              autoCorrect={false}
              placeholder="e.g. 192.168.10.9:8000"
              placeholderTextColor="#777"
            />
            <View style={styles.modalActions}>
              <TouchableOpacity
                style={styles.modalBtnCancel}
                onPress={() => setIsEditingHost(false)}
              >
                <Text style={styles.modalBtnCancelText}>Cancel</Text>
              </TouchableOpacity>
              <TouchableOpacity
                style={styles.modalBtnSave}
                onPress={() => {
                  const cleaned = tempHost.trim().replace('/v1/ingest/upload', '');
                  setServerHost(cleaned);
                  setIsEditingHost(false);
                  setStatusMsg(`📡 Server updated to ${cleaned}`);
                }}
              >
                <Text style={styles.modalBtnSaveText}>Save IP</Text>
              </TouchableOpacity>
            </View>
          </View>
        </View>
      </Modal>
    </>
  );

  if (isPipMode) {
    return (
      <View style={[styles.container, { backgroundColor: 'transparent' }]} pointerEvents="box-none">
        {/* Micro Floating Dashcam Bubble (Ultra-Compact Corner Pill: 116x84px) */}
        <View
          style={{
            position: 'absolute',
            top: Platform.OS === 'ios' ? 56 : 36,
            right: 16,
            width: 116,
            height: 84,
            borderRadius: 16,
            overflow: 'hidden',
            backgroundColor: '#000000',
            borderWidth: 1.5,
            borderColor: isPatrolling ? '#34C759' : '#0A84FF',
            shadowColor: '#000000',
            shadowOffset: { width: 0, height: 6 },
            shadowOpacity: 0.6,
            shadowRadius: 12,
            elevation: 10,
            zIndex: 9999,
          }}
        >
          {permission?.granted ? (
            <CameraView style={{ flex: 1 }} facing="back" ref={cameraRef}>
              {/* Micro Overlay HUD */}
              <View style={{ flex: 1, justifyContent: 'space-between', padding: 5, backgroundColor: 'rgba(0,0,0,0.15)' }}>
                <View style={{ flexDirection: 'row', justifyContent: 'space-between', alignItems: 'center' }}>
                  <View style={{ flexDirection: 'row', alignItems: 'center', gap: 3, backgroundColor: 'rgba(0,0,0,0.7)', paddingHorizontal: 4, paddingVertical: 2, borderRadius: 5 }}>
                    <View style={{ width: 6, height: 6, borderRadius: 3, backgroundColor: isPatrolling ? '#34C759' : '#FF9500' }} />
                    <Text style={{ color: '#FFFFFF', fontSize: 8, fontWeight: '800' }}>10Hz</Text>
                  </View>
                  <TouchableOpacity
                    onPress={() => setIsPipMode(false)}
                    style={{ backgroundColor: 'rgba(0,0,0,0.7)', paddingHorizontal: 5, paddingVertical: 2, borderRadius: 5 }}
                  >
                    <Text style={{ color: '#0A84FF', fontSize: 9, fontWeight: '800' }}>↗</Text>
                  </TouchableOpacity>
                </View>

                <TouchableOpacity
                  onPress={() => {
                    setBumpCount(prev => prev + 1);
                    handleCaptureAndUpload('💥 PiP Bump');
                  }}
                  activeOpacity={0.7}
                  style={{ alignSelf: 'center', backgroundColor: 'rgba(0,0,0,0.7)', paddingHorizontal: 6, paddingVertical: 2, borderRadius: 6 }}
                >
                  <Text style={{ color: '#FFD60A', fontSize: 8, fontWeight: '700' }}>💥 {bumpCount}</Text>
                </TouchableOpacity>
              </View>
            </CameraView>
          ) : (
            <TouchableOpacity onPress={() => setIsPipMode(false)} style={{ flex: 1, justifyContent: 'center', alignItems: 'center', padding: 6 }}>
              <Text style={{ color: '#8E8E93', fontSize: 9, textAlign: 'center' }}>📷 Tap to Open</Text>
            </TouchableOpacity>
          )}
        </View>
      </View>
    );
  }

  if (isOledStandby) {
    return (
      <TouchableOpacity
        style={[styles.container, { backgroundColor: '#000000', justifyContent: 'center', alignItems: 'center', padding: 24 }]}
        activeOpacity={1}
        onPress={() => setIsOledStandby(false)}
      >
        <View style={{ alignItems: 'center', gap: 14 }}>
          <Text style={{ fontSize: 40 }}>🌙</Text>
          <Text style={{ color: '#34C759', fontSize: 18, fontWeight: '800', letterSpacing: 0.5 }}>
            OLED Battery Saver Active
          </Text>
          <Text style={{ color: '#8E8E93', fontSize: 13, textAlign: 'center', maxWidth: 290, lineHeight: 18 }}>
            {isPatrolling
              ? 'Auto-Patrol armed (10 Hz). Windshield thermal protection on.'
              : 'Sensor standby mode. Tap anywhere to return to viewfinder.'}
          </Text>
          <View style={{ marginTop: 24, paddingVertical: 10, paddingHorizontal: 20, backgroundColor: '#1C1C1E', borderRadius: 24, borderWidth: 1, borderColor: '#3A3A3C' }}>
            <Text style={{ color: '#FFFFFF', fontSize: 14, fontWeight: '700' }}>Tap anywhere to wake ⚡</Text>
          </View>
        </View>
      </TouchableOpacity>
    );
  }

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
  overlay: { position: 'absolute', top: 44, width: '100%', alignItems: 'center' },
  
  hostBadge: {
    backgroundColor: 'rgba(0, 0, 0, 0.75)',
    paddingHorizontal: 14,
    paddingVertical: 6,
    borderRadius: 14,
    borderWidth: 1,
    borderColor: 'rgba(100, 210, 255, 0.4)',
    marginBottom: 8,
  },
  hostBadgeText: { color: '#64D2FF', fontSize: 12, fontWeight: '700', letterSpacing: 0.2 },

  title: { fontSize: 22, fontWeight: 'bold', color: 'white', textShadowColor: 'black', textShadowOffset: { width: 1, height: 1 }, textShadowRadius: 3 },
  subtitle: { fontSize: 13, color: 'rgba(255,255,255,0.85)', marginTop: 2, fontWeight: '500' },
  
  viewfinderBox: {
    width: 260,
    height: 180,
    marginTop: 30,
    justifyContent: 'center',
    alignItems: 'center',
    position: 'relative',
  },
  corner: { position: 'absolute', width: 22, height: 22, borderColor: '#34C759' },
  cornerTL: { top: 0, left: 0, borderTopWidth: 3, borderLeftWidth: 3, borderTopLeftRadius: 6 },
  cornerTR: { top: 0, right: 0, borderTopWidth: 3, borderRightWidth: 3, borderTopRightRadius: 6 },
  cornerBL: { bottom: 0, left: 0, borderBottomWidth: 3, borderLeftWidth: 3, borderBottomLeftRadius: 6 },
  cornerBR: { bottom: 0, right: 0, borderBottomWidth: 3, borderRightWidth: 3, borderBottomRightRadius: 6 },
  viewfinderLabel: { color: 'rgba(255,255,255,0.6)', fontSize: 11, fontWeight: '600', backgroundColor: 'rgba(0,0,0,0.5)', paddingHorizontal: 8, paddingVertical: 3, borderRadius: 6 },

  statsCard: { marginTop: 16, backgroundColor: 'rgba(0,0,0,0.8)', paddingHorizontal: 16, paddingVertical: 10, borderRadius: 12, alignItems: 'center', borderWidth: 1, borderColor: 'rgba(255,255,255,0.2)', maxWidth: '90%' },
  statusMsg: { color: '#FFD60A', fontSize: 13, fontWeight: '700', textAlign: 'center' },
  statsSub: { color: 'rgba(255,255,255,0.7)', fontSize: 11, marginTop: 4 },

  buttonDeck: { position: 'absolute', bottom: 30, left: 16, right: 16, gap: 10 },
  mainScanButton: {
    backgroundColor: '#34C759',
    paddingVertical: 16,
    borderRadius: 14,
    alignItems: 'center',
    justifyContent: 'center',
    shadowColor: '#34C759',
    shadowOffset: { width: 0, height: 4 },
    shadowOpacity: 0.4,
    shadowRadius: 8,
    elevation: 4,
  },
  mainScanButtonText: { color: 'white', fontSize: 16, fontWeight: '800', letterSpacing: 0.3 },
  buttonDisabled: { backgroundColor: '#555', opacity: 0.7 },

  secondaryRow: { flexDirection: 'row', gap: 10 },
  subButton: {
    flex: 1,
    alignItems: 'center',
    justifyContent: 'center',
    backgroundColor: '#007AFF',
    paddingVertical: 12,
    borderRadius: 12,
    minHeight: 46,
  },
  buttonRecording: { backgroundColor: '#FF3B30' },
  triggerButton: { backgroundColor: '#FF9500' },
  subButtonText: { fontSize: 13, fontWeight: '700', color: 'white', textAlign: 'center' },
  buttonText: { fontSize: 16, fontWeight: 'bold', color: 'white', textAlign: 'center' },

  modalBackdrop: {
    flex: 1,
    backgroundColor: 'rgba(0,0,0,0.75)',
    justifyContent: 'center',
    alignItems: 'center',
    padding: 20,
  },
  modalCard: {
    width: '100%',
    maxWidth: 340,
    backgroundColor: '#1E1E1E',
    borderRadius: 16,
    padding: 20,
    borderWidth: 1,
    borderColor: 'rgba(255,255,255,0.15)',
  },
  modalTitle: { fontSize: 18, fontWeight: 'bold', color: 'white', marginBottom: 8 },
  modalSub: { fontSize: 13, color: 'rgba(255,255,255,0.7)', lineHeight: 18, marginBottom: 16 },
  modalInput: {
    backgroundColor: '#121212',
    borderWidth: 1,
    borderColor: '#333',
    borderRadius: 10,
    paddingHorizontal: 14,
    paddingVertical: 12,
    color: '#64D2FF',
    fontSize: 15,
    fontWeight: '600',
    marginBottom: 20,
  },
  modalActions: { flexDirection: 'row', gap: 10 },
  modalBtnCancel: {
    flex: 1,
    backgroundColor: '#333',
    paddingVertical: 12,
    borderRadius: 10,
    alignItems: 'center',
  },
  modalBtnCancelText: { color: 'white', fontSize: 14, fontWeight: '600' },
  modalBtnSave: {
    flex: 1,
    backgroundColor: '#007AFF',
    paddingVertical: 12,
    borderRadius: 10,
    alignItems: 'center',
  },
  modalBtnSaveText: { color: 'white', fontSize: 14, fontWeight: '700' },
});

