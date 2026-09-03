import React, { useState, useRef, useEffect } from 'react';
import {
  StyleSheet,
  Text,
  View,
  TouchableOpacity,
  Alert,
  ActivityIndicator,
  Platform,
} from 'react-native';
import { CameraView, useCameraPermissions } from 'expo-camera';
import * as Location from 'expo-location';
import * as FileSystem from 'expo-file-system';

// ─── Configuration ─────────────────────────────────────────────────────
const API_BASE = process.env.EXPO_PUBLIC_API_BASE || 'http://localhost:8000';
const CHUNK_SIZE = 5 * 1024 * 1024; // 5 MB per chunk
const MAX_RETRIES = 3;

// ─── Types ─────────────────────────────────────────────────────────────
interface UploadSession {
  session_id: string;
  total_chunks: number;
  chunk_size_hint: number;
  upload_url_template: string;
}

interface UploadProgress {
  phase: 'idle' | 'chunking' | 'uploading' | 'completing' | 'done' | 'error';
  currentChunk: number;
  totalChunks: number;
  message: string;
}

export default function VideoUploadScreen() {
  const [permission, requestPermission] = useCameraPermissions();
  const [isRecording, setIsRecording] = useState(false);
  const [recordingDuration, setRecordingDuration] = useState(0);
  const [progress, setProgress] = useState<UploadProgress>({
    phase: 'idle',
    currentChunk: 0,
    totalChunks: 0,
    message: 'Ready to record',
  });

  const cameraRef = useRef<CameraView>(null);
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null);

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
      if (timerRef.current) clearInterval(timerRef.current);
    };
  }, []);

  // ─── Recording ─────────────────────────────────────────────────────

  const startRecording = async () => {
    if (Platform.OS === 'web' || !cameraRef.current) {
      // On web or without camera, run the simulated multi-chunk upload
      await simulateWebVideoUpload();
      return;
    }

    setIsRecording(true);
    setRecordingDuration(0);
    setProgress({ phase: 'idle', currentChunk: 0, totalChunks: 0, message: 'Recording...' });

    timerRef.current = setInterval(() => {
      setRecordingDuration((prev) => prev + 1);
    }, 1000);

    try {
      const video = await cameraRef.current.recordAsync({ maxDuration: 60 });
      if (timerRef.current) clearInterval(timerRef.current);
      setIsRecording(false);

      if (video && video.uri) {
        await handleVideoRecorded(video.uri);
      }
    } catch (e) {
      if (timerRef.current) clearInterval(timerRef.current);
      setIsRecording(false);
      console.error('Recording error:', e);
      setProgress({ phase: 'error', currentChunk: 0, totalChunks: 0, message: `Recording failed: ${e}` });
    }
  };

  const stopRecording = () => {
    if (cameraRef.current) {
      try {
        cameraRef.current.stopRecording();
      } catch (_) {}
    }
  };

  // ─── Web Simulation ────────────────────────────────────────────────

  const simulateWebVideoUpload = async () => {
    try {
      setProgress({ phase: 'chunking', currentChunk: 0, totalChunks: 2, message: 'Simulating video capture (2 chunks)...' });
      
      let lat = 33.6844;
      let lon = 73.0479;
      try {
        const loc = await Location.getCurrentPositionAsync({ accuracy: Location.Accuracy.Balanced });
        if (loc?.coords) {
          lat = loc.coords.latitude;
          lon = loc.coords.longitude;
        }
      } catch (_) {}

      // 1. Create upload session for 2 chunks
      const session = await createUploadSession({
        device_id: 'web-volunteer',
        filename: `patrol_test_${Date.now()}.mp4`,
        total_chunks: 2,
        latitude: lat,
        longitude: lon,
        content_type: 'video/mp4',
      });

      // 2. Upload dummy chunks (base64 encoded minimal bytes)
      const dummyBase64 = btoa('FLUX_VIDEO_TEST_CHUNK_PAYLOAD_DATA_' + Date.now());
      
      for (let i = 0; i < 2; i++) {
        setProgress({ phase: 'uploading', currentChunk: i + 1, totalChunks: 2, message: `Uploading chunk ${i + 1}/2...` });
        await uploadChunkWithRetry(session.session_id, i, dummyBase64);
      }

      // 3. Complete
      setProgress({ phase: 'completing', currentChunk: 2, totalChunks: 2, message: 'Finalizing & queuing for AI...' });
      const result = await completeUpload(session.session_id);

      setProgress({
        phase: 'done',
        currentChunk: 2,
        totalChunks: 2,
        message: `✅ Video uploaded! Event ID: ${result.event_id?.substring(0, 8) || 'queued'}`,
      });
    } catch (e: any) {
      console.error('Simulated video upload error:', e);
      setProgress({ phase: 'error', currentChunk: 0, totalChunks: 0, message: `Upload failed: ${e.message || e}` });
    }
  };

  // ─── Chunked Upload Pipeline ───────────────────────────────────────

  const handleVideoRecorded = async (videoUri: string) => {
    try {
      // 1. Get GPS location
      let latitude = 33.6844;
      let longitude = 73.0479;
      try {
        const loc = await Location.getCurrentPositionAsync({ accuracy: Location.Accuracy.Balanced });
        if (loc?.coords) {
          latitude = loc.coords.latitude;
          longitude = loc.coords.longitude;
        }
      } catch (_) {}

      // 2. Get file info
      const fileInfo = await FileSystem.getInfoAsync(videoUri);
      if (!fileInfo.exists || !fileInfo.size) {
        setProgress({ phase: 'error', currentChunk: 0, totalChunks: 0, message: 'Video file not found' });
        return;
      }
      const fileSize = fileInfo.size;
      const totalChunks = Math.ceil(fileSize / CHUNK_SIZE);

      setProgress({ phase: 'chunking', currentChunk: 0, totalChunks, message: `Preparing ${totalChunks} chunks...` });

      // 3. Create upload session
      const session = await createUploadSession({
        device_id: 'mobile-volunteer',
        filename: `recording_${Date.now()}.mp4`,
        total_chunks: totalChunks,
        latitude,
        longitude,
        content_type: 'video/mp4',
      });

      // 4. Upload each chunk with retries
      setProgress({ phase: 'uploading', currentChunk: 0, totalChunks, message: 'Uploading...' });

      for (let i = 0; i < totalChunks; i++) {
        const offset = i * CHUNK_SIZE;
        const length = Math.min(CHUNK_SIZE, fileSize - offset);

        const chunkBase64 = await FileSystem.readAsStringAsync(videoUri, {
          encoding: ((FileSystem as any).EncodingType?.Base64 || 'base64') as any,
          position: offset,
          length,
        });

        await uploadChunkWithRetry(session.session_id, i, chunkBase64);

        setProgress({
          phase: 'uploading',
          currentChunk: i + 1,
          totalChunks,
          message: `Uploaded chunk ${i + 1}/${totalChunks}`,
        });
      }

      // 5. Complete the upload
      setProgress({ phase: 'completing', currentChunk: totalChunks, totalChunks, message: 'Finalizing...' });
      const result = await completeUpload(session.session_id);

      setProgress({
        phase: 'done',
        currentChunk: totalChunks,
        totalChunks,
        message: `✅ Upload complete! Event: ${result.event_id.substring(0, 8)}`,
      });

      // Clean up local file
      await FileSystem.deleteAsync(videoUri, { idempotent: true });
    } catch (e: any) {
      console.error('Upload pipeline error:', e);
      setProgress({
        phase: 'error',
        currentChunk: 0,
        totalChunks: 0,
        message: `❌ Upload failed: ${e.message || e}`,
      });
    }
  };

  // ─── API Calls ─────────────────────────────────────────────────────

  const createUploadSession = async (params: {
    device_id: string;
    filename: string;
    total_chunks: number;
    latitude: number;
    longitude: number;
    content_type: string;
  }): Promise<UploadSession> => {
    const res = await fetch(`${API_BASE}/api/uploads`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(params),
    });
    if (!res.ok) throw new Error(`Session creation failed: ${res.status}`);
    return res.json();
  };

  const uploadChunkWithRetry = async (
    sessionId: string,
    chunkIndex: number,
    chunkBase64: string,
  ) => {
    let lastError: Error | null = null;

    for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
      let tempChunkFile: string | null = null;
      try {
        const formData = new FormData();
        
        if (Platform.OS === 'web') {
          const byteCharacters = atob(chunkBase64);
          const byteNumbers = new Array(byteCharacters.length);
          for (let b = 0; b < byteCharacters.length; b++) {
            byteNumbers[b] = byteCharacters.charCodeAt(b);
          }
          const byteArray = new Uint8Array(byteNumbers);
          const blob = new Blob([byteArray], { type: 'application/octet-stream' });
          formData.append('chunk', blob, `chunk_${chunkIndex}`);
        } else {
          tempChunkFile = `${(FileSystem as any).cacheDirectory || ''}chunk_${sessionId}_${chunkIndex}_${attempt}.bin`;
          await FileSystem.writeAsStringAsync(tempChunkFile, chunkBase64, {
            encoding: ((FileSystem as any).EncodingType?.Base64 || 'base64') as any,
          });
          formData.append('chunk', {
            uri: tempChunkFile,
            name: `chunk_${chunkIndex}`,
            type: 'application/octet-stream',
          } as any);
        }

        const res = await fetch(
          `${API_BASE}/api/uploads/${sessionId}/chunks/${chunkIndex}`,
          {
            method: 'PUT',
            body: formData,
            headers: { Accept: 'application/json' },
          },
        );

        if (tempChunkFile) {
          await FileSystem.deleteAsync(tempChunkFile, { idempotent: true }).catch(() => {});
        }

        if (res.ok) return;
        throw new Error(`Chunk upload failed: ${res.status}`);
      } catch (e: any) {
        if (tempChunkFile) {
          await FileSystem.deleteAsync(tempChunkFile, { idempotent: true }).catch(() => {});
        }
        lastError = e;
        // Exponential backoff: 1s, 2s, 4s
        const delay = Math.pow(2, attempt) * 1000;
        console.warn(`Chunk ${chunkIndex} attempt ${attempt + 1} failed, retrying in ${delay}ms...`);
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
    }
    throw lastError || new Error(`Chunk ${chunkIndex} upload failed after ${MAX_RETRIES} retries`);
  };

  const completeUpload = async (sessionId: string) => {
    const res = await fetch(`${API_BASE}/api/uploads/${sessionId}/complete`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
    });
    if (!res.ok) throw new Error(`Complete upload failed: ${res.status}`);
    return res.json();
  };

  // ─── Progress Bar ──────────────────────────────────────────────────

  const getProgressPercent = () => {
    if (progress.totalChunks === 0) return 0;
    return (progress.currentChunk / progress.totalChunks) * 100;
  };

  const getPhaseColor = () => {
    switch (progress.phase) {
      case 'done': return '#30d158';
      case 'error': return '#ff453a';
      case 'uploading': return '#0a84ff';
      case 'completing': return '#ff9f0a';
      default: return '#8e8e93';
    }
  };

  // ─── Render ────────────────────────────────────────────────────────

  const isUploading = ['chunking', 'uploading', 'completing'].includes(progress.phase);

  const renderContent = () => (
    <>
      <View style={styles.overlay}>
        <Text style={styles.title}>📹 Video Patrol</Text>
        <Text style={styles.subtitle}>Record & auto-chunk upload</Text>

        {isRecording && (
          <View style={styles.recordingBadge}>
            <View style={styles.recordingDot} />
            <Text style={styles.recordingText}>
              REC {Math.floor(recordingDuration / 60)}:{String(recordingDuration % 60).padStart(2, '0')}
            </Text>
          </View>
        )}
      </View>

      {/* Upload Progress */}
      {progress.phase !== 'idle' && (
        <View style={styles.progressPanel}>
          <Text style={[styles.progressMessage, { color: getPhaseColor() }]}>
            {progress.message}
          </Text>
          {isUploading && (
            <View style={styles.progressBarContainer}>
              <View style={[styles.progressBar, { width: `${getProgressPercent()}%`, backgroundColor: getPhaseColor() }]} />
            </View>
          )}
          {isUploading && <ActivityIndicator size="small" color={getPhaseColor()} style={{ marginTop: 8 }} />}
        </View>
      )}

      {/* Controls */}
      <View style={styles.buttonContainer}>
        {!isRecording ? (
          <TouchableOpacity
            style={[styles.button, isUploading && styles.buttonDisabled]}
            onPress={startRecording}
            disabled={isUploading}
          >
            <Text style={styles.buttonText}>
              {isUploading
                ? 'Uploading...'
                : Platform.OS === 'web'
                ? '📹 Test Chunked Video Upload'
                : '🔴 Start Recording'}
            </Text>
          </TouchableOpacity>
        ) : (
          <TouchableOpacity style={[styles.button, styles.buttonRecording]} onPress={stopRecording}>
            <Text style={styles.buttonText}>⏹ Stop Recording</Text>
          </TouchableOpacity>
        )}
      </View>
    </>
  );

  return (
    <View style={styles.container}>
      {permission?.granted && Platform.OS !== 'web' ? (
        <CameraView style={styles.camera} facing="back" mode="video" ref={cameraRef}>
          {renderContent()}
        </CameraView>
      ) : (
        <View style={[styles.camera, styles.webFallback]}>
          <Text style={styles.webFallbackTitle}>📹 Video Patrol Standby</Text>
          <Text style={styles.webFallbackText}>
            {Platform.OS === 'web'
              ? 'Web test mode active. You can trigger a multi-chunk upload test below.'
              : 'Camera permission required for video recording.'}
          </Text>
          {!permission?.granted && Platform.OS !== 'web' && (
            <TouchableOpacity style={styles.permissionBtn} onPress={requestPermission}>
              <Text style={styles.buttonText}>Grant Permission</Text>
            </TouchableOpacity>
          )}
          {renderContent()}
        </View>
      )}
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
  overlay: { position: 'absolute', top: 60, width: '100%', alignItems: 'center' },
  title: {
    fontSize: 24, fontWeight: 'bold', color: 'white',
    textShadowColor: 'black', textShadowOffset: { width: 1, height: 1 }, textShadowRadius: 3,
  },
  subtitle: { fontSize: 14, color: 'rgba(255,255,255,0.7)', marginTop: 4 },
  permissionText: { color: 'white', textAlign: 'center', marginBottom: 20, padding: 20, fontSize: 16 },

  recordingBadge: {
    flexDirection: 'row', alignItems: 'center',
    marginTop: 16, backgroundColor: 'rgba(255,59,48,0.8)',
    paddingHorizontal: 16, paddingVertical: 8, borderRadius: 20,
  },
  recordingDot: {
    width: 10, height: 10, borderRadius: 5,
    backgroundColor: 'white', marginRight: 8,
  },
  recordingText: { color: 'white', fontWeight: 'bold', fontSize: 16, fontVariant: ['tabular-nums'] },

  progressPanel: {
    position: 'absolute', bottom: 140, left: 20, right: 20,
    backgroundColor: 'rgba(0,0,0,0.75)', padding: 16,
    borderRadius: 12, borderWidth: 1, borderColor: 'rgba(255,255,255,0.15)',
  },
  progressMessage: { fontSize: 13, fontWeight: '600', textAlign: 'center' },
  progressBarContainer: {
    height: 4, backgroundColor: 'rgba(255,255,255,0.15)',
    borderRadius: 2, marginTop: 10, overflow: 'hidden',
  },
  progressBar: { height: '100%', borderRadius: 2 },

  buttonContainer: {
    flex: 1, flexDirection: 'row', backgroundColor: 'transparent',
    margin: 40, marginBottom: 60,
  },
  button: {
    flex: 1, alignSelf: 'flex-end', alignItems: 'center',
    backgroundColor: '#007AFF', padding: 16, borderRadius: 12,
  },
  buttonRecording: { backgroundColor: '#FF3B30' },
  buttonDisabled: { backgroundColor: '#555', opacity: 0.7 },
  buttonText: { fontSize: 18, fontWeight: 'bold', color: 'white' },
});
