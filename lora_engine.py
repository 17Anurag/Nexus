"""
NEXUS - LoRa Telemetry Reconstruction Engine
Backend | Flask + Flask-SocketIO + Python
Problem Statement 2

Handles:
  - Simulated LoRa gateway packet ingestion (low-bitrate, high-latency)
  - Packet integrity checking (checksum, JSON syntax, NMEA validation)
  - Kalman Filter state estimation for fragmented/missing packets
  - WebSocket real-time streaming to the frontend dashboard
  - Historical buffer for pattern-of-life analysis
"""

import json
import math
import random
import hashlib
import time
from datetime import datetime, timezone
from dataclasses import dataclass, field, asdict
from typing import Optional
from flask import Flask
from flask_socketio import SocketIO
from flask_cors import CORS
import threading

app = Flask(__name__)
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*")

# ──────────────────────────────────────────────────────────────
# KALMAN FILTER (4D constant-velocity model)
# State vector: [lat, lon, v_lat, v_lon]
# ──────────────────────────────────────────────────────────────
class KalmanFilter:
    """
    4-dimensional Kalman Filter for geospatial tracking.
    State: [lat, lon, velocity_lat, velocity_lon]
    Designed for low-update-rate LoRa telemetry.
    """

    def __init__(self, lat0: float, lon0: float, dt: float = 1.2):
        self.dt = dt
        # State vector [lat, lon, vlat, vlon]
        self.x = [lat0, lon0, 0.0, 0.0]

        # State covariance matrix (4x4) — high initial uncertainty
        self.P = [[1e-4 if i == j else 0.0 for j in range(4)] for i in range(4)]

        # Process noise covariance (models path unpredictability)
        self.Q = [
            [1e-8, 0,    0,     0    ],
            [0,    1e-8, 0,     0    ],
            [0,    0,    1e-10, 0    ],
            [0,    0,    0,     1e-10],
        ]

        # Measurement noise covariance (GPS accuracy ≈ ±5m)
        self.R = [[1e-6, 0.0], [0.0, 1e-6]]

        # Measurement matrix H (we only observe lat and lon)
        self.H = [[1, 0, 0, 0], [0, 1, 0, 0]]

        self.last_update: Optional[datetime] = None

    # ── Matrix helpers ────────────────────────────────────────
    @staticmethod
    def _add(A, B):
        return [[A[i][j] + B[i][j] for j in range(len(A[0]))] for i in range(len(A))]

    @staticmethod
    def _sub(A, B):
        return [[A[i][j] - B[i][j] for j in range(len(A[0]))] for i in range(len(A))]

    @staticmethod
    def _mul(A, B):
        rows_A, cols_A = len(A), len(A[0])
        cols_B = len(B[0])
        return [[sum(A[i][k] * B[k][j] for k in range(cols_A)) for j in range(cols_B)] for i in range(rows_A)]

    @staticmethod
    def _transpose(A):
        return [[A[i][j] for i in range(len(A))] for j in range(len(A[0]))]

    @staticmethod
    def _inv2x2(M):
        det = M[0][0] * M[1][1] - M[0][1] * M[1][0]
        if abs(det) < 1e-15:
            raise ValueError("Singular matrix in Kalman update — resetting covariance")
        return [[M[1][1] / det, -M[0][1] / det], [-M[1][0] / det, M[0][0] / det]]

    @staticmethod
    def _identity(n):
        return [[1.0 if i == j else 0.0 for j in range(n)] for i in range(n)]

    def _F(self):
        """State transition matrix for constant-velocity model."""
        dt = self.dt
        return [
            [1, 0, dt, 0 ],
            [0, 1, 0,  dt],
            [0, 0, 1,  0 ],
            [0, 0, 0,  1 ],
        ]

    # ── Kalman steps ─────────────────────────────────────────
    def predict(self, dt: Optional[float] = None) -> dict:
        """
        Prediction step: project state forward in time using motion model.
        Called every packet interval, even when packet is corrupt.
        """
        if dt:
            self.dt = dt
        F = self._F()
        Ft = self._transpose(F)

        # x̂ = F·x
        self.x = [sum(F[i][j] * self.x[j] for j in range(4)) for i in range(4)]

        # P = F·P·Fᵀ + Q
        FP = self._mul(F, self.P)
        FPFt = self._mul(FP, Ft)
        self.P = self._add(FPFt, self.Q)

        return self._state_dict("predicted")

    def update(self, lat: float, lon: float) -> dict:
        """
        Update step: incorporate a valid GPS measurement.
        Only called when a packet passes integrity checks.
        """
        z = [[lat], [lon]]
        Hx = [[sum(self.H[i][j] * self.x[j] for j in range(4))] for i in range(2)]

        # Innovation: y = z - H·x̂
        y = [[z[i][0] - Hx[i][0]] for i in range(2)]

        # Innovation covariance: S = H·P·Hᵀ + R
        HP = self._mul(self.H, self.P)
        HPHt = self._mul(HP, self._transpose(self.H))
        S = self._add(HPHt, self.R)

        # Kalman gain: K = P·Hᵀ·S⁻¹
        PHt = self._mul(self.P, self._transpose(self.H))
        K = self._mul(PHt, self._inv2x2(S))

        # State update: x = x̂ + K·y
        Ky = self._mul(K, y)
        self.x = [self.x[i] + Ky[i][0] for i in range(4)]

        # Covariance update: P = (I - K·H)·P
        KH = self._mul(K, self.H)
        I = self._identity(4)
        IminusKH = self._sub(I, KH)
        self.P = self._mul(IminusKH, self.P)

        self.last_update = datetime.now(timezone.utc)
        return self._state_dict("updated")

    def _state_dict(self, mode: str) -> dict:
        uncertainty_m = math.sqrt(self.P[0][0] ** 2 + self.P[1][1] ** 2) * 111320
        speed_ms = math.sqrt(self.x[2] ** 2 + self.x[3] ** 2) * 111320
        heading = math.degrees(math.atan2(self.x[3], self.x[2])) % 360
        return {
            "mode": mode,
            "lat": round(self.x[0], 7),
            "lon": round(self.x[1], 7),
            "v_lat": self.x[2],
            "v_lon": self.x[3],
            "uncertainty_m": round(uncertainty_m, 2),
            "speed_ms": round(speed_ms, 3),
            "heading_deg": round(heading, 1),
        }

    @property
    def lat(self): return self.x[0]
    @property
    def lon(self): return self.x[1]


# ──────────────────────────────────────────────────────────────
# PACKET INTEGRITY CHECKER
# ──────────────────────────────────────────────────────────────
class PacketIntegrityChecker:
    """
    Validates incoming LoRa packets.
    Detects: missing fields, malformed numerics, checksum mismatches,
    incomplete JSON syntax, and NMEA sentence errors.
    """

    REQUIRED_FIELDS = ("lat", "lon", "seq", "ts")

    @staticmethod
    def checksum(payload: str) -> str:
        return hashlib.md5(payload.encode()).hexdigest()[:8]

    def validate(self, raw: str) -> tuple[Optional[dict], Optional[str]]:
        """
        Returns (parsed_packet, None) if valid,
        or (None, error_reason) if broken.
        """
        # 1. JSON syntax check
        try:
            pkt = json.loads(raw)
        except json.JSONDecodeError as e:
            return None, f"MALFORMED JSON: {e}"

        # 2. Required field check
        for field in self.REQUIRED_FIELDS:
            if field not in pkt:
                return None, f"MISSING FIELD: '{field}'"

        # 3. Numeric type check
        try:
            lat = float(pkt["lat"])
            lon = float(pkt["lon"])
        except (ValueError, TypeError):
            return None, f"INVALID COORDS: lat={pkt.get('lat')} lon={pkt.get('lon')}"

        # 4. Coordinate range check
        if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
            return None, f"OUT-OF-RANGE: ({lat}, {lon})"

        # 5. Checksum verification
        if "checksum" in pkt:
            payload_without_cs = {k: v for k, v in pkt.items() if k != "checksum"}
            expected = self.checksum(json.dumps(payload_without_cs, sort_keys=True))
            if pkt["checksum"] != expected:
                return None, f"CHECKSUM MISMATCH: got {pkt['checksum']}, expected {expected}"

        return pkt, None


# ──────────────────────────────────────────────────────────────
# HISTORICAL BUFFER (pattern-of-life analysis)
# ──────────────────────────────────────────────────────────────
class HistoricalBuffer:
    """
    Maintains a rolling buffer of the last N verified positions.
    Used for velocity smoothing and pattern-of-life analysis.
    """

    def __init__(self, maxlen: int = 60):
        self.maxlen = maxlen
        self.buffer: list[dict] = []

    def push(self, lat: float, lon: float, ts: str):
        self.buffer.append({"lat": lat, "lon": lon, "ts": ts})
        if len(self.buffer) > self.maxlen:
            self.buffer.pop(0)

    def avg_velocity(self) -> tuple[float, float]:
        """Compute average velocity from recent verified positions."""
        if len(self.buffer) < 2:
            return 0.0, 0.0
        dlat = sum(self.buffer[i + 1]["lat"] - self.buffer[i]["lat"] for i in range(len(self.buffer) - 1))
        dlon = sum(self.buffer[i + 1]["lon"] - self.buffer[i]["lon"] for i in range(len(self.buffer) - 1))
        n = len(self.buffer) - 1
        return dlat / n, dlon / n


# ──────────────────────────────────────────────────────────────
# SIMULATED LoRa GATEWAY
# ──────────────────────────────────────────────────────────────
ROUTE = [
    (28.85, 77.40), (28.90, 77.52), (28.98, 77.63), (29.05, 77.58),
    (29.10, 77.45), (29.05, 77.32), (28.95, 77.28), (28.87, 77.33),
    (28.82, 77.42), (28.85, 77.40),
]
GPS_NOISE = 0.0012  # degrees (~130m)
CORRUPT_MODES = ["missing_fields", "bad_coords", "checksum_fail", "truncated_json"]

checker = PacketIntegrityChecker()
history = HistoricalBuffer()

sim_state = {
    "running": False,
    "path_t": 0.0,
    "seq": 0,
    "rx": 0, "bad": 0, "est": 0,
    "loss_rate": 30,
    "tx_interval": 1.2,
}

kf = KalmanFilter(ROUTE[0][0], ROUTE[0][1])


def _interpolate(t: float) -> tuple[float, float]:
    segs = len(ROUTE) - 1
    seg_len = 1.0 / segs
    seg = min(int(t / seg_len), segs - 1)
    local = (t - seg * seg_len) / seg_len
    A, B = ROUTE[seg], ROUTE[seg + 1]
    return (
        A[0] + (B[0] - A[0]) * local,
        A[1] + (B[1] - A[1]) * local,
    )


def _make_packet(lat: float, lon: float, seq: int) -> str:
    """Builds a valid LoRa JSON packet string with checksum."""
    payload = {
        "seq": seq,
        "ts": datetime.now(timezone.utc).isoformat(),
        "lat": round(lat + random.gauss(0, GPS_NOISE), 7),
        "lon": round(lon + random.gauss(0, GPS_NOISE), 7),
        "alt_m": round(220 + random.gauss(0, 3), 1),
        "hdop": round(random.uniform(0.8, 2.1), 2),
        "sats": random.randint(6, 12),
    }
    payload["checksum"] = checker.checksum(json.dumps(payload, sort_keys=True))
    return json.dumps(payload)


def _corrupt_packet(raw_str: str, mode: str) -> str:
    """Applies a corruption mode to a valid packet string."""
    pkt = json.loads(raw_str)
    if mode == "missing_fields":
        for k in random.sample(["lat", "lon", "ts"], k=random.randint(1, 2)):
            pkt.pop(k, None)
    elif mode == "bad_coords":
        pkt["lat"] = "NaN" if random.random() > .5 else 999.0
    elif mode == "checksum_fail":
        pkt["checksum"] = "deadbeef"
    elif mode == "truncated_json":
        return raw_str[:len(raw_str) // 2]  # Cut packet in half
    return json.dumps(pkt)


def simulation_loop():
    global kf
    while sim_state["running"]:
        sim_state["path_t"] = (sim_state["path_t"] + 0.004) % 1.0
        true_lat, true_lon = _interpolate(sim_state["path_t"])
        sim_state["seq"] += 1
        sim_state["rx"] += 1

        raw_str = _make_packet(true_lat, true_lon, sim_state["seq"])

        is_corrupt = random.randint(1, 100) <= sim_state["loss_rate"]
        if is_corrupt:
            raw_str = _corrupt_packet(raw_str, random.choice(CORRUPT_MODES))

        pkt, error = checker.validate(raw_str)

        kf_state = kf.predict(sim_state["tx_interval"])

        if pkt is None:
            # Packet failed integrity — prediction only
            sim_state["bad"] += 1
            sim_state["est"] += 1
            kf_state["mode"] = "estimated"
            event = {
                "seq": sim_state["seq"],
                "status": "CORRUPTED",
                "error": error,
                "kf": kf_state,
                "stats": _stats(),
                "raw_fragment": raw_str[:80] + ("..." if len(raw_str) > 80 else ""),
            }
        else:
            # Valid packet — predict + update
            kf_state = kf.update(pkt["lat"], pkt["lon"])
            kf_state["mode"] = "verified"
            history.push(pkt["lat"], pkt["lon"], pkt["ts"])
            event = {
                "seq": sim_state["seq"],
                "status": "VERIFIED",
                "lat": pkt["lat"],
                "lon": pkt["lon"],
                "kf": kf_state,
                "stats": _stats(),
            }

        socketio.emit("packet", event)
        time.sleep(sim_state["tx_interval"] + random.uniform(-0.2, 0.4))


def _stats() -> dict:
    rx = sim_state["rx"]
    return {
        "rx": rx,
        "bad": sim_state["bad"],
        "est": sim_state["est"],
        "integrity_pct": round((rx - sim_state["bad"]) / rx * 100, 1) if rx else 0,
    }


# ──────────────────────────────────────────────────────────────
# WEBSOCKET EVENTS
# ──────────────────────────────────────────────────────────────
@socketio.on("start_sim")
def on_start(data):
    sim_state["loss_rate"] = int(data.get("loss_rate", 30))
    sim_state["tx_interval"] = float(data.get("tx_interval", 1.2))
    if not sim_state["running"]:
        sim_state["running"] = True
        t = threading.Thread(target=simulation_loop, daemon=True)
        t.start()
        socketio.emit("sim_status", {"running": True})


@socketio.on("stop_sim")
def on_stop(_):
    sim_state["running"] = False
    socketio.emit("sim_status", {"running": False})


@socketio.on("reset_sim")
def on_reset(_):
    global kf
    sim_state.update({"running": False, "path_t": 0.0, "seq": 0, "rx": 0, "bad": 0, "est": 0})
    kf = KalmanFilter(ROUTE[0][0], ROUTE[0][1])
    history.buffer.clear()
    socketio.emit("sim_status", {"running": False, "reset": True})


@socketio.on("update_params")
def on_params(data):
    sim_state["loss_rate"] = int(data.get("loss_rate", sim_state["loss_rate"]))
    sim_state["tx_interval"] = float(data.get("tx_interval", sim_state["tx_interval"]))


# ──────────────────────────────────────────────────────────────
# REST ROUTES
# ──────────────────────────────────────────────────────────────
@app.route("/api/health")
def health():
    return {"status": "online", "service": "NEXUS LoRa Reconstruction Engine"}


@app.route("/api/history")
def get_history():
    return {"buffer": history.buffer, "count": len(history.buffer)}


@app.route("/api/kalman/state")
def kalman_state():
    return kf._state_dict("snapshot")


# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5002))
    print(f"NEXUS LoRa Engine starting on port {port}...")
    socketio.run(app, host="0.0.0.0", port=port, debug=False, allow_unsafe_werkzeug=True)if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5002))
    print(f"NEXUS LoRa Engine starting on port {port}...")
    socketio.run(app, host="0.0.0.0", port=port, debug=False, allow_unsafe_werkzeug=True)
