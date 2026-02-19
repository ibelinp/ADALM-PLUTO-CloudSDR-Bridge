#!/usr/bin/env python3
"""
ADALM-PLUTO CloudSDR Bridge v2.0
Connects ADALM-PLUTO to SpectraVue via CloudSDR protocol emulation.

This bridge allows SpectraVue to connect to an ADALM-PLUTO as if it were a
CloudSDR receiver. Supports contiguous streaming and FIFO block capture modes,
adaptive master clock selection, gain controls, and frequency offset for
accessing the full PLUTO tuning range.

Based on the USRP B210 CloudSDR Bridge architecture.
Author: Pieter Ibelings
License: MIT License
Version: 2.0
"""

import socket
import struct
import threading
import time
import logging
import argparse
import math
import numpy as np
import queue
from collections import deque
from typing import Dict, Any, Optional, Tuple

# Import pyadi-iio for PLUTO
try:
    import adi
    PLUTO_AVAILABLE = True
    print("pyadi-iio found")
except ImportError:
    PLUTO_AVAILABLE = False
    print("WARNING: pyadi-iio not found - running in simulation mode")
    print("   Install: pip install pyadi-iio")

# =============================================================================
# FREQUENCY OFFSET CONFIGURATION
# =============================================================================
"""
SpectraVue limitation: Can only tune 0-2 GHz
PLUTO capability: 70 MHz - 6 GHz (with firmware mod)

Use --offset to access PLUTO's full range beyond SpectraVue's 2 GHz limit:
  --offset 0        → Normal operation (0-2 GHz)
  --offset 2e9      → Access 2-4 GHz (SpectraVue 0-2GHz maps to PLUTO 2-4GHz)
  --offset 4e9      → Access 4-6 GHz (SpectraVue 0-2GHz maps to PLUTO 4-6GHz)

Example: --offset 2.4e9
  SpectraVue shows 100 MHz → PLUTO actually tunes to 2.5 GHz

Safety: Validates final frequency is within PLUTO range (70 MHz - 6 GHz)
"""

# Global frequency offset (set via command line --offset)
FREQUENCY_OFFSET = 0.0  # Hz - Added to all SpectraVue frequencies

# =============================================================================
# SPECTRAVUE TO PLUTO GAIN MAPPING CONFIGURATION
# =============================================================================
"""
SpectraVue sends gain commands via CI_RX_RF_GAIN (0x0038) using these values:
- 0dB   (0x00) - Maximum sensitivity
- -10dB (0xF6) - High sensitivity
- -20dB (0xEC) - Medium sensitivity
- -30dB (0xE2) - Low sensitivity

These are mapped to optimal PLUTO hardware gain values (0-73dB range):
- PLUTO gain selection balances sensitivity vs noise performance
- Values chosen based on SDR best practices
- All gain settings are safe for PLUTO hardware
"""

# SpectraVue gain levels (signed 8-bit values)
SPECTRAVUE_GAIN_0DB = 0      # 0x00
SPECTRAVUE_GAIN_10DB = -10   # 0xF6
SPECTRAVUE_GAIN_20DB = -20   # 0xEC
SPECTRAVUE_GAIN_30DB = -30   # 0xE2

# PLUTO hardware gain mappings (0-73dB range)
PLUTO_GAIN_MAP = {
    SPECTRAVUE_GAIN_0DB:  60,   # High sensitivity for weak signals
    SPECTRAVUE_GAIN_10DB: 45,   # Good sensitivity, reduced noise
    SPECTRAVUE_GAIN_20DB: 30,   # Medium sensitivity for average signals
    SPECTRAVUE_GAIN_30DB: 15,   # Low sensitivity for strong signals
}

# Default PLUTO gain if SpectraVue sends unexpected value
DEFAULT_PLUTO_GAIN = 45  # Safe middle-ground value

class PlutoInterface:
    """
    Interface to ADALM-PLUTO hardware.

    Uses double-buffering to absorb USB burst transfers and provide smooth
    sample delivery to UDP clients. Supports exact CloudSDR sample rates
    via adaptive master clock selection.
    """

    # Available PLUTO master clocks (same AD936x family as B210)
    MASTER_CLOCKS = [
        61.44e6,    # Primary CloudSDR clock
        30.72e6,    # Half clock (for high decimation rates)
        15.36e6,    # Quarter clock
        7.68e6,     # Eighth clock
    ]

    # CloudSDR standard rates - PLUTO supports 2.048, 1.2288, 0.6144 MHz
    # Format: rate_hz: (preferred_master_clock, decimation)
    CLOUDSDR_EXACT_RATES = {
        2048000:   (61.44e6, 30),     # 61.44 ÷ 30 = 2.048 MHz
        1228800:   (61.44e6, 50),     # 61.44 ÷ 50 = 1.2288 MHz
        614400:    (61.44e6, 100),    # 61.44 ÷ 100 = 614.4 kHz
    }

    def __init__(self, simulation_mode=False, pluto_uri="ip:192.168.2.1"):
        """Initialize PLUTO interface with double-buffering architecture."""
        self.simulation_mode = simulation_mode
        self.pluto_uri = pluto_uri
        self.sample_rate = 2.048e6
        self.center_freq = 100e6
        self.gain = 30
        self.running = False

        # PLUTO objects
        self.sdr = None

        # Double-buffering architecture using numpy ring buffers.
        # The primary buffer absorbs USB burst transfers from the PLUTO.
        # The secondary buffer provides smooth delivery to the UDP sender.
        # Both use pre-allocated numpy arrays with write/read indices for
        # zero-copy bulk operations — critical for sustaining 2.048 MS/s.
        self.buffer_lock = threading.Lock()

        default_capacity = int(2.048e6 * 4.0)
        self.primary_buf = np.zeros(default_capacity, dtype=np.complex64)
        self.primary_write = 0  # Next write position
        self.primary_count = 0  # Samples currently in buffer

        secondary_capacity = int(2.048e6 * 1.0)
        self.secondary_buf = np.zeros(secondary_capacity, dtype=np.complex64)
        self.secondary_write = 0
        self.secondary_read = 0
        self.secondary_count = 0
        self.secondary_target = int(2.048e6 * 0.3)

        # Buffer statistics
        self.samples_written = 0
        self.samples_read = 0
        self.buffer_underruns = 0
        self.buffer_overruns = 0
        self.last_buffer_check = time.time()

        # Thread control
        self.producer_running = False
        self.consumer_running = False
        self.buffer_manager_thread = None

        # Rate tracking
        self.last_rate_check = time.time()
        self.last_samples_written = 0
        self.last_samples_read = 0
        self.actual_write_rate = 0
        self.actual_read_rate = 0
        self.write_rate_history = deque(maxlen=10)
        self.read_rate_history = deque(maxlen=10)

        # Statistics
        self.samples_generated = 0

        # Current master clock (adaptive)
        self.current_master_clock = 61.44e6

    def find_optimal_master_clock(self, target_rate):
        """Find optimal master clock and decimation for target rate."""
        for master_clock in self.MASTER_CLOCKS:
            decimation = master_clock / target_rate
            if 1 <= decimation <= 512 and abs(decimation - round(decimation)) < 0.001:
                return master_clock, int(round(decimation))
        return None, None

    def get_exact_rate_config(self, requested_rate):
        """Get exact PLUTO configuration with adaptive master clock selection."""
        if requested_rate in self.CLOUDSDR_EXACT_RATES:
            master_clock, decimation = self.CLOUDSDR_EXACT_RATES[requested_rate]
            exact_rate = master_clock / decimation

            print(f"PLUTO: Using master clock {master_clock/1e6:.2f} MHz, decimation {decimation}")
            return exact_rate, master_clock, True
        else:
            # Try to find optimal master clock for non-standard rate
            master_clock, decimation = self.find_optimal_master_clock(requested_rate)
            if master_clock is not None:
                exact_rate = master_clock / decimation
                print(f"PLUTO: Custom rate using master clock {master_clock/1e6:.2f} MHz, decimation {decimation}")
                return exact_rate, master_clock, False
            else:
                # Fallback to closest standard rate
                closest_rate = min(self.CLOUDSDR_EXACT_RATES.keys(),
                                 key=lambda x: abs(x - requested_rate))
                master_clock, decimation = self.CLOUDSDR_EXACT_RATES[closest_rate]
                exact_rate = master_clock / decimation
                print(f"PLUTO: Fallback to closest rate {exact_rate/1e6:.6f} MHz")
                return exact_rate, master_clock, False

    def initialize(self):
        """Initialize PLUTO hardware using pyadi-iio."""
        if self.simulation_mode or not PLUTO_AVAILABLE:
            print("PLUTO: Running in simulation mode")
            return True

        try:
            print(f"PLUTO: Connecting to {self.pluto_uri}...")

            self.sdr = adi.Pluto(uri=self.pluto_uri)

            print(f"PLUTO: Connected successfully")
            print(f"PLUTO: Hardware ID: {self.sdr.ctx.attrs.get('hw_model', 'Unknown')}")

            # RX buffer size — set during configure() based on sample rate
            self.sdr.rx_buffer_size = 32768

            print("PLUTO: Hardware initialized")
            return True

        except Exception as e:
            print(f"PLUTO: Hardware initialization failed: {e}")
            print("PLUTO: Falling back to simulation mode")
            self.simulation_mode = True
            return True

    def configure(self, requested_sample_rate, center_freq, gain, verbose=1):
        """Configure PLUTO with adaptive master clock for exact CloudSDR sample rates."""
        exact_rate, master_clock, is_exact = self.get_exact_rate_config(requested_sample_rate)

        if is_exact:
            print(f"PLUTO: EXACT CloudSDR rate - Requested: {requested_sample_rate/1e6:.6f} MHz -> Exact: {exact_rate/1e6:.6f} MHz")
        else:
            print(f"PLUTO: Non-standard rate - Requested: {requested_sample_rate/1e6:.6f} MHz -> Closest: {exact_rate/1e6:.6f} MHz")

        print(f"PLUTO: Freq: {center_freq/1e6:.3f} MHz, Gain: {gain} dB")

        if self.simulation_mode:
            self.sample_rate = exact_rate
            self.center_freq = center_freq
            self.gain = gain
            self.current_master_clock = master_clock
            print("PLUTO: Simulation configured")
            self._update_buffer_sizes()
            return True

        try:
            # Configure PLUTO sample rate first
            self.sdr.sample_rate = int(exact_rate)
            actual_rate = self.sdr.sample_rate
            print(f"   Sample Rate configured: {actual_rate/1e6:.6f} MHz")

            rate_error = abs(actual_rate - exact_rate)
            if rate_error > 1.0:
                print(f"   Rate error: {rate_error:.1f} Hz")
            else:
                print(f"   Exact rate achieved: {actual_rate:.6f} Hz")

            # Scale RX buffer to ~16ms worth of samples (larger = fewer USB
            # transactions = less overhead, but more latency per burst).
            buf_size = max(4096, int(actual_rate * 0.016))
            # Round to power of 2 for DMA alignment
            buf_size = 1 << (buf_size - 1).bit_length()
            self.sdr.rx_buffer_size = buf_size
            print(f"   RX buffer size: {buf_size} samples ({buf_size/actual_rate*1000:.1f}ms)")

            # Set RF bandwidth BEFORE setting frequency
            self.sdr.rx_rf_bandwidth = int(actual_rate)

            # Set frequency (center tuning - no LO offset for now)
            self.sdr.rx_lo = int(center_freq)
            actual_freq = self.sdr.rx_lo

            if verbose >= 2:
                print(f"DEBUG: Center tuning - Target: {center_freq/1e6:.3f} MHz (no LO offset)")

            # Set gain (manual gain mode)
            self.sdr.gain_control_mode_chan0 = "manual"
            self.sdr.rx_hardwaregain_chan0 = gain
            actual_gain = self.sdr.rx_hardwaregain_chan0

            # Store configuration
            self.sample_rate = actual_rate
            self.center_freq = actual_freq
            self.gain = actual_gain
            self.current_master_clock = master_clock

            self._update_buffer_sizes()

            print(f"PLUTO configured:")
            print(f"   Frequency: {actual_freq/1e6:.6f} MHz")
            print(f"   Sample Rate: {actual_rate:.6f} Hz ({actual_rate/1e6:.6f} MHz)")
            print(f"   Gain: {actual_gain:.1f} dB")
            print(f"   RF Bandwidth: {self.sdr.rx_rf_bandwidth/1e6:.3f} MHz")

            return True

        except Exception as e:
            print(f"PLUTO: Configuration failed: {e}")
            import traceback
            traceback.print_exc()
            return False

    def _update_buffer_sizes(self):
        """Update buffer sizes based on current sample rate."""
        with self.buffer_lock:
            primary_capacity = int(self.sample_rate * 4.0)
            secondary_capacity = int(self.sample_rate * 1.0)
            self.secondary_target = int(self.sample_rate * 0.3)

            self.primary_buf = np.zeros(primary_capacity, dtype=np.complex64)
            self.primary_write = 0
            self.primary_count = 0

            self.secondary_buf = np.zeros(secondary_capacity, dtype=np.complex64)
            self.secondary_write = 0
            self.secondary_read = 0
            self.secondary_count = 0

            print(f"   Primary Buffer: {primary_capacity/1e3:.0f}k samples ({primary_capacity/self.sample_rate:.1f}s)")
            print(f"   Secondary Buffer: {secondary_capacity/1e3:.0f}k samples ({secondary_capacity/self.sample_rate:.1f}s)")
            print(f"   Target Depth: {self.secondary_target/1e3:.0f}k samples ({self.secondary_target/self.sample_rate:.1f}s)")

    def start_streaming(self):
        """Start double-buffered streaming from PLUTO hardware."""
        if self.running:
            return

        self.running = True
        self.producer_running = True
        self.consumer_running = True

        print("PLUTO: Starting streaming architecture...")

        # Reset statistics
        self.samples_written = 0
        self.samples_read = 0
        self.buffer_underruns = 0
        self.buffer_overruns = 0
        self.last_rate_check = time.time()

        # Start producer thread (PLUTO -> Primary buffer)
        producer_thread = threading.Thread(target=self._pluto_producer_thread, daemon=True)
        producer_thread.start()

        # Start buffer manager (Primary -> Secondary buffer)
        self.buffer_manager_thread = threading.Thread(target=self._buffer_manager_thread, daemon=True)
        self.buffer_manager_thread.start()

    def stop_streaming(self):
        """Stop streaming."""
        if not self.running:
            return

        self.running = False
        self.producer_running = False
        self.consumer_running = False
        print("PLUTO: Stopping streaming...")

    def _pluto_producer_thread(self):
        """Producer thread entry point."""
        try:
            if self.simulation_mode:
                self._simulation_producer()
            else:
                self._hardware_producer()
        except Exception as e:
            print(f"PLUTO: Producer error: {e}")

    def _ring_write(self, buf, write_pos, count, capacity, data):
        """Write a numpy array into a ring buffer. Returns (new_write_pos, new_count)."""
        n = len(data)
        space = capacity - count
        if n > space:
            # Overflow: drop oldest by advancing the read side implicitly
            n = space
            data = data[:n]
        end = write_pos + n
        if end <= capacity:
            buf[write_pos:end] = data
        else:
            first = capacity - write_pos
            buf[write_pos:capacity] = data[:first]
            buf[0:n - first] = data[first:]
        return (write_pos + n) % capacity, count + n

    def _ring_read(self, buf, read_pos, count, capacity, n):
        """Read n samples from a ring buffer. Returns (data, new_read_pos, new_count)."""
        if n > count:
            n = count
        if n == 0:
            return None, read_pos, count
        end = read_pos + n
        if end <= capacity:
            data = buf[read_pos:end].copy()
        else:
            first = capacity - read_pos
            data = np.empty(n, dtype=buf.dtype)
            data[:first] = buf[read_pos:capacity]
            data[first:] = buf[0:n - first]
        return data, (read_pos + n) % capacity, count - n

    def _buffer_manager_thread(self):
        """Transfer samples from primary to secondary buffer in bulk."""
        print("PLUTO: Buffer manager started")

        target_rate = self.sample_rate
        transfer_interval = 0.005  # 5ms intervals
        transfer_multiplier = 3.0 if target_rate > 1e6 else 2.0
        samples_per_transfer = int(target_rate * transfer_interval * transfer_multiplier)

        last_transfer_time = time.perf_counter()

        while self.consumer_running:
            try:
                current_time = time.perf_counter()

                if current_time - last_transfer_time >= transfer_interval:
                    with self.buffer_lock:
                        primary_cap = len(self.primary_buf)
                        secondary_cap = len(self.secondary_buf)
                        secondary_space = secondary_cap - self.secondary_count

                        n = min(samples_per_transfer, self.primary_count, secondary_space)

                        if n > 0:
                            # Bulk read from primary ring
                            # Calculate read position (write_pos - count)
                            primary_read = (self.primary_write - self.primary_count) % primary_cap
                            chunk, primary_read, self.primary_count = self._ring_read(
                                self.primary_buf, primary_read, self.primary_count, primary_cap, n)

                            if chunk is not None:
                                # Bulk write to secondary ring
                                self.secondary_write, self.secondary_count = self._ring_write(
                                    self.secondary_buf, self.secondary_write,
                                    self.secondary_count, secondary_cap, chunk)

                    last_transfer_time = current_time
                else:
                    sleep_time = transfer_interval - (current_time - last_transfer_time)
                    if sleep_time > 0:
                        time.sleep(sleep_time * 0.5)

            except Exception as e:
                print(f"PLUTO: Buffer manager error: {e}")
                break

    def _simulation_producer(self):
        """Simulation producer generating test signal bursts."""
        print("PLUTO: Simulation producer started")

        carrier_freq = self.sample_rate * 0.2
        phase = 0.0
        burst_size = int(self.sample_rate * 0.05)  # 50ms bursts
        burst_interval = 0.03  # 30ms between bursts

        while self.producer_running:
            try:
                noise_power = 0.05
                noise = noise_power * (np.random.randn(burst_size) + 1j * np.random.randn(burst_size))

                phase_increment = 2 * np.pi * carrier_freq / self.sample_rate
                phases = phase + np.arange(burst_size) * phase_increment
                carrier = 0.2 * np.exp(1j * phases)
                phase = (phase + burst_size * phase_increment) % (2 * np.pi)

                signal = (noise + carrier).astype(np.complex64)

                with self.buffer_lock:
                    cap = len(self.primary_buf)
                    self.primary_write, self.primary_count = self._ring_write(
                        self.primary_buf, self.primary_write, self.primary_count, cap, signal)
                    self.samples_written += len(signal)

                self.samples_generated += burst_size
                time.sleep(burst_interval)

            except Exception as e:
                print(f"PLUTO: Simulation error: {e}")
                break

    def _hardware_producer(self):
        """Hardware producer: absorb PLUTO USB bursts into primary ring buffer."""
        print("PLUTO: Hardware producer started")

        try:
            if not self.sdr:
                print("PLUTO: No SDR object available")
                return

            self.sdr.rx_enabled_channels = [0]

            print(f"PLUTO: Producer ready - buffer size {self.sdr.rx_buffer_size}")

            last_perf_check = time.perf_counter()
            samples_in_period = 0

            while self.producer_running:
                try:
                    samples = self.sdr.rx()

                    if samples is not None and len(samples) > 0:
                        chunk = np.asarray(samples, dtype=np.complex64)
                        with self.buffer_lock:
                            cap = len(self.primary_buf)
                            self.primary_write, self.primary_count = self._ring_write(
                                self.primary_buf, self.primary_write,
                                self.primary_count, cap, chunk)
                            self.samples_written += len(chunk)

                        self.samples_generated += len(chunk)
                        samples_in_period += len(chunk)

                    current_time = time.perf_counter()
                    if current_time - last_perf_check > 5.0:
                        elapsed = current_time - last_perf_check
                        actual_rate = samples_in_period / elapsed
                        rate_error = abs(actual_rate - self.sample_rate) / self.sample_rate * 100

                        if rate_error > 5.0:
                            print(f"   Producer: {actual_rate:.0f} Hz ({rate_error:.1f}% error)")

                        last_perf_check = current_time
                        samples_in_period = 0

                except Exception as e:
                    print(f"PLUTO: Receive error: {e}")
                    break

        except Exception as e:
            print(f"PLUTO: Producer error: {e}")
        finally:
            if self.sdr:
                try:
                    self.sdr.rx_destroy_buffer()
                except:
                    pass

    def get_iq_block(self, total_samples):
        """Capture a single block of IQ samples directly from PLUTO hardware.

        Used by FIFO mode where SpectraVue requests discrete blocks of samples
        rather than a continuous stream. Each block is independent — phase
        continuity between blocks is not required.

        Returns a numpy complex64 array of total_samples, or None on error.
        """
        if self.simulation_mode:
            # Generate a block of simulated data
            carrier_freq = self.sample_rate * 0.2
            noise_power = 0.05
            t = np.arange(total_samples) / self.sample_rate
            carrier = 0.2 * np.exp(1j * 2 * np.pi * carrier_freq * t)
            noise = noise_power * (np.random.randn(total_samples) + 1j * np.random.randn(total_samples))
            return (carrier + noise).astype(np.complex64)

        if not self.sdr:
            return None

        try:
            # Set buffer size to match requested block
            self.sdr.rx_buffer_size = total_samples
            self.sdr.rx_enabled_channels = [0]

            # Single-shot capture
            samples = self.sdr.rx()

            # Destroy buffer to reset for next capture
            self.sdr.rx_destroy_buffer()

            if samples is not None and len(samples) > 0:
                return np.array(samples, dtype=np.complex64)
            return None

        except Exception as e:
            print(f"PLUTO: Block capture error: {e}")
            try:
                self.sdr.rx_destroy_buffer()
            except:
                pass
            return None

    def get_iq_data(self, num_samples):
        """Get IQ data from secondary ring buffer."""
        with self.buffer_lock:
            if self.secondary_count < num_samples:
                self.buffer_underruns += 1
                return None

            cap = len(self.secondary_buf)
            data, self.secondary_read, self.secondary_count = self._ring_read(
                self.secondary_buf, self.secondary_read,
                self.secondary_count, cap, num_samples)
            if data is not None:
                self.samples_read += len(data)
            return data


class CloudSDREmulator:
    """
    CloudSDR protocol emulator for ADALM-PLUTO.

    Emulates a CloudSDR device, allowing SpectraVue and other CloudSDR-compatible
    software to connect to an ADALM-PLUTO transparently.

    Protocol implementation based on the USRP B210 bridge.
    """

    def __init__(self, host: str = "localhost", port: int = 50000, verbose: int = 1, frequency_offset: float = 0.0, pluto_uri: str = "ip:192.168.2.1"):
        self.host = host
        self.port = port
        self.verbose = verbose
        self.frequency_offset = frequency_offset
        self.tcp_socket = None
        self.udp_socket = None
        self.client_socket = None
        self.running = False
        self.capturing = False

        # Update global frequency offset
        global FREQUENCY_OFFSET
        FREQUENCY_OFFSET = frequency_offset

        self.pluto = PlutoInterface(simulation_mode=not PLUTO_AVAILABLE, pluto_uri=pluto_uri)

        # Configure logging
        log_format = '%(asctime)s - %(levelname)s - %(message)s'
        if verbose == 0:
            logging.basicConfig(level=logging.ERROR, format=log_format)
        elif verbose == 1:
            logging.basicConfig(level=logging.INFO, format=log_format)
        else:
            logging.basicConfig(level=logging.DEBUG, format=log_format)

        self.logger = logging.getLogger(__name__)

        # FIFO (block capture) mode support
        # In FIFO mode, SpectraVue requests discrete blocks of fifo_count*4096
        # samples. Each block is captured independently (no phase continuity),
        # sent to SpectraVue, then the next block is captured. This is used for
        # higher-rate spectrum display without demodulation.
        self.fifo_mode = False
        self.fifo_complete = False  # Track if current FIFO block is done

        # Command debouncing - coalesces rapid frequency/gain changes
        # so only the most recent value is applied after a quiet period
        self._pending_freq = None        # Most recent pending frequency (Hz)
        self._pending_gain = None        # Most recent pending gain (dB)
        self._last_command_time = 0.0    # Time of last freq/gain command
        self._debounce_lock = threading.Lock()
        self._debounce_event = threading.Event()
        self._debounce_interval = 0.100  # 100ms quiet period before applying
        self._debounce_thread = None

        # CloudSDR device information
        self.device_info = {
            'name': 'CloudSDR',
            'serial': 'PLUTO001',
            'interface_version': 2,
            'firmware_version': 8,
            'hardware_version': 100,
            'fpga_config': (1, 2, 9, 'PLUTOFPGA'),
            'product_id': bytes([0x43, 0x4C, 0x53, 0x44]),
            'options': (0x20, 0x00, b'\x00\x00\x00\x00'),
        }

        # Receiver settings
        self.receiver_settings = {
            'frequency': 100000000,   # 100 MHz
            'sample_rate': 2048000,   # 2.048 MHz
            'rf_gain': 30,
            'rf_filter': 0,
            'ad_modes': 3,
            'state': 0x01,
            'data_type': 0x80,
            'capture_mode': 0x80,
            'fifo_count': 0,
            'channel_mode': 0,
            'packet_size': 0,
            'udp_ip': None,
            'udp_port': None,
        }

        # CloudSDR Control Item codes
        self.CI_CODES = {
            0x0001: 'CI_GENERAL_INTERFACE_NAME',
            0x0002: 'CI_GENERAL_INTERFACE_SERIALNUM',
            0x0003: 'CI_GENERAL_INTERFACE_VERSION',
            0x0004: 'CI_GENERAL_HW_FW_VERSIONS',
            0x0005: 'CI_GENERAL_STATUS_ERROR_CODE',
            0x0009: 'CI_GENERAL_PRODUCT_ID',
            0x000A: 'CI_GENERAL_OPTIONS',
            0x000B: 'CI_GENERAL_SECURITY_CODE',
            0x000C: 'CI_GENERAL_FPGA_CONFIG',
            0x0018: 'CI_RX_STATE',
            0x0020: 'CI_RX_FREQUENCY',
            0x0030: 'CI_RX_RF_INPUT_PORT_SELECT',
            0x0032: 'CI_RX_RF_INPUT_PORT_RANGE',
            0x0038: 'CI_RX_RF_GAIN',
            0x0044: 'CI_RX_RF_FILTER',
            0x008A: 'CI_RX_AD_MODES',
            0x00B0: 'CI_RX_AD_INPUT_SAMPLE_RATE_CAL',
            0x00B8: 'CI_RX_OUT_SAMPLE_RATE',
            0x00C4: 'CI_RX_DATA_OUTPUT_PACKET_SIZE',
            0x00C5: 'CI_RX_DATA_OUTPUT_UDP_IP_PORT',
            0x00D0: 'CI_RX_DC_CALIBRATION_DATA',
            0x0302: 'CI_UPDATE_MODE_PARAMETERS',
        }

    def start(self):
        """Start the CloudSDR emulator server."""
        self.running = True

        print("ADALM-PLUTO CloudSDR Bridge")
        print("=" * 50)

        if not self.pluto.initialize():
            self.logger.error("Failed to initialize PLUTO")
            return

        self.pluto.configure(
            self.receiver_settings['sample_rate'],
            self.receiver_settings['frequency'],
            self.receiver_settings['rf_gain'],
            self.verbose
        )

        # Start debounce thread for coalescing rapid frequency/gain changes
        self._debounce_thread = threading.Thread(
            target=self._debounce_worker,
            daemon=True,
            name="CommandDebounce"
        )
        self._debounce_thread.start()

        # Start TCP server
        self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.tcp_socket.bind((self.host, self.port))
        self.tcp_socket.listen(1)

        print(f"Listening on {self.host}:{self.port}")
        print("Contiguous streaming + FIFO block capture")
        print("Command debouncing enabled")
        print("Ready for SpectraVue connection...")
        print("=" * 50)
        print()

        try:
            while self.running:
                client_socket, client_addr = self.tcp_socket.accept()
                self.logger.info(f"SpectraVue connected from {client_addr}")
                self.client_socket = client_socket

                client_thread = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket,)
                )
                client_thread.daemon = True
                client_thread.start()
                client_thread.join()
                self.logger.info(f"SpectraVue {client_addr} disconnected")

        except KeyboardInterrupt:
            self.logger.info("Shutting down...")
        finally:
            self.stop()

    def stop(self):
        """Stop the emulator."""
        self.running = False
        self.capturing = False

        # Wake debounce thread so it can exit
        self._debounce_event.set()

        if self.pluto:
            self.pluto.stop_streaming()

        if self.client_socket:
            self.client_socket.close()
        if self.tcp_socket:
            self.tcp_socket.close()
        if self.udp_socket:
            self.udp_socket.close()

    def _debounce_worker(self):
        """Worker thread that coalesces rapid frequency/gain changes.

        When SpectraVue sends rapid frequency changes (e.g. scroll wheel),
        this thread waits for commands to stop arriving, then applies only
        the most recent values in a single configure() call. This prevents
        the Pluto from sequentially processing 20+ tune/IQ-balance cycles.
        """
        while self.running:
            # Wait until something is pending
            self._debounce_event.wait(timeout=0.5)
            if not self.running:
                break

            with self._debounce_lock:
                if self._pending_freq is None and self._pending_gain is None:
                    self._debounce_event.clear()
                    continue

            # Wait for the quiet period to elapse (no new commands)
            while self.running:
                with self._debounce_lock:
                    elapsed = time.time() - self._last_command_time
                    pending_freq = self._pending_freq
                    pending_gain = self._pending_gain

                if elapsed >= self._debounce_interval:
                    break
                # Sleep for remaining debounce time
                time.sleep(max(0.005, self._debounce_interval - elapsed))

            if not self.running:
                break

            # Consume pending values
            with self._debounce_lock:
                freq_to_apply = self._pending_freq
                gain_to_apply = self._pending_gain
                self._pending_freq = None
                self._pending_gain = None
                self._debounce_event.clear()

            if freq_to_apply is None and gain_to_apply is None:
                continue

            # Use current settings for any value that wasn't pending
            apply_freq = freq_to_apply if freq_to_apply is not None else self.receiver_settings['frequency']
            apply_gain = gain_to_apply if gain_to_apply is not None else self.receiver_settings['rf_gain']

            if self.verbose >= 1:
                skipped = ""
                if freq_to_apply is not None:
                    skipped = f"Freq={apply_freq/1e6:.3f}MHz"
                if gain_to_apply is not None:
                    skipped += f"{' ' if skipped else ''}Gain={apply_gain}dB"
                self.logger.info(f"Debounce: applying coalesced config ({skipped})")

            self.pluto.configure(
                self.receiver_settings['sample_rate'],
                apply_freq,
                apply_gain
            )

    def handle_client(self, client_socket: socket.socket):
        """Handle CloudSDR protocol messages from client.

        CloudSDR protocol framing:
        - 2-byte header: bits[12:0] = total message length, bits[15:13] = message type
        - Message types: 0=SET, 1=REQUEST, 2=REQUEST_RANGE
        - Payload starts with 2-byte CI code followed by parameters
        """
        try:
            while self.running:
                header_data = client_socket.recv(2)
                if not header_data:
                    break

                if len(header_data) < 2:
                    continue

                header = struct.unpack('<H', header_data)[0]
                length = header & 0x1FFF
                msg_type = (header >> 13) & 0x07

                remaining = length - 2
                if remaining > 0:
                    msg_data = client_socket.recv(remaining)
                    if len(msg_data) < remaining:
                        continue
                else:
                    msg_data = b''

                self.process_message(client_socket, msg_type, msg_data)

        except Exception as e:
            self.logger.error(f"Error handling client: {e}")
        finally:
            client_socket.close()

    def process_message(self, client_socket: socket.socket, msg_type: int, data: bytes):
        """Process CloudSDR control messages."""
        if len(data) < 2:
            self.send_nak(client_socket)
            return

        ci_code = struct.unpack('<H', data[:2])[0]
        params = data[2:] if len(data) > 2 else b''

        if msg_type == 0:  # SET
            self.handle_set_message(client_socket, ci_code, params)
        elif msg_type == 1:  # REQUEST
            self.handle_request_message(client_socket, ci_code, params)
        elif msg_type == 2:  # REQUEST_RANGE
            self.handle_request_range_message(client_socket, ci_code, params)
        else:
            self.send_nak(client_socket)

    def handle_set_message(self, client_socket: socket.socket, ci_code: int, params: bytes):
        """Handle SET control item messages."""
        response_data = struct.pack('<H', ci_code) + params

        if ci_code == 0x0020:  # CI_RX_FREQUENCY
            if len(params) >= 6:
                channel = params[0]
                if len(params) >= 9:
                    spectravue_freq = struct.unpack('<Q', params[1:9])[0]
                else:
                    spectravue_freq = struct.unpack('<Q', params[1:6] + b'\x00\x00\x00')[0]

                # Apply frequency offset to get actual PLUTO frequency
                actual_freq = spectravue_freq + self.frequency_offset

                # Validate PLUTO frequency range (70 MHz - 6 GHz)
                if actual_freq < 70e6:
                    self.logger.warning(f"Final frequency {actual_freq/1e6:.3f} MHz below PLUTO range (70 MHz minimum)")
                    actual_freq = max(actual_freq, 70e6)
                elif actual_freq > 6e9:
                    self.logger.warning(f"Final frequency {actual_freq/1e6:.3f} MHz above PLUTO range (6 GHz maximum)")
                    actual_freq = min(actual_freq, 6e9)

                old_freq = self.receiver_settings['frequency']
                self.receiver_settings['frequency'] = actual_freq

                if abs(actual_freq - old_freq) > 1000:
                    # Queue for debounced application instead of immediate configure.
                    # This coalesces rapid scroll-wheel frequency changes so only
                    # the final value is sent to the Pluto hardware.
                    with self._debounce_lock:
                        self._pending_freq = actual_freq
                        self._last_command_time = time.time()
                    self._debounce_event.set()

                if self.verbose >= 1:
                    if self.frequency_offset > 0:
                        self.logger.info(f"Frequency: SpectraVue {spectravue_freq/1e6:.3f} MHz + Offset {self.frequency_offset/1e6:.0f} MHz = PLUTO {actual_freq/1e6:.3f} MHz")
                    else:
                        self.logger.info(f"PLUTO Frequency: {actual_freq/1e6:.6f} MHz")

        elif ci_code == 0x00B8:  # CI_RX_OUT_SAMPLE_RATE
            if len(params) >= 5:
                channel = params[0]
                rate = struct.unpack('<I', params[1:5])[0]

                old_rate = self.receiver_settings['sample_rate']
                self.receiver_settings['sample_rate'] = rate

                if abs(rate - old_rate) > 1000:
                    self.pluto.configure(
                        rate,
                        self.receiver_settings['frequency'],
                        self.receiver_settings['rf_gain'],
                        self.verbose
                    )

                if self.verbose >= 1:
                    self.logger.info(f"PLUTO Sample rate: {rate/1e6:.6f} MHz")

        elif ci_code == 0x0038:  # CI_RX_RF_GAIN
            if len(params) >= 2:
                channel = params[0]
                spectravue_gain = struct.unpack('<b', params[1:2])[0]

                # Map SpectraVue gain to PLUTO hardware gain
                if spectravue_gain in PLUTO_GAIN_MAP:
                    pluto_gain = PLUTO_GAIN_MAP[spectravue_gain]
                    gain_desc = f"SpectraVue {spectravue_gain}dB -> PLUTO {pluto_gain}dB"
                else:
                    pluto_gain = DEFAULT_PLUTO_GAIN
                    gain_desc = f"SpectraVue {spectravue_gain}dB (unknown) -> PLUTO {pluto_gain}dB (default)"
                    self.logger.warning(f"Unknown SpectraVue gain value: {spectravue_gain}dB, using default PLUTO gain: {pluto_gain}dB")

                old_gain = self.receiver_settings['rf_gain']
                self.receiver_settings['rf_gain'] = pluto_gain

                if abs(pluto_gain - old_gain) > 0.5:
                    with self._debounce_lock:
                        self._pending_gain = pluto_gain
                        self._last_command_time = time.time()
                    self._debounce_event.set()

                if self.verbose >= 1:
                    self.logger.info(f"Gain Control: {gain_desc}")

        elif ci_code == 0x0018:  # CI_RX_STATE
            if len(params) >= 4:
                data_type = params[0]
                run_stop = params[1]
                capture_mode = params[2]
                fifo_count = params[3]

                self.receiver_settings['state'] = run_stop
                self.receiver_settings['data_type'] = data_type
                self.receiver_settings['capture_mode'] = capture_mode
                self.receiver_settings['fifo_count'] = fifo_count

                # Decode mode fields
                is_complex = (data_type & 0x80) != 0
                is_24bit = (capture_mode & 0x80) != 0
                is_fifo = (capture_mode & 0x03) == 0x01

                if self.verbose >= 1:
                    state_str = "RUN" if run_stop == 0x02 else "IDLE"
                    bit_str = "24-bit" if is_24bit else "16-bit"
                    mode_str = "Complex I/Q" if is_complex else "Real A/D"
                    capture_str = "FIFO" if is_fifo else "Contiguous"
                    if is_fifo and run_stop == 0x02:
                        block_samples = fifo_count * 4096
                        self.logger.info(f"Receiver State: {state_str}, {mode_str}, {bit_str}, {capture_str} ({block_samples} samples/block)")
                    else:
                        self.logger.info(f"Receiver State: {state_str}, {mode_str}, {bit_str}, {capture_str}")

                if run_stop == 0x02:
                    self.fifo_complete = False
                    if is_fifo:
                        self.start_fifo_capture(is_complex, is_24bit, fifo_count)
                    else:
                        self.start_data_capture()
                else:
                    self.stop_data_capture()

        elif ci_code == 0x00C4:  # CI_RX_DATA_OUTPUT_PACKET_SIZE
            if len(params) >= 2:
                channel = params[0]
                packet_size = params[1]
                self.receiver_settings['packet_size'] = packet_size
                size_str = "Small" if packet_size == 1 else "Large"
                if self.verbose >= 1:
                    self.logger.info(f"Packet size: {size_str}")

        elif ci_code == 0x00C5:  # CI_RX_DATA_OUTPUT_UDP_IP_PORT
            if len(params) >= 6:
                ip_bytes = params[:4]
                port = struct.unpack('<H', params[4:6])[0]
                ip = ".".join(str(b) for b in ip_bytes)
                self.receiver_settings['udp_ip'] = ip
                self.receiver_settings['udp_port'] = port
                if self.verbose >= 1:
                    self.logger.info(f"UDP output: {ip}:{port}")

        self.send_response(client_socket, response_data)

    def handle_request_message(self, client_socket: socket.socket, ci_code: int, params: bytes):
        """Handle REQUEST control item messages."""

        response_data = None

        if ci_code == 0x0001:  # CI_GENERAL_INTERFACE_NAME
            name = self.device_info['name'].encode('ascii') + b'\x00'
            response_data = struct.pack('<H', ci_code) + name

        elif ci_code == 0x0002:  # CI_GENERAL_INTERFACE_SERIALNUM
            serial = self.device_info['serial'].encode('ascii') + b'\x00'
            response_data = struct.pack('<H', ci_code) + serial

        elif ci_code == 0x0003:  # CI_GENERAL_INTERFACE_VERSION
            version = struct.pack('<H', self.device_info['interface_version'])
            response_data = struct.pack('<H', ci_code) + version

        elif ci_code == 0x0004:  # CI_GENERAL_HW_FW_VERSIONS
            if len(params) >= 1:
                fw_id = params[0]
                if fw_id in [0, 1]:
                    version = struct.pack('<BH', fw_id, self.device_info['firmware_version'])
                elif fw_id == 2:
                    version = struct.pack('<BH', fw_id, self.device_info['hardware_version'])
                elif fw_id == 3:
                    config_id, config_ver = self.device_info['fpga_config'][:2]
                    version = struct.pack('<BBB', fw_id, config_id, config_ver)
                else:
                    self.send_nak(client_socket)
                    return
                response_data = struct.pack('<H', ci_code) + version

        elif ci_code == 0x0005:  # CI_GENERAL_STATUS_ERROR_CODE
            # Report BUSY while capturing (contiguous or FIFO)
            if self.receiver_settings['state'] == 0x02:
                status = 0x0C  # BUSY
            else:
                status = 0x0B  # IDLE
            response_data = struct.pack('<HB', ci_code, status)

        elif ci_code == 0x0009:  # CI_GENERAL_PRODUCT_ID
            response_data = struct.pack('<H', ci_code) + self.device_info['product_id']

        elif ci_code == 0x000A:  # CI_GENERAL_OPTIONS
            opt1, opt2, opt_detail = self.device_info['options']
            response_data = struct.pack('<HBB', ci_code, opt1, opt2) + opt_detail

        elif ci_code == 0x000B:  # CI_GENERAL_SECURITY_CODE
            if len(params) >= 4:
                key = struct.unpack('<I', params[:4])[0]
                code = key ^ 0x12345678
                response_data = struct.pack('<HI', ci_code, code)

        elif ci_code == 0x000C:  # CI_GENERAL_FPGA_CONFIG
            config_num, config_id, config_rev, config_desc = self.device_info['fpga_config']
            desc = config_desc.encode('ascii') + b'\x00'
            response_data = struct.pack('<HBBB', ci_code, config_num, config_id, config_rev) + desc

        elif ci_code == 0x0020:  # CI_RX_FREQUENCY
            channel = params[0] if len(params) > 0 else 0
            actual_freq = self.receiver_settings['frequency']
            spectravue_freq = actual_freq - self.frequency_offset
            spectravue_freq = max(0, spectravue_freq)
            freq_bytes = struct.pack('<Q', int(spectravue_freq))
            response_data = struct.pack('<HB', ci_code, channel) + freq_bytes

        elif ci_code == 0x0038:  # CI_RX_RF_GAIN
            if len(params) >= 1:
                channel = params[0]
                current_pluto_gain = self.receiver_settings['rf_gain']

                # Find closest SpectraVue gain level
                closest_spectravue_gain = SPECTRAVUE_GAIN_0DB
                min_diff = float('inf')
                for sv_gain, pluto_gain in PLUTO_GAIN_MAP.items():
                    diff = abs(pluto_gain - current_pluto_gain)
                    if diff < min_diff:
                        min_diff = diff
                        closest_spectravue_gain = sv_gain

                response_data = struct.pack('<HBb', ci_code, channel, closest_spectravue_gain)

        elif ci_code == 0x00B0:  # CI_RX_AD_INPUT_SAMPLE_RATE_CAL
            if len(params) >= 1:
                channel = params[0]
                ad_cal_rate = 61440000  # 61.44 MHz
                response_data = struct.pack('<HBI', ci_code, channel, ad_cal_rate)

        elif ci_code == 0x00B8:  # CI_RX_OUT_SAMPLE_RATE
            if len(params) >= 1:
                channel = params[0]
                rate = self.receiver_settings['sample_rate']
                response_data = struct.pack('<HBI', ci_code, channel, rate)

        elif ci_code == 0x0030:  # CI_RX_RF_INPUT_PORT_SELECT
            if len(params) >= 1:
                channel = params[0]
                port_select = 0
                response_data = struct.pack('<HBB', ci_code, channel, port_select)

        elif ci_code == 0x0032:  # CI_RX_RF_INPUT_PORT_RANGE
            channel = params[0] if len(params) >= 1 else 0
            response_data = struct.pack('<HB', ci_code, channel) + b'\x00\x00\x00\x00\x00\x00\x00'

        elif ci_code == 0x00D0:  # CI_RX_DC_CALIBRATION_DATA
            if len(params) >= 1:
                channel = params[0]
                response_data = struct.pack('<HB', ci_code, channel) + b'\x00\x00'

        elif ci_code == 0x0302:  # CI_UPDATE_MODE_PARAMETERS
            if len(params) >= 1:
                device_id = params[0]
                flash_size = 256 * 1024
                page_size = 256
                sector_size = 16 * 1024
                response_data = struct.pack('<HBIII', ci_code, device_id, flash_size, page_size, sector_size)

        else:
            if self.verbose >= 1:
                self.logger.warning(f"Unsupported request: 0x{ci_code:04X}")
            self.send_nak(client_socket)
            return

        if response_data:
            self.send_response(client_socket, response_data)
        else:
            self.send_nak(client_socket)

    def handle_request_range_message(self, client_socket: socket.socket, ci_code: int, params: bytes):
        """Handle REQUEST_RANGE control item messages."""

        if ci_code == 0x0020:  # CI_RX_FREQUENCY range for PLUTO
            # PLUTO frequency range: 70 MHz to 6 GHz
            response_bytes = bytes([
                0x20, 0x00,  # CI code (0x0020)
                0x20,        # Channel 32 (0x20)
                0x01,        # 1 frequency range
                0x80, 0x1A, 0x2C, 0x04, 0x00,  # Min freq: 70 MHz (5 bytes)
                0x00, 0x00, 0x20, 0x65, 0x01, 0x00, 0x00, 0x00, # Max freq: 6 GHz (8 bytes)
                0x00, 0x00  # Padding (2 bytes)
            ])

            if self.verbose >= 1:
                self.logger.info(f"PLUTO frequency range: 70 MHz to 6 GHz")

            self.send_response(client_socket, response_bytes)

        else:
            if self.verbose >= 1:
                self.logger.warning(f"Range request not supported for CI 0x{ci_code:04X}")
            self.send_nak(client_socket)

    def send_response(self, client_socket: socket.socket, data: bytes):
        """Send response message to client using CloudSDR framing."""
        length = len(data) + 2

        if len(data) >= 2:
            ci_code = struct.unpack('<H', data[:2])[0]
            if ci_code == 0x0020 and len(data) > 10:  # Frequency range response
                type_field = 2
            else:
                type_field = 0
        else:
            type_field = 0

        header = length | (type_field << 13)
        message = struct.pack('<H', header) + data
        client_socket.send(message)

    def send_nak(self, client_socket: socket.socket):
        """Send NAK response to client."""
        length = 2
        type_field = 0
        header = length | (type_field << 13)
        message = struct.pack('<H', header)
        client_socket.send(message)

    def start_data_capture(self):
        """Start data capture and UDP streaming."""
        if not self.capturing:
            self.capturing = True
            self.logger.info("Starting PLUTO -> SpectraVue data streaming...")

            self.pluto.start_streaming()

            udp_thread = threading.Thread(target=self.udp_data_sender)
            udp_thread.daemon = True
            udp_thread.start()

    def stop_data_capture(self):
        """Stop data capture and UDP streaming."""
        if self.capturing:
            was_fifo = self.fifo_mode
            self.capturing = False
            self.fifo_mode = False
            if not was_fifo:
                # Only stop streaming infrastructure for contiguous mode;
                # FIFO mode doesn't use the streaming double-buffer.
                self.pluto.stop_streaming()
            self.logger.info(f"Stopped {'FIFO' if was_fifo else 'contiguous'} data capture")

    def start_fifo_capture(self, is_complex: bool, is_24bit: bool, fifo_count: int):
        """Start FIFO (block capture) mode.

        In FIFO mode, SpectraVue requests discrete blocks of samples for
        spectrum display without demodulation. Each block of fifo_count * 4096
        samples is captured independently from the Pluto, packetized, and sent
        via UDP. Blocks repeat continuously until SpectraVue sends STOP.

        Phase continuity between blocks is NOT required — SpectraVue only
        FFTs each block for spectrum display.
        """
        if not self.capturing:
            self.capturing = True
            self.fifo_mode = True
            block_samples = fifo_count * 4096

            self.logger.info(f"Starting FIFO capture: {'Complex I/Q' if is_complex else 'Real A/D'}, "
                           f"{'24-bit' if is_24bit else '16-bit'}, "
                           f"{block_samples} samples/block ({fifo_count} x 4096)")

            fifo_thread = threading.Thread(
                target=self.fifo_data_sender,
                args=(is_complex, is_24bit, block_samples),
                daemon=True,
                name="FIFOCapture"
            )
            fifo_thread.start()

    def fifo_data_sender(self, is_complex: bool, is_24bit: bool, total_samples: int):
        """Send FIFO blocks continuously until stopped.

        Each iteration:
        1. Captures total_samples from the Pluto in one shot
        2. Packetizes and sends them via UDP
        3. Repeats for the next block
        """
        try:
            # Create UDP socket
            udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024 * 1024)

            client_ip = self.client_socket.getpeername()[0] if self.client_socket else self.host
            if self.receiver_settings['udp_port']:
                udp_port = self.receiver_settings['udp_port']
            else:
                udp_port = self.port
            udp_addr = (client_ip, udp_port)

            # Determine packet format
            packet_size_setting = self.receiver_settings.get('packet_size', 0)

            if is_complex:
                bytes_per_sample = 6 if is_24bit else 4
            else:
                bytes_per_sample = 3 if is_24bit else 2

            # Scale from PLUTO raw ADC range (12-bit, ~-2048..+2047) to output range
            PLUTO_ADC_PEAK = 2048.0

            if is_24bit:
                if packet_size_setting == 1:  # Small
                    header_bytes = b'\x84\x81' if is_complex else b'\x84\x81'
                    samples_per_packet = 64
                else:  # Large
                    header_bytes = b'\xA4\x85' if is_complex else b'\xA4\x85'
                    samples_per_packet = 240
                scale_factor = (8388607.0 / PLUTO_ADC_PEAK) * 0.7
            else:  # 16-bit
                if packet_size_setting == 1:  # Small
                    header_bytes = b'\x04\x82'
                    samples_per_packet = 128 if is_complex else 256
                else:  # Large
                    header_bytes = b'\x04\x84'
                    samples_per_packet = 256 if is_complex else 512
                scale_factor = (32767.0 / PLUTO_ADC_PEAK) * 0.7

            packets_per_block = (total_samples + samples_per_packet - 1) // samples_per_packet
            data_size = samples_per_packet * bytes_per_sample

            self.logger.info(f"FIFO: {samples_per_packet} samples/packet, "
                           f"{packets_per_block} packets/block -> {udp_addr[0]}:{udp_addr[1]}")

            sequence = 1
            block_number = 0

            # Pre-allocate conversion buffers
            if is_24bit:
                real_int32 = np.empty(samples_per_packet, dtype=np.int32)
                imag_int32 = np.empty(samples_per_packet, dtype=np.int32)
            else:
                real_int16 = np.empty(samples_per_packet, dtype=np.int16)
                imag_int16 = np.empty(samples_per_packet, dtype=np.int16)
                interleaved = np.empty(samples_per_packet * 2, dtype=np.int16)

            data_buffer = bytearray(data_size)

            while self.capturing and self.running and self.receiver_settings['state'] == 0x02:
                block_number += 1

                if self.verbose >= 2:
                    self.logger.debug(f"FIFO block #{block_number}: capturing {total_samples} samples...")

                # Capture one block from Pluto hardware
                block_data = self.pluto.get_iq_block(total_samples)

                if block_data is None:
                    self.logger.warning(f"FIFO block #{block_number}: capture returned None, retrying...")
                    time.sleep(0.01)
                    continue

                # Packetize and send the block
                samples_sent = 0
                for packet_num in range(packets_per_block):
                    if not self.capturing or self.receiver_settings['state'] != 0x02:
                        break

                    remaining = total_samples - samples_sent
                    current_samples = min(samples_per_packet, remaining)

                    # Extract this packet's worth of samples
                    chunk = block_data[samples_sent:samples_sent + current_samples]

                    # Pad if last packet is short
                    if len(chunk) < samples_per_packet:
                        chunk = np.pad(chunk, (0, samples_per_packet - len(chunk)),
                                      mode='constant', constant_values=0)

                    # Convert to packet format
                    if is_complex:
                        if is_24bit:
                            real_vals = np.real(chunk) * scale_factor
                            imag_vals = np.imag(chunk) * scale_factor
                            np.clip(real_vals, -8388608, 8388607, out=real_vals)
                            np.clip(imag_vals, -8388608, 8388607, out=imag_vals)
                            real_int32[:] = real_vals.astype(np.int32)
                            imag_int32[:] = imag_vals.astype(np.int32)

                            real_bytes = real_int32.astype('<i4').tobytes()
                            imag_bytes = imag_int32.astype('<i4').tobytes()

                            for i in range(samples_per_packet):
                                idx = i * 6
                                src = i * 4
                                data_buffer[idx:idx+3] = real_bytes[src:src+3]
                                data_buffer[idx+3:idx+6] = imag_bytes[src:src+3]
                        else:
                            real_vals = np.real(chunk) * scale_factor
                            imag_vals = np.imag(chunk) * scale_factor
                            np.clip(real_vals, -32768, 32767, out=real_vals)
                            np.clip(imag_vals, -32768, 32767, out=imag_vals)
                            real_int16[:] = real_vals.astype(np.int16)
                            imag_int16[:] = imag_vals.astype(np.int16)

                            interleaved[0::2] = real_int16
                            interleaved[1::2] = imag_int16
                            data_buffer[:] = interleaved.tobytes()
                    else:
                        # Real A/D mode — send only the real component
                        if is_24bit:
                            real_vals = np.real(chunk) * scale_factor
                            np.clip(real_vals, -8388608, 8388607, out=real_vals)
                            real_int32[:] = real_vals.astype(np.int32)
                            real_bytes = real_int32.astype('<i4').tobytes()

                            for i in range(samples_per_packet):
                                idx = i * 3
                                src = i * 4
                                data_buffer[idx:idx+3] = real_bytes[src:src+3]
                        else:
                            real_vals = np.real(chunk) * scale_factor
                            np.clip(real_vals, -32768, 32767, out=real_vals)
                            real_int16[:current_samples] = real_vals[:current_samples].astype(np.int16)
                            data_buffer[:current_samples*2] = real_int16[:current_samples].tobytes()

                    # Build and send packet
                    header = header_bytes + struct.pack('<H', sequence)
                    packet = header + bytes(data_buffer)
                    udp_sock.sendto(packet, udp_addr)

                    samples_sent += current_samples
                    sequence = (sequence + 1) % 65536
                    if sequence == 0:
                        sequence = 1

                self.fifo_complete = True

                if self.verbose >= 1 and block_number <= 5:
                    self.logger.info(f"FIFO block #{block_number} sent: {samples_sent} samples")
                elif self.verbose >= 1 and block_number % 100 == 0:
                    self.logger.info(f"FIFO blocks sent: {block_number}")

            if self.verbose >= 1:
                self.logger.info(f"FIFO capture stopped after {block_number} blocks")

        except Exception as e:
            self.logger.error(f"FIFO sender error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            udp_sock.close()

    def udp_data_sender(self):
        """Send PLUTO IQ data to SpectraVue via UDP (contiguous mode)."""
        try:
            # Create UDP socket
            self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024*1024)

            client_ip = self.client_socket.getpeername()[0] if self.client_socket else self.host

            if self.receiver_settings['udp_port']:
                udp_port = self.receiver_settings['udp_port']
            else:
                udp_port = self.port

            udp_addr = (client_ip, udp_port)

            sequence = 0
            packet_count = 0

            # Get packet configuration
            data_type = self.receiver_settings.get('data_type', 0x80)
            capture_mode = self.receiver_settings.get('capture_mode', 0x80)
            sample_rate = self.receiver_settings['sample_rate']
            packet_size_setting = self.receiver_settings['packet_size']

            is_24bit = bool(capture_mode & 0x80)

            # Determine packet parameters (matching B210 format)
            #
            # Scale factor: pyadi-iio returns raw ADC values from the AD9363.
            # The IIO driver sign-extends to 16-bit (-32768..+32767) but the
            # 12-bit ADC only uses about -2048..+2047 at most signal levels.
            # We scale these raw values to fill ~70% of the output integer range.
            #   24-bit output max: 8388607  -> scale = 8388607/2048 * 0.7 ~ 2867
            #   16-bit output max: 32767    -> scale = 32767/2048 * 0.7  ~ 11.2
            # This keeps strong signals within range while preserving dynamic range.
            PLUTO_ADC_PEAK = 2048.0  # 12-bit ADC peak value

            if is_24bit:
                if packet_size_setting == 1:  # Small packets
                    header_bytes = b'\x84\x81'
                    samples_per_packet = 64
                    data_size = 384
                else:  # Large packets
                    header_bytes = b'\xA4\x85'
                    samples_per_packet = 240
                    data_size = 1440
                scale_factor = (8388607.0 / PLUTO_ADC_PEAK) * 0.7
            else:  # 16-bit
                if packet_size_setting == 1:  # Small packets
                    header_bytes = b'\x04\x82'
                    samples_per_packet = 128
                    data_size = 512
                else:  # Large packets
                    header_bytes = b'\x04\x84'
                    samples_per_packet = 256
                    data_size = 1024
                scale_factor = (32767.0 / PLUTO_ADC_PEAK) * 0.7

            # Pre-allocate buffers
            data_buffer = bytearray(data_size)
            zero_samples = np.zeros(samples_per_packet, dtype=np.complex64)

            # Calculate precise timing
            packets_per_second = sample_rate / samples_per_packet
            packet_interval = 1.0 / packets_per_second

            mode_str = "24-bit" if is_24bit else "16-bit"
            size_str = "Large" if packet_size_setting == 0 else "Small"

            self.logger.info(f"UDP: {samples_per_packet} samples/packet, {packets_per_second:.1f} pps, {packet_interval*1000:.3f}ms interval")
            self.logger.info(f"Streaming PLUTO IQ -> SpectraVue at {udp_addr[0]}:{udp_addr[1]} ({mode_str} {size_str})")

            # Timing: we schedule each packet relative to a wall-clock anchor.
            # If the thread gets stalled (CPU load, GC, etc.) we detect when
            # we've fallen behind by more than max_behind and re-anchor rather
            # than trying to burst-catch-up, which would cause rate spikes.
            start_time = time.perf_counter()
            next_packet_time = start_time
            max_behind = packet_interval * 8  # Allow up to 8 packets of drift

            # Statistics
            samples_sent = 0
            last_stats_time = start_time
            rate_samples = deque(maxlen=20)

            # Pre-allocate arrays for 24-bit processing
            if is_24bit:
                real_vals_24 = np.empty(samples_per_packet, dtype=np.float32)
                imag_vals_24 = np.empty(samples_per_packet, dtype=np.float32)
                real_int32 = np.empty(samples_per_packet, dtype=np.int32)
                imag_int32 = np.empty(samples_per_packet, dtype=np.int32)
            # Pre-allocate arrays for 16-bit processing
            if not is_24bit:
                real_vals = np.empty(samples_per_packet, dtype=np.float32)
                imag_vals = np.empty(samples_per_packet, dtype=np.float32)
                real_int = np.empty(samples_per_packet, dtype=np.int16)
                imag_int = np.empty(samples_per_packet, dtype=np.int16)
                interleaved = np.empty(samples_per_packet * 2, dtype=np.int16)

            while self.capturing and self.running:
                loop_start = time.perf_counter()

                # Precise timing
                time_until_next = next_packet_time - loop_start
                if time_until_next > 0:
                    if time_until_next > 0.001:
                        time.sleep(time_until_next * 0.9)
                    # Busy wait for final precision
                    while time.perf_counter() < next_packet_time:
                        pass

                # Get data from PLUTO
                iq_data = self.pluto.get_iq_data(samples_per_packet)
                if iq_data is None:
                    iq_data = zero_samples

                # Update sequence
                sequence = (sequence + 1) % 65536
                if sequence == 0:
                    sequence = 1

                # Create packet header
                header = header_bytes + struct.pack('<H', sequence)

                # Convert samples to packet format
                if is_24bit:
                    np.multiply(np.real(iq_data), scale_factor, out=real_vals_24)
                    np.multiply(np.imag(iq_data), scale_factor, out=imag_vals_24)
                    np.clip(real_vals_24, -8388608, 8388607, out=real_vals_24)
                    np.clip(imag_vals_24, -8388608, 8388607, out=imag_vals_24)
                    np.round(real_vals_24, out=real_vals_24)
                    np.round(imag_vals_24, out=imag_vals_24)
                    real_int32[:] = real_vals_24.astype(np.int32)
                    imag_int32[:] = imag_vals_24.astype(np.int32)

                    real_bytes = real_int32.astype('<i4').tobytes()
                    imag_bytes = imag_int32.astype('<i4').tobytes()

                    for i in range(len(iq_data)):
                        idx = i * 6
                        real_start = i * 4
                        data_buffer[idx:idx+3] = real_bytes[real_start:real_start+3]
                        data_buffer[idx+3:idx+6] = imag_bytes[real_start:real_start+3]
                else:
                    np.multiply(np.real(iq_data), scale_factor, out=real_vals)
                    np.multiply(np.imag(iq_data), scale_factor, out=imag_vals)
                    np.clip(real_vals, -32768, 32767, out=real_vals)
                    np.clip(imag_vals, -32768, 32767, out=imag_vals)
                    np.round(real_vals, out=real_vals)
                    np.round(imag_vals, out=imag_vals)
                    real_int[:] = real_vals.astype(np.int16)
                    imag_int[:] = imag_vals.astype(np.int16)

                    interleaved[0::2] = real_int
                    interleaved[1::2] = imag_int
                    data_buffer[:] = interleaved.tobytes()

                # Send packet with header
                packet = header + data_buffer

                try:
                    self.udp_socket.sendto(packet, udp_addr)
                    packet_count += 1
                    samples_sent += samples_per_packet

                    if packet_count <= 3 and self.verbose >= 1:
                        self.logger.info(f"PLUTO -> SpectraVue: packet #{packet_count}")

                    # Rate monitoring
                    if packet_count % 3000 == 0:
                        current_time = time.perf_counter()
                        elapsed = current_time - last_stats_time
                        if elapsed > 0:
                            actual_sample_rate = samples_sent / elapsed
                            rate_samples.append(actual_sample_rate)
                            smoothed_rate = np.mean(rate_samples)
                            rate_error = abs(smoothed_rate - sample_rate) / sample_rate * 100

                            if self.verbose >= 1:
                                self.logger.info(f"Rate (smoothed): {smoothed_rate:.0f} Hz "
                                               f"(target: {sample_rate:.0f} Hz, error: {rate_error:.2f}%)")

                        samples_sent = 0
                        last_stats_time = current_time

                except Exception as e:
                    self.logger.error(f"Error sending UDP data: {e}")
                    break

                # Advance to next packet time
                next_packet_time += packet_interval

                # If we've fallen too far behind wall clock (CPU stall, GC pause,
                # etc.), re-anchor timing to now instead of burst-catching-up.
                # A brief gap is better than a rate spike.
                now = time.perf_counter()
                if now - next_packet_time > max_behind:
                    skipped = int((now - next_packet_time) / packet_interval)
                    next_packet_time = now
                    if self.verbose >= 1:
                        self.logger.info(f"Timing: re-anchored (skipped {skipped} packet slots)")

        except Exception as e:
            self.logger.error(f"UDP sender error: {e}")
        finally:
            if self.udp_socket:
                self.udp_socket.close()
                self.udp_socket = None


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='ADALM-PLUTO CloudSDR Bridge - Connect PLUTO to SpectraVue',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('-v', '--verbose', type=int, default=1, choices=[0,1,2,3],
                      help='Verbosity level: 0=quiet, 1=normal, 2=debug, 3=trace')
    parser.add_argument('-p', '--port', type=int, default=50000,
                      help='TCP port to listen on')
    parser.add_argument('--pluto-uri', type=str, default="ip:192.168.2.1",
                      help='PLUTO URI (e.g., "usb:" for auto-detect, "ip:192.168.2.1" for network)')
    parser.add_argument('--offset', type=float, default=0.0,
                      help='Frequency offset in Hz (access PLUTO full range beyond SpectraVue 2GHz limit)')
    parser.add_argument('--host', type=str, default="localhost",
                      help='Host address to bind to')
    args = parser.parse_args()

    print("=" * 60)
    print("ADALM-PLUTO CloudSDR Bridge v2.0")
    print("ADALM-PLUTO -> CloudSDR Protocol -> SpectraVue")
    print(f"Server: {args.host}:{args.port}")
    print(f"PLUTO: {args.pluto_uri}")
    print("Contiguous streaming + FIFO block capture")
    print("Command debouncing enabled")
    print(f"Supported sample rates: 2.048, 1.2288, 0.6144 MHz")

    # Show frequency offset configuration
    if args.offset > 0:
        print(f"Frequency Offset: +{args.offset/1e6:.0f} MHz")
        print(f"   SpectraVue 0-2 GHz -> PLUTO {args.offset/1e6:.0f}-{(args.offset + 2e9)/1e6:.0f} MHz")
        max_sv_freq = min(2000, (6e9 - args.offset)/1e6)
        if max_sv_freq < 2000:
            print(f"   SpectraVue range limited to 0-{max_sv_freq:.0f} MHz (PLUTO 6 GHz limit)")
    else:
        print("Frequency Offset: None (normal 0-2 GHz operation)")

    print("=" * 60)

    if not PLUTO_AVAILABLE:
        print("pyadi-iio not found - running in simulation mode")
        print("   Install: pip install pyadi-iio")

    print()
    print("SpectraVue Gain Control Mapping:")
    print(f"   0dB  -> {PLUTO_GAIN_MAP[SPECTRAVUE_GAIN_0DB]}dB PLUTO (High sensitivity)")
    print(f"   -10dB -> {PLUTO_GAIN_MAP[SPECTRAVUE_GAIN_10DB]}dB PLUTO (Good sensitivity)")
    print(f"   -20dB -> {PLUTO_GAIN_MAP[SPECTRAVUE_GAIN_20DB]}dB PLUTO (Medium sensitivity)")
    print(f"   -30dB -> {PLUTO_GAIN_MAP[SPECTRAVUE_GAIN_30DB]}dB PLUTO (Low sensitivity)")
    print()

    bridge = CloudSDREmulator(host=args.host, port=args.port, verbose=args.verbose,
                             frequency_offset=args.offset, pluto_uri=args.pluto_uri)

    try:
        bridge.start()
    except KeyboardInterrupt:
        print("\nShutting down...")
        bridge.stop()


if __name__ == "__main__":
    main()
