#!/usr/bin/env python3
"""
USRP B210 CloudSDR Bridge v1.0
Connects USRP B210 to SpectraVue via CloudSDR protocol emulation

This bridge allows SpectraVue to connect to a USRP B210 as if it were a CloudSDR device.
Features professional double-buffering, adaptive master clock selection, and complete
SpectraVue compatibility including gain controls and frequency offset support.

Author: Integration based on Pieter Ibelings' CloudSDR Emulator
License: MIT License
Version: 1.0
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

# Import UHD Python bindings
try:
    import uhd
    UHD_AVAILABLE = True
    print("✅ UHD Python bindings found - full B210 functionality available")
except ImportError:
    UHD_AVAILABLE = False
    print("⚠️  UHD Python bindings not found - running in simulation mode")

# =============================================================================
# FREQUENCY OFFSET CONFIGURATION
# =============================================================================
"""
SpectraVue limitation: Can only tune 0-2 GHz
B210 capability: 70 MHz - 6 GHz

Use --offset to access B210's full range beyond SpectraVue's 2 GHz limit:
  --offset 0        → Normal operation (0-2 GHz)
  --offset 2e9      → Access 2-4 GHz (SpectraVue 0-2GHz maps to B210 2-4GHz)  
  --offset 4e9      → Access 4-6 GHz (SpectraVue 0-2GHz maps to B210 4-6GHz)

Example: --offset 2.4e9
  SpectraVue shows 100 MHz → B210 actually tunes to 2.5 GHz

Safety: Validates final frequency is within B210 range (70 MHz - 6 GHz)
"""

# Global frequency offset (set via command line --offset)
FREQUENCY_OFFSET = 0.0  # Hz - Added to all SpectraVue frequencies

# =============================================================================
# SPECTRAVUE TO B210 GAIN MAPPING CONFIGURATION
# =============================================================================
"""
SpectraVue sends gain commands via CI_RX_RF_GAIN (0x0038) using these values:
- 0dB   (0x00) - Maximum sensitivity
- -10dB (0xF6) - High sensitivity  
- -20dB (0xEC) - Medium sensitivity
- -30dB (0xE2) - Low sensitivity

These are mapped to optimal B210 hardware gain values (0-76dB range):
- B210 gain selection balances sensitivity vs noise performance
- Values chosen based on Ettus recommendations and practical RF usage
- All gain settings are safe for B210 hardware
"""

# SpectraVue gain levels (signed 8-bit values)
SPECTRAVUE_GAIN_0DB = 0      # 0x00
SPECTRAVUE_GAIN_10DB = -10   # 0xF6  
SPECTRAVUE_GAIN_20DB = -20   # 0xEC
SPECTRAVUE_GAIN_30DB = -30   # 0xE2

# B210 hardware gain mappings (0-76dB range)
B210_GAIN_MAP = {
    SPECTRAVUE_GAIN_0DB:  60,   # High sensitivity for weak signals
    SPECTRAVUE_GAIN_10DB: 45,   # Good sensitivity, reduced noise
    SPECTRAVUE_GAIN_20DB: 30,   # Medium sensitivity for average signals  
    SPECTRAVUE_GAIN_30DB: 15,   # Low sensitivity for strong signals
}

# Default B210 gain if SpectraVue sends unexpected value
DEFAULT_B210_GAIN = 45  # Safe middle-ground value

# =============================================================================

class B210Interface:
    """
    Interface to USRP B210 hardware with professional streaming architecture.
    
    Implements double-buffering to absorb USB burst transfers and provide
    smooth sample delivery to UDP clients. Supports exact CloudSDR sample rates
    with adaptive master clock selection.
    """
    
    # Available B210 master clocks (adaptive selection for optimal decimation)
    MASTER_CLOCKS = [
        61.44e6,    # Primary CloudSDR clock
        30.72e6,    # Half clock (for high decimation rates)
        15.36e6,    # Quarter clock  
        7.68e6,     # Eighth clock
    ]
    
    # CloudSDR standard rates - now achievable with adaptive master clock
    # Format: rate_hz: (preferred_master_clock, decimation)
    CLOUDSDR_EXACT_RATES = {
        2048000:   (61.44e6, 30),     # 61.44 ÷ 30 = 2.048 MHz
        1228800:   (61.44e6, 50),     # 61.44 ÷ 50 = 1.2288 MHz  
        614400:    (61.44e6, 100),    # 61.44 ÷ 100 = 614.4 kHz
        495483:    (61.44e6, 124),    # 61.44 ÷ 124 = 495.483 kHz
        370120:    (61.44e6, 166),    # 61.44 ÷ 166 = 370.120 kHz
        245760:    (61.44e6, 250),    # 61.44 ÷ 250 = 245.76 kHz
        122880:    (61.44e6, 500),    # 61.44 ÷ 500 = 122.88 kHz
        64000:     (30.72e6, 480),    # 30.72 ÷ 480 = 64 kHz
        48000:     (15.36e6, 320),    # 15.36 ÷ 320 = 48 kHz
        34594:     (15.36e6, 444),    # 15.36 ÷ 444 = 34.594 kHz
        30720:     (15.36e6, 500),    # 15.36 ÷ 500 = 30.72 kHz
        16000:     (7.68e6, 480),     # 7.68 ÷ 480 = 16 kHz
    }
    
    def __init__(self, simulation_mode=False):
        """Initialize B210 interface with double-buffering architecture."""
        self.simulation_mode = simulation_mode
        self.sample_rate = 2.048e6
        self.center_freq = 100e6
        self.gain = 30
        self.running = False
        
        # UHD objects
        self.usrp = None
        self.rx_streamer = None
        
        # Double-buffering architecture
        self.buffer_lock = threading.Lock()
        
        # Primary buffer: Absorbs USB bursts from USRP
        self.primary_buffer = deque(maxlen=int(2.048e6 * 4.0))  # 4 seconds capacity
        
        # Secondary buffer: Provides smooth delivery to UDP
        self.secondary_buffer = deque(maxlen=int(2.048e6 * 1.0))  # 1 second capacity
        self.secondary_target = int(2.048e6 * 0.3)  # 300ms target depth
        
        # Buffer management
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
        """Get exact B210 configuration with adaptive master clock selection."""
        if requested_rate in self.CLOUDSDR_EXACT_RATES:
            master_clock, decimation = self.CLOUDSDR_EXACT_RATES[requested_rate]
            exact_rate = master_clock / decimation
            
            print(f"B210: Using master clock {master_clock/1e6:.2f} MHz, decimation {decimation}")
            return exact_rate, master_clock, True
        else:
            # Try to find optimal master clock for non-standard rate
            master_clock, decimation = self.find_optimal_master_clock(requested_rate)
            if master_clock is not None:
                exact_rate = master_clock / decimation
                print(f"B210: Custom rate using master clock {master_clock/1e6:.2f} MHz, decimation {decimation}")
                return exact_rate, master_clock, False
            else:
                # Fallback to closest standard rate
                closest_rate = min(self.CLOUDSDR_EXACT_RATES.keys(), 
                                 key=lambda x: abs(x - requested_rate))
                master_clock, decimation = self.CLOUDSDR_EXACT_RATES[closest_rate]
                exact_rate = master_clock / decimation
                print(f"B210: Fallback to closest rate {exact_rate/1e6:.6f} MHz")
                return exact_rate, master_clock, False
        
    def initialize(self, device_args=""):
        """Initialize B210 hardware using UHD."""
        if self.simulation_mode or not UHD_AVAILABLE:
            print("B210: Running in simulation mode")
            return True
        
        try:
            print("B210: Initializing hardware...")
            
            if device_args:
                self.usrp = uhd.usrp.MultiUSRP(device_args)
            else:
                self.usrp = uhd.usrp.MultiUSRP()
            
            print(f"B210: Connected to {self.usrp.get_pp_string()}")
            print(f"B210: Motherboard: {self.usrp.get_mboard_name()}")
            print(f"B210: RX Subdev: {self.usrp.get_rx_subdev_name()}")
            
            print("✅ B210: Hardware initialized successfully")
            return True
            
        except Exception as e:
            print(f"❌ B210: Hardware initialization failed: {e}")
            print("B210: Falling back to simulation mode")
            self.simulation_mode = True
            return True
    
    def configure(self, requested_sample_rate, center_freq, gain, verbose=1):
        """Configure B210 with adaptive master clock for exact CloudSDR sample rates."""
        exact_rate, master_clock, is_exact = self.get_exact_rate_config(requested_sample_rate)
        
        if is_exact:
            print(f"B210: EXACT CloudSDR rate - Requested: {requested_sample_rate/1e6:.6f} MHz → Exact: {exact_rate/1e6:.6f} MHz")
        else:
            print(f"B210: Non-standard rate - Requested: {requested_sample_rate/1e6:.6f} MHz → Closest: {exact_rate/1e6:.6f} MHz")
        
        print(f"B210: Freq: {center_freq/1e6:.3f} MHz, Gain: {gain} dB")
        
        if self.simulation_mode:
            self.sample_rate = exact_rate
            self.center_freq = center_freq
            self.gain = gain
            self.current_master_clock = master_clock
            print("B210: Simulation configured")
            self._update_buffer_sizes()
            return True
        
        try:
            # Set adaptive master clock for optimal decimation
            self.usrp.set_master_clock_rate(master_clock)
            actual_master_clock = self.usrp.get_master_clock_rate()
            print(f"   Master clock: {actual_master_clock/1e6:.2f} MHz")
            
            if abs(actual_master_clock - master_clock) > 1000:
                print(f"⚠️  Master clock error: Got {actual_master_clock/1e6:.2f} MHz (expected {master_clock/1e6:.2f} MHz)")
            
            # Store current master clock
            self.current_master_clock = actual_master_clock
            
            # Configure RX channel A
            self.usrp.set_rx_subdev_spec(uhd.usrp.SubdevSpec("A:A"))
            
            # Set exact sample rate
            self.usrp.set_rx_rate(exact_rate, 0)
            actual_rate = self.usrp.get_rx_rate(0)
            
            rate_error = abs(actual_rate - exact_rate)
            if rate_error > 1.0:
                print(f"⚠️  Rate error: {rate_error:.1f} Hz")
            else:
                print(f"✅ EXACT rate achieved: {actual_rate:.6f} Hz")
            
            # Set frequency with LO offset to avoid center notch and spurs
            lo_offset = exact_rate * 0.6  # 0.6 × sample rate for optimal offset
            tune_req = uhd.types.TuneRequest(center_freq, lo_offset)
            self.usrp.set_rx_freq(tune_req, 0)
            actual_freq = self.usrp.get_rx_freq(0)
            
            if verbose >= 2:
                print(f"DEBUG: LO offset tuning - Target: {center_freq/1e6:.3f} MHz, LO offset: {lo_offset/1e6:.3f} MHz")
            
            self.usrp.set_rx_gain(gain, 0)
            actual_gain = self.usrp.get_rx_gain(0)
            
            # Store configuration
            self.sample_rate = actual_rate
            self.center_freq = actual_freq
            self.gain = actual_gain
            
            self._update_buffer_sizes()
            
            print(f"✅ B210 configured:")
            print(f"   Frequency: {actual_freq/1e6:.6f} MHz")
            print(f"   Sample Rate: {actual_rate:.6f} Hz ({actual_rate/1e6:.6f} MHz)")
            print(f"   Gain: {actual_gain:.1f} dB")
            
            # Create optimized RX streamer
            st_args = uhd.usrp.StreamArgs("fc32", "sc16")
            st_args.channels = [0]
            st_args.args = uhd.types.DeviceAddr("num_recv_frames=128,recv_frame_size=8192")
            self.rx_streamer = self.usrp.get_rx_stream(st_args)
            
            print(f"   Max samples per packet: {self.rx_streamer.get_max_num_samps()}")
            
            return True
            
        except Exception as e:
            print(f"❌ B210: Configuration failed: {e}")
            return False
    
    def _update_buffer_sizes(self):
        """Update buffer sizes based on current sample rate."""
        with self.buffer_lock:
            primary_capacity = int(self.sample_rate * 4.0)  # 4 second burst absorption
            secondary_capacity = int(self.sample_rate * 1.0)  # 1 second smooth delivery
            self.secondary_target = int(self.sample_rate * 0.3)  # 300ms target
            
            self.primary_buffer = deque(maxlen=primary_capacity)
            self.secondary_buffer = deque(maxlen=secondary_capacity)
            
            print(f"   Primary Buffer: {primary_capacity/1e3:.0f}k samples ({primary_capacity/self.sample_rate:.1f}s)")
            print(f"   Secondary Buffer: {secondary_capacity/1e3:.0f}k samples ({secondary_capacity/self.sample_rate:.1f}s)")
            print(f"   Target Depth: {self.secondary_target/1e3:.0f}k samples ({self.secondary_target/self.sample_rate:.1f}s)")
    
    def start_streaming(self):
        """Start streaming with professional architecture."""
        if self.running:
            return
            
        self.running = True
        self.producer_running = True
        self.consumer_running = True
        
        print("B210: Starting streaming architecture...")
        
        # Reset statistics
        self.samples_written = 0
        self.samples_read = 0
        self.buffer_underruns = 0
        self.buffer_overruns = 0
        self.last_rate_check = time.time()
        
        # Start producer thread (USRP → Primary buffer)
        producer_thread = threading.Thread(target=self._usrp_producer_thread, daemon=True)
        producer_thread.start()
        
        # Start buffer manager (Primary → Secondary buffer)
        self.buffer_manager_thread = threading.Thread(target=self._buffer_manager_thread, daemon=True)
        self.buffer_manager_thread.start()
    
    def stop_streaming(self):
        """Stop streaming."""
        if not self.running:
            return
            
        self.running = False
        self.producer_running = False
        self.consumer_running = False
        print("B210: Stopping streaming...")
    
    def _usrp_producer_thread(self):
        """Producer thread entry point."""
        try:
            if self.simulation_mode:
                self._simulation_producer()
            else:
                self._hardware_producer()
        except Exception as e:
            print(f"B210: Producer error: {e}")
    
    def _buffer_manager_thread(self):
        """Buffer manager: Smooth transfer from primary to secondary buffer."""
        print("B210: Buffer manager started")
        
        target_rate = self.sample_rate
        transfer_interval = 0.005  # 5ms intervals
        # Scale transfer rate with sample rate for burst handling
        transfer_multiplier = 3.0 if target_rate > 1e6 else 2.0
        samples_per_transfer = int(target_rate * transfer_interval * transfer_multiplier)
        
        last_transfer_time = time.perf_counter()
        
        while self.consumer_running:
            try:
                current_time = time.perf_counter()
                
                if current_time - last_transfer_time >= transfer_interval:
                    with self.buffer_lock:
                        primary_size = len(self.primary_buffer)
                        secondary_size = len(self.secondary_buffer)
                        secondary_space = self.secondary_buffer.maxlen - secondary_size
                        
                        samples_to_transfer = min(
                            samples_per_transfer,
                            primary_size,
                            secondary_space
                        )
                        
                        if samples_to_transfer > 0:
                            transferred_samples = []
                            for _ in range(samples_to_transfer):
                                if self.primary_buffer:
                                    transferred_samples.append(self.primary_buffer.popleft())
                                else:
                                    break
                            
                            if transferred_samples:
                                self.secondary_buffer.extend(transferred_samples)
                    
                    last_transfer_time = current_time
                else:
                    sleep_time = transfer_interval - (current_time - last_transfer_time)
                    if sleep_time > 0:
                        time.sleep(sleep_time * 0.5)
                
            except Exception as e:
                print(f"B210: Buffer manager error: {e}")
                break
    
    def _simulation_producer(self):
        """Simulation producer with realistic USB burst behavior."""
        print("B210: Simulation producer started")
        
        carrier_freq = self.sample_rate * 0.2
        phase = 0.0
        burst_size = int(self.sample_rate * 0.05)  # 50ms bursts
        burst_interval = 0.03  # 30ms between bursts
        
        while self.producer_running:
            try:
                # Generate signal burst
                noise_power = 0.05
                noise = noise_power * (np.random.randn(burst_size) + 1j * np.random.randn(burst_size))
                
                phase_increment = 2 * np.pi * carrier_freq / self.sample_rate
                phases = phase + np.arange(burst_size) * phase_increment
                carrier = 0.2 * np.exp(1j * phases)
                phase = (phase + burst_size * phase_increment) % (2 * np.pi)
                
                signal = (noise + carrier).astype(np.complex64)
                
                with self.buffer_lock:
                    self.primary_buffer.extend(signal)
                    self.samples_written += len(signal)
                
                self.samples_generated += burst_size
                time.sleep(burst_interval)
                
            except Exception as e:
                print(f"B210: Simulation error: {e}")
                break
    
    def _hardware_producer(self):
        """Hardware producer: Absorb USRP USB bursts into primary buffer."""
        print("B210: Hardware producer started")
        
        try:
            if not self.rx_streamer:
                print("❌ B210: No RX streamer available")
                return
            
            max_samps = self.rx_streamer.get_max_num_samps()
            samples = np.zeros(max_samps, dtype=np.complex64)
            metadata = uhd.types.RXMetadata()
            
            # Start streaming
            stream_cmd = uhd.types.StreamCMD(uhd.types.StreamMode.start_cont)
            stream_cmd.stream_now = True
            self.rx_streamer.issue_stream_cmd(stream_cmd)
            
            print(f"B210: Producer ready - max {max_samps} samples/recv")
            
            # Performance monitoring
            last_perf_check = time.perf_counter()
            samples_in_period = 0
            large_recv_count = 0
            total_recv_calls = 0
            
            while self.producer_running:
                try:
                    num_rx_samps = self.rx_streamer.recv(samples, metadata, timeout=0.1)
                    total_recv_calls += 1
                    
                    if metadata.error_code != uhd.types.RXMetadataErrorCode.none:
                        if metadata.error_code == uhd.types.RXMetadataErrorCode.overflow:
                            pass  # Expected with burst absorption
                        else:
                            print(f"B210: RX error: {metadata.strerror()}")
                        continue
                    
                    if num_rx_samps > 0:
                        if num_rx_samps > 1000:
                            large_recv_count += 1
                        
                        received_samples = samples[:num_rx_samps]
                        samples_in_period += num_rx_samps
                        
                        with self.buffer_lock:
                            self.primary_buffer.extend(received_samples)
                            self.samples_written += num_rx_samps
                        
                        self.samples_generated += num_rx_samps
                    
                    # Lightweight monitoring
                    current_time = time.perf_counter()
                    if current_time - last_perf_check > 5.0:  # Every 5 seconds
                        elapsed = current_time - last_perf_check
                        actual_rate = samples_in_period / elapsed
                        rate_error = abs(actual_rate - self.sample_rate) / self.sample_rate * 100
                        
                        if rate_error > 5.0:  # Only log if there's an issue
                            print(f"⚠️ Producer: {actual_rate:.0f} Hz ({rate_error:.1f}% error)")
                        
                        last_perf_check = current_time
                        samples_in_period = 0
                        large_recv_count = 0
                        total_recv_calls = 0
                    
                except Exception as e:
                    print(f"B210: Receive error: {e}")
                    break
            
        except Exception as e:
            print(f"B210: Producer error: {e}")
        finally:
            if self.rx_streamer:
                try:
                    stream_cmd = uhd.types.StreamCMD(uhd.types.StreamMode.stop_cont)
                    self.rx_streamer.issue_stream_cmd(stream_cmd)
                except:
                    pass

    def get_iq_data(self, num_samples):
        """Get IQ data from smooth secondary buffer."""
        with self.buffer_lock:
            secondary_size = len(self.secondary_buffer)
            
            if secondary_size == 0:
                self.buffer_underruns += 1
                return None
            
            samples_to_get = min(num_samples, secondary_size)
            result = np.array([self.secondary_buffer.popleft() for _ in range(samples_to_get)], dtype=np.complex64)
            self.samples_read += samples_to_get
            
            return result if len(result) == num_samples else None


class CloudSDREmulator:
    """
    CloudSDR protocol emulator for USRP B210.
    
    Emulates a CloudSDR device, allowing SpectraVue and other CloudSDR-compatible
    software to connect to a USRP B210 transparently.
    """
    
    def __init__(self, host: str = "localhost", port: int = 50000, verbose: int = 1, frequency_offset: float = 0.0):
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
        
        self.b210 = B210Interface(simulation_mode=not UHD_AVAILABLE)
        
        # Configure logging
        log_format = '%(asctime)s - %(levelname)s - %(message)s'
        if verbose == 0:
            logging.basicConfig(level=logging.ERROR, format=log_format)
        elif verbose == 1:
            logging.basicConfig(level=logging.INFO, format=log_format)
        else:
            logging.basicConfig(level=logging.DEBUG, format=log_format)
        
        self.logger = logging.getLogger(__name__)
        
        # CloudSDR device information
        self.device_info = {
            'name': 'CloudSDR',
            'serial': 'B210001',
            'interface_version': 2,
            'firmware_version': 8,
            'hardware_version': 100,
            'fpga_config': (1, 2, 9, 'B210FPGA'),
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
        
        print("🚀 USRP B210 CloudSDR Bridge")
        print("=" * 50)
        
        if not self.b210.initialize():
            self.logger.error("Failed to initialize B210")
            return
        
        self.b210.configure(
            self.receiver_settings['sample_rate'],
            self.receiver_settings['frequency'],
            self.receiver_settings['rf_gain'],
            self.verbose
        )
        
        # Start TCP server
        self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.tcp_socket.bind((self.host, self.port))
        self.tcp_socket.listen(1)
        
        print(f"📡 Listening on {self.host}:{self.port}")
        print("✅ Professional streaming architecture")
        print("✅ CloudSDR protocol emulation")
        print("✅ SpectraVue gain control support")
        print("✅ Ready for SpectraVue connection...")
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
        
        if self.b210:
            self.b210.stop_streaming()
        
        if self.client_socket:
            self.client_socket.close()
        if self.tcp_socket:
            self.tcp_socket.close()
        if self.udp_socket:
            self.udp_socket.close()

    def handle_client(self, client_socket: socket.socket):
        """Handle CloudSDR protocol messages from client."""
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
                
                # Apply frequency offset to get actual B210 frequency
                actual_freq = spectravue_freq + self.frequency_offset
                
                # Validate B210 frequency range (70 MHz - 6 GHz)
                if actual_freq < 70e6:
                    self.logger.warning(f"Final frequency {actual_freq/1e6:.3f} MHz below B210 range (70 MHz minimum)")
                    actual_freq = max(actual_freq, 70e6)
                elif actual_freq > 6e9:
                    self.logger.warning(f"Final frequency {actual_freq/1e6:.3f} MHz above B210 range (6 GHz maximum)")
                    actual_freq = min(actual_freq, 6e9)
                
                old_freq = self.receiver_settings['frequency']
                self.receiver_settings['frequency'] = actual_freq
                
                if abs(actual_freq - old_freq) > 1000:
                    self.b210.configure(
                        self.receiver_settings['sample_rate'],
                        actual_freq,
                        self.receiver_settings['rf_gain']
                    )
                
                if self.verbose >= 1:
                    if self.frequency_offset > 0:
                        self.logger.info(f"🔄 Frequency: SpectraVue {spectravue_freq/1e6:.3f} MHz + Offset {self.frequency_offset/1e6:.0f} MHz = B210 {actual_freq/1e6:.3f} MHz")
                    else:
                        self.logger.info(f"B210 Frequency: {actual_freq/1e6:.6f} MHz")
                    
        elif ci_code == 0x00B8:  # CI_RX_OUT_SAMPLE_RATE
            if len(params) >= 5:
                channel = params[0]
                rate = struct.unpack('<I', params[1:5])[0]
                
                old_rate = self.receiver_settings['sample_rate']
                self.receiver_settings['sample_rate'] = rate
                
                if abs(rate - old_rate) > 1000:
                    self.b210.configure(
                        rate,
                        self.receiver_settings['frequency'],
                        self.receiver_settings['rf_gain'],
                        self.verbose
                    )
                
                if self.verbose >= 1:
                    self.logger.info(f"B210 Sample rate: {rate/1e6:.6f} MHz")
                    
        elif ci_code == 0x0038:  # CI_RX_RF_GAIN
            if len(params) >= 2:
                channel = params[0]  # Channel ID (ignored)
                spectravue_gain = struct.unpack('<b', params[1:2])[0]  # Signed 8-bit gain
                
                # Map SpectraVue gain to B210 hardware gain
                if spectravue_gain in B210_GAIN_MAP:
                    b210_gain = B210_GAIN_MAP[spectravue_gain]
                    gain_desc = f"SpectraVue {spectravue_gain}dB → B210 {b210_gain}dB"
                else:
                    b210_gain = DEFAULT_B210_GAIN
                    gain_desc = f"SpectraVue {spectravue_gain}dB (unknown) → B210 {b210_gain}dB (default)"
                    self.logger.warning(f"Unknown SpectraVue gain value: {spectravue_gain}dB, using default B210 gain: {b210_gain}dB")
                
                # Apply gain change to B210 if significant
                old_gain = self.receiver_settings['rf_gain']
                self.receiver_settings['rf_gain'] = b210_gain
                
                if abs(b210_gain - old_gain) > 0.5:  # Significant change
                    self.b210.configure(
                        self.receiver_settings['sample_rate'],
                        self.receiver_settings['frequency'],
                        b210_gain,
                        self.verbose
                    )
                
                if self.verbose >= 1:
                    self.logger.info(f"🎛️ Gain Control: {gain_desc}")
                    
        elif ci_code == 0x0018:  # CI_RX_STATE
            if len(params) >= 4:
                data_type = params[0]
                run_stop = params[1]
                capture_mode = params[2]
                fifo_count = params[3]
                
                self.receiver_settings['state'] = run_stop
                self.receiver_settings['data_type'] = data_type
                self.receiver_settings['capture_mode'] = capture_mode
                
                if self.verbose >= 1:
                    state_str = "RUN" if run_stop == 0x02 else "IDLE"
                    bit_str = "24-bit" if (capture_mode & 0x80) else "16-bit"
                    self.logger.info(f"Receiver State: {state_str} ({bit_str})")
                
                if run_stop == 0x02:
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
            status = 0x0C if self.capturing else 0x0B  # BUSY or IDLE
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
            # Return the SpectraVue frequency (actual frequency - offset)
            actual_freq = self.receiver_settings['frequency']
            spectravue_freq = actual_freq - self.frequency_offset
            
            # Ensure SpectraVue frequency is non-negative
            spectravue_freq = max(0, spectravue_freq)
            
            freq_bytes = struct.pack('<Q', int(spectravue_freq))
            response_data = struct.pack('<HB', ci_code, channel) + freq_bytes
            
        elif ci_code == 0x0038:  # CI_RX_RF_GAIN
            if len(params) >= 1:
                channel = params[0]
                # Return current B210 gain mapped back to SpectraVue format
                current_b210_gain = self.receiver_settings['rf_gain']
                
                # Find closest SpectraVue gain level
                closest_spectravue_gain = SPECTRAVUE_GAIN_0DB
                min_diff = float('inf')
                for sv_gain, b210_gain in B210_GAIN_MAP.items():
                    diff = abs(b210_gain - current_b210_gain)
                    if diff < min_diff:
                        min_diff = diff
                        closest_spectravue_gain = sv_gain
                
                response_data = struct.pack('<HBb', ci_code, channel, closest_spectravue_gain)
                
        elif ci_code == 0x00B0:  # CI_RX_AD_INPUT_SAMPLE_RATE_CAL
            if len(params) >= 1:
                channel = params[0]
                ad_cal_rate = 61440000  # 61.44 MHz for B210
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
        
        if ci_code == 0x0020:  # CI_RX_FREQUENCY range for B210
            # B210 frequency range: 70 MHz to 6 GHz
            response_bytes = bytes([
                0x20, 0x00,  # CI code (0x0020)
                0x20,        # Channel 32 (0x20)
                0x01,        # 1 frequency range
                0x80, 0x1A, 0x2C, 0x04, 0x00,  # Min freq: 70 MHz (5 bytes)
                0x00, 0x00, 0x20, 0x65, 0x01, 0x00, 0x00, 0x00, # Max freq: 6 GHz (8 bytes)
                0x00, 0x00  # Padding (2 bytes)
            ])
            
            if self.verbose >= 1:
                self.logger.info(f"B210 frequency range: 70 MHz to 6 GHz")
            
            self.send_response(client_socket, response_bytes)
            
        else:
            if self.verbose >= 1:
                self.logger.warning(f"Range request not supported for CI 0x{ci_code:04X}")
            self.send_nak(client_socket)

    def send_response(self, client_socket: socket.socket, data: bytes):
        """Send response message to client."""
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
            self.logger.info("Starting B210 → SpectraVue data streaming...")
            
            self.b210.start_streaming()
            
            udp_thread = threading.Thread(target=self.udp_data_sender)
            udp_thread.daemon = True
            udp_thread.start()

    def stop_data_capture(self):
        """Stop data capture and UDP streaming."""
        if self.capturing:
            self.capturing = False
            self.b210.stop_streaming()
            self.logger.info("Stopped B210 → SpectraVue data streaming")

    def udp_data_sender(self):
        """Send B210 IQ data to SpectraVue with precision timing."""
        try:
            # Create optimized UDP socket
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
            
            # Determine packet parameters
            if is_24bit:
                if packet_size_setting == 1:  # Small packets
                    header_bytes = b'\x84\x81'
                    samples_per_packet = 64
                    data_size = 384
                    scale_factor = 8388607 * 0.7
                else:  # Large packets
                    header_bytes = b'\xA4\x85'
                    samples_per_packet = 240
                    data_size = 1440
                    scale_factor = 8388607 * 0.7
            else:  # 16-bit
                if packet_size_setting == 1:  # Small packets
                    header_bytes = b'\x04\x82'
                    samples_per_packet = 128
                    data_size = 512
                    scale_factor = 32767 * 0.7
                else:  # Large packets
                    header_bytes = b'\x04\x84'
                    samples_per_packet = 256
                    data_size = 1024
                    scale_factor = 32767 * 0.7
            
            # Pre-allocate buffers
            data_buffer = bytearray(data_size)
            zero_samples = np.zeros(samples_per_packet, dtype=np.complex64)
            
            # Calculate precise timing
            packets_per_second = sample_rate / samples_per_packet
            packet_interval = 1.0 / packets_per_second
            
            mode_str = "24-bit" if is_24bit else "16-bit"
            size_str = "Large" if packet_size_setting == 0 else "Small"
            
            self.logger.info(f"UDP: {samples_per_packet} samples/packet, {packets_per_second:.1f} pps, {packet_interval*1000:.3f}ms interval")
            self.logger.info(f"Streaming B210 IQ → SpectraVue at {udp_addr[0]}:{udp_addr[1]} ({mode_str} {size_str})")
            
            # High-resolution timing
            start_time = time.perf_counter()
            next_packet_time = start_time
            
            # Timing correction
            timing_error_accumulator = 0.0
            timing_correction_factor = 1.0
            
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
                
                # Get data from B210
                iq_data = self.b210.get_iq_data(samples_per_packet)
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
                    # Ultra-fast vectorized 24-bit conversion
                    np.multiply(np.real(iq_data), scale_factor, out=real_vals_24)
                    np.multiply(np.imag(iq_data), scale_factor, out=imag_vals_24) 
                    np.clip(real_vals_24, -8388608, 8388607, out=real_vals_24)
                    np.clip(imag_vals_24, -8388608, 8388607, out=imag_vals_24)
                    np.round(real_vals_24, out=real_vals_24)
                    np.round(imag_vals_24, out=imag_vals_24)
                    real_int32[:] = real_vals_24.astype(np.int32)
                    imag_int32[:] = imag_vals_24.astype(np.int32)
                    
                    # Convert to bytes and extract 24-bit portions (vectorized)
                    real_bytes = real_int32.astype('<i4').tobytes()
                    imag_bytes = imag_int32.astype('<i4').tobytes()
                    
                    # Interleave 24-bit samples efficiently  
                    for i in range(len(iq_data)):
                        idx = i * 6
                        real_start = i * 4
                        data_buffer[idx:idx+3] = real_bytes[real_start:real_start+3]
                        data_buffer[idx+3:idx+6] = imag_bytes[real_start:real_start+3]
                else:
                    # 16-bit conversion
                    np.multiply(np.real(iq_data), scale_factor, out=real_vals)
                    np.multiply(np.imag(iq_data), scale_factor, out=imag_vals)
                    np.clip(real_vals, -32768, 32767, out=real_vals)
                    np.clip(imag_vals, -32768, 32767, out=imag_vals)
                    np.round(real_vals, out=real_vals)
                    np.round(imag_vals, out=imag_vals)
                    real_int[:] = real_vals.astype(np.int16)
                    imag_int[:] = imag_vals.astype(np.int16)
                    
                    # Interleave and convert to bytes
                    interleaved[0::2] = real_int
                    interleaved[1::2] = imag_int
                    data_buffer[:] = interleaved.tobytes()
                
                # Send packet
                packet = header + data_buffer
                
                try:
                    self.udp_socket.sendto(packet, udp_addr)
                    packet_count += 1
                    samples_sent += samples_per_packet
                    
                    # Minimal logging
                    if packet_count <= 3 and self.verbose >= 1:
                        self.logger.info(f"B210 → SpectraVue: packet #{packet_count}")
                    
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
                                self.logger.info(f"📊 Rate (smoothed): {smoothed_rate:.0f} Hz "
                                               f"(target: {sample_rate:.0f} Hz, error: {rate_error:.2f}%)")
                        
                        samples_sent = 0
                        last_stats_time = current_time
                        
                except Exception as e:
                    self.logger.error(f"Error sending UDP data: {e}")
                    break
                
                # Timing correction
                next_packet_time += packet_interval * timing_correction_factor
                
                # Adaptive timing correction
                if packet_count % 1000 == 0:
                    current_time = time.perf_counter()
                    
                    expected_time = start_time + (packet_count * packet_interval)
                    actual_time = current_time
                    timing_error = actual_time - expected_time
                    timing_error_accumulator += timing_error
                    
                    if abs(timing_error_accumulator) > 0.01:  # More than 10ms error
                        correction = -timing_error_accumulator / (packet_count * packet_interval)
                        timing_correction_factor = 1.0 + (correction * 0.1)
                        timing_correction_factor = max(0.95, min(1.05, timing_correction_factor))
                        
                        if self.verbose >= 2:
                            self.logger.debug(f"Timing correction: {timing_correction_factor:.6f}")
                        
                        timing_error_accumulator *= 0.5
                
        except Exception as e:
            self.logger.error(f"UDP sender error: {e}")
        finally:
            if self.udp_socket:
                self.udp_socket.close()
                self.udp_socket = None


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='USRP B210 CloudSDR Bridge - Connect USRP B210 to SpectraVue',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('-v', '--verbose', type=int, default=1, choices=[0,1,2,3],
                      help='Verbosity level: 0=quiet, 1=normal, 2=debug, 3=trace')
    parser.add_argument('-p', '--port', type=int, default=50000,
                      help='TCP port to listen on')
    parser.add_argument('-d', '--device', type=str, default="",
                      help='UHD device arguments (e.g., "serial=12345")')
    parser.add_argument('--offset', type=float, default=0.0,
                      help='Frequency offset in Hz (access B210 full range beyond SpectraVue 2GHz limit)')
    parser.add_argument('--host', type=str, default="localhost",
                      help='Host address to bind to')
    args = parser.parse_args()
    
    print("=" * 60)
    print("🚀 USRP B210 CloudSDR Bridge")
    print("📡 USRP B210 → CloudSDR Protocol → SpectraVue")
    print(f"🔗 Server: {args.host}:{args.port}")
    print("✅ Professional streaming architecture")
    print("✅ CloudSDR protocol emulation")
    print("✅ SpectraVue gain control support")
    print("✅ Supports standard CloudSDR sample rates")
    
    # Show frequency offset configuration
    if args.offset > 0:
        print(f"🔄 Frequency Offset: +{args.offset/1e6:.0f} MHz")
        print(f"   SpectraVue 0-2 GHz → B210 {args.offset/1e6:.0f}-{(args.offset + 2e9)/1e6:.0f} MHz")
        max_sv_freq = min(2000, (6e9 - args.offset)/1e6)
        if max_sv_freq < 2000:
            print(f"   ⚠️  SpectraVue range limited to 0-{max_sv_freq:.0f} MHz (B210 6 GHz limit)")
    else:
        print("🔄 Frequency Offset: None (normal 0-2 GHz operation)")
    
    print("=" * 60)
    
    if not UHD_AVAILABLE:
        print("⚠️  UHD not found - running in simulation mode")
        print("   Install: conda install -c conda-forge gnuradio")
    
    print()
    print("🎛️ SpectraVue Gain Control Mapping:")
    print(f"   0dB  → {B210_GAIN_MAP[SPECTRAVUE_GAIN_0DB]}dB B210 (High sensitivity)")
    print(f"   -10dB → {B210_GAIN_MAP[SPECTRAVUE_GAIN_10DB]}dB B210 (Good sensitivity)")  
    print(f"   -20dB → {B210_GAIN_MAP[SPECTRAVUE_GAIN_20DB]}dB B210 (Medium sensitivity)")
    print(f"   -30dB → {B210_GAIN_MAP[SPECTRAVUE_GAIN_30DB]}dB B210 (Low sensitivity)")
    print()
    
    bridge = CloudSDREmulator(host=args.host, port=args.port, verbose=args.verbose, frequency_offset=args.offset)
    
    try:
        bridge.start()
    except KeyboardInterrupt:
        print("\nShutting down...")
        bridge.stop()


if __name__ == "__main__":
    main()
