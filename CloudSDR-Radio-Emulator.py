#!/usr/bin/env python3
"""
CloudSDR Radio Emulator
A software emulator for CloudSDR hardware that enables SpectraVue 
to operate with full 0-2 GHz frequency range and UDP data streaming.

Author: Pieter Ibelings
License: MIT License

Copyright (c) 2025 Pieter Ibelings

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

Usage:
    # Quiet mode (errors only)
    python cloudsdr_emulator.py -v 0
    
    # Normal mode (important messages) - RECOMMENDED
    python cloudsdr_emulator.py -v 1
    
    # Debug mode (detailed protocol)
    python cloudsdr_emulator.py -v 2
    
    # Trace mode (everything including UDP details)
    python cloudsdr_emulator.py -v 3
"""

import socket
import struct
import threading
import time
import logging
import argparse
import math
import random
from typing import Dict, Any, Optional, Tuple

class CloudSDREmulator:
    def __init__(self, host: str = "localhost", port: int = 50000, verbose: int = 1):
        self.host = host
        self.port = port
        self.verbose = verbose
        self.tcp_socket = None
        self.udp_socket = None
        self.client_socket = None
        self.running = False
        self.capturing = False
        
        # Configure logging based on verbosity
        log_format = '%(asctime)s - %(levelname)s - %(message)s'
        if verbose == 0:
            logging.basicConfig(level=logging.ERROR, format=log_format)
        elif verbose == 1:
            logging.basicConfig(level=logging.INFO, format=log_format)
        else:  # verbose >= 2
            logging.basicConfig(level=logging.DEBUG, format=log_format)
        
        self.logger = logging.getLogger(__name__)
        
        # Device information for CloudSDR - MATCHED TO REAL HARDWARE
        self.device_info = {
            'name': 'CloudSDR',
            'serial': 'CSDR0001',
            'interface_version': 2,
            'firmware_version': 8,
            'hardware_version': 100,
            'fpga_config': (1, 2, 9, 'StdFPGA'),
            'product_id': bytes([0x43, 0x4C, 0x53, 0x44]),  # 'CLSD'
            'options': (0x20, 0x00, b'\x00\x00\x00\x00'),  # 0x20 matches real hardware
        }
        
        # Current receiver settings
        self.receiver_settings = {
            'frequency': 100000000,  # 100 MHz
            'sample_rate': 2048000,  # 2.048 MHz
            'rf_gain': 0,           # 0 dB
            'rf_filter': 0,         # Auto
            'ad_modes': 3,          # Dither on, A/D gain 1.5
            'state': 0x01,          # Idle
            'data_type': 0x80,      # Complex I/Q
            'capture_mode': 0x80,   # 24-bit contiguous
            'fifo_count': 0,
            'channel_mode': 0,
            'packet_size': 0,
            'udp_ip': None,
            'udp_port': None,
            'nco_phase': 0,
            'ad_scale': 0xFFFF,
            'dc_offset': 0,
            'vhf_uhf_gain': {'auto': 1, 'lna': 14, 'mixer': 8, 'if_output': 6},
            'pulse_mode': 0,
            'sync_mode': 0,
            'trigger_freq': 0,
            'trigger_phase': 0,
        }
        
        # Message type definitions
        self.MSG_TYPE_SET = 0x00
        self.MSG_TYPE_REQUEST = 0x20
        self.MSG_TYPE_REQUEST_RANGE = 0x40
        self.MSG_TYPE_RESPONSE = 0x00
        self.MSG_TYPE_UNSOLICITED = 0x20
        
        # Control Item codes
        self.CI_CODES = {
            0x0001: 'CI_GENERAL_INTERFACE_NAME',
            0x0002: 'CI_GENERAL_INTERFACE_SERIALNUM',
            0x0003: 'CI_GENERAL_INTERFACE_VERSION',
            0x0004: 'CI_GENERAL_HW_FW_VERSIONS',
            0x0005: 'CI_GENERAL_STATUS_ERROR_CODE',
            0x0008: 'CI_GENERAL_CUSTOM_NAME',
            0x0009: 'CI_GENERAL_PRODUCT_ID',
            0x000A: 'CI_GENERAL_OPTIONS',
            0x000B: 'CI_GENERAL_SECURITY_CODE',
            0x000C: 'CI_GENERAL_FPGA_CONFIG',
            0x0018: 'CI_RX_STATE',
            0x0019: 'CI_RX_CHANNEL_SETUP',
            0x0020: 'CI_RX_FREQUENCY',
            0x0022: 'CI_RX_NCO_PHASE_OFFSET',
            0x0023: 'CI_RX_AD_AMPLITUDE_SCALE',
            0x0030: 'CI_RX_RF_INPUT_PORT_SELECT',
            0x0032: 'CI_RX_RF_INPUT_PORT_RANGE',
            0x0038: 'CI_RX_RF_GAIN',
            0x003A: 'CI_RX_VHF_UHF_DOWN_CONVERTER_GAIN',
            0x0044: 'CI_RX_RF_FILTER',
            0x008A: 'CI_RX_AD_MODES',
            0x00B0: 'CI_RX_AD_INPUT_SAMPLE_RATE_CAL',
            0x00B2: 'CI_RX_INTERNAL_TRIGGER_FREQ',
            0x00B3: 'CI_RX_INTERNAL_TRIGGER_PHASE',
            0x00B4: 'CI_RX_INPUT_SYNC_MODES',
            0x00B6: 'CI_RX_PULSE_OUTPUT_MODES',
            0x00B8: 'CI_RX_OUT_SAMPLE_RATE',
            0x00C4: 'CI_RX_DATA_OUTPUT_PACKET_SIZE',
            0x00C5: 'CI_RX_DATA_OUTPUT_UDP_IP_PORT',
            0x00D0: 'CI_RX_DC_CALIBRATION_DATA',
            0x0118: 'CI_TX_STATE',
            0x0120: 'CI_TX_FREQUENCY',
            0x012A: 'CI_UNKNOWN_012A',
            0x0150: 'CI_RX_CW_STARTUP_MESSAGE',
            0x01B8: 'CI_TX_OUT_SAMPLE_RATE',
            0x0302: 'CI_UPDATE_MODE_PARAMETERS',
        }

    def start(self):
        """Start the CloudSDR emulator server"""
        self.running = True
        
        self.tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.tcp_socket.bind((self.host, self.port))
        self.tcp_socket.listen(1)
        
        self.logger.info(f"CloudSDR Emulator started on {self.host}:{self.port}")
        self.logger.info("Waiting for client connection...")
        
        try:
            while self.running:
                client_socket, client_addr = self.tcp_socket.accept()
                self.logger.info(f"Client connected from {client_addr}")
                self.client_socket = client_socket
                
                client_thread = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket,)
                )
                client_thread.daemon = True
                client_thread.start()
                client_thread.join()
                self.logger.info(f"Client {client_addr} disconnected")
                
        except KeyboardInterrupt:
            self.logger.info("Shutting down...")
        finally:
            self.stop()

    def stop(self):
        """Stop the emulator"""
        self.running = False
        self.capturing = False
        
        if self.client_socket:
            self.client_socket.close()
        if self.tcp_socket:
            self.tcp_socket.close()
        if self.udp_socket:
            self.udp_socket.close()

    def handle_client(self, client_socket: socket.socket):
        """Handle messages from connected client"""
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
        """Process incoming control messages"""
        if len(data) < 2:
            self.send_nak(client_socket)
            return
            
        ci_code = struct.unpack('<H', data[:2])[0]
        params = data[2:] if len(data) > 2 else b''
        
        ci_name = self.CI_CODES.get(ci_code, f"UNKNOWN_0x{ci_code:04X}")
        
        if self.verbose >= 2:
            msg_types = {0: "SET", 1: "REQUEST", 2: "REQUEST_RANGE"}
            msg_type_name = msg_types.get(msg_type, f"TYPE_{msg_type}")
            self.logger.debug(f"{msg_type_name} {ci_name} (0x{ci_code:04X})")
        
        if msg_type == 0:  # SET
            self.handle_set_message(client_socket, ci_code, params)
        elif msg_type == 1:  # REQUEST
            self.handle_request_message(client_socket, ci_code, params)
        elif msg_type == 2:  # REQUEST_RANGE
            self.handle_request_range_message(client_socket, ci_code, params)
        else:
            self.send_nak(client_socket)

    def handle_set_message(self, client_socket: socket.socket, ci_code: int, params: bytes):
        """Handle SET control item messages"""
        response_data = struct.pack('<H', ci_code) + params
        
        if ci_code == 0x0020:  # CI_RX_FREQUENCY
            if len(params) >= 6:
                channel = params[0]
                if len(params) >= 9:
                    freq = struct.unpack('<Q', params[1:9])[0]
                else:
                    freq = struct.unpack('<Q', params[1:6] + b'\x00\x00\x00')[0]
                self.receiver_settings['frequency'] = freq
                if self.verbose >= 1:
                    self.logger.info(f"Frequency set to {freq:,} Hz")
        elif ci_code == 0x00B8:  # CI_RX_OUT_SAMPLE_RATE
            if len(params) >= 5:
                channel = params[0]
                rate = struct.unpack('<I', params[1:5])[0]
                self.receiver_settings['sample_rate'] = rate
                if self.verbose >= 1:
                    self.logger.info(f"Sample rate set to {rate:,} Hz")
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
                    self.logger.info(f"Receiver State: {state_str}")
                
                if run_stop == 0x02:
                    self.start_data_capture()
                else:
                    self.stop_data_capture()
        elif ci_code == 0x00C5:  # CI_RX_DATA_OUTPUT_UDP_IP_PORT
            if len(params) >= 6:
                ip_bytes = params[:4]
                port = struct.unpack('<H', params[4:6])[0]
                ip = ".".join(str(b) for b in ip_bytes)
                self.receiver_settings['udp_ip'] = ip
                self.receiver_settings['udp_port'] = port
                if self.verbose >= 1:
                    self.logger.info(f"UDP output set to {ip}:{port}")
        
        self.send_response(client_socket, response_data)

    def handle_request_message(self, client_socket: socket.socket, ci_code: int, params: bytes):
        """Handle REQUEST control item messages"""
        
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
            freq = self.receiver_settings['frequency']
            freq_bytes = struct.pack('<Q', freq)
            response_data = struct.pack('<HB', ci_code, channel) + freq_bytes
            
        elif ci_code == 0x00B0:  # CI_RX_AD_INPUT_SAMPLE_RATE_CAL
            if len(params) >= 1:
                channel = params[0]
                ad_cal_rate = 122880000  # 122.88 MHz
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
        """Handle REQUEST_RANGE control item messages"""
        
        if self.verbose >= 1:
            self.logger.info(f"REQUEST_RANGE for CI 0x{ci_code:04X}")
        
        if ci_code == 0x0020:  # CI_RX_FREQUENCY range
            # CRITICAL: This is the new response per new Interface Spec. Min Freq = 5 bytes, Max Freq = 8 bytes plus 2 bytes padding
            
            # Build the exact response that real hardware sends (19 bytes of data)
            response_bytes = bytes([
                0x20, 0x00,  # CI code (0x0020)
                0x20,        # Channel 32 (0x20) - CRITICAL!
                0x01,        # 1 frequency range
                0x00, 0x00, 0x00, 0x00, 0x00,  # Min freq: 0 Hz (5 bytes)
                0x00, 0x94, 0x35, 0x77, 0x00, 0x00, 0x00, 0x00, # Max freq: 2000000000 Hz = 2 GHz (8 bytes)
                0x00, 0x00  # Padding (2 bytes)
            ])
            
            if self.verbose >= 1:
                self.logger.info(f"Frequency range: 0 Hz to 2 GHz (8-byte format)")
            
            self.send_response(client_socket, response_bytes)
            
        else:
            if self.verbose >= 1:
                self.logger.warning(f"Range request not supported for CI 0x{ci_code:04X}")
            self.send_nak(client_socket)

    def send_response(self, client_socket: socket.socket, data: bytes):
        """Send response message to client"""
        length = len(data) + 2
        
        # Check if this is a frequency range response - needs special type field
        if len(data) >= 2:
            ci_code = struct.unpack('<H', data[:2])[0]
            if ci_code == 0x0020 and len(data) > 10:  # Frequency range response
                type_field = 2  # Special type for range responses!
            else:
                type_field = self.MSG_TYPE_RESPONSE
        else:
            type_field = self.MSG_TYPE_RESPONSE
            
        header = length | (type_field << 13)
        message = struct.pack('<H', header) + data
        client_socket.send(message)

    def send_nak(self, client_socket: socket.socket):
        """Send NAK response"""
        length = 2
        type_field = self.MSG_TYPE_RESPONSE
        header = length | (type_field << 13)
        message = struct.pack('<H', header)
        client_socket.send(message)

    def start_data_capture(self):
        """Start UDP data capture simulation"""
        if not self.capturing:
            self.capturing = True
            self.logger.info("Starting UDP data capture...")
            
            # Start UDP data thread
            udp_thread = threading.Thread(target=self.udp_data_sender)
            udp_thread.daemon = True
            udp_thread.start()

    def stop_data_capture(self):
        """Stop UDP data capture"""
        if self.capturing:
            self.capturing = False
            self.logger.info("Stopping UDP data capture...")

    def udp_data_sender(self):
        """Send simulated UDP data packets with noise and carrier"""
        try:
            # Create UDP socket
            self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            
            # Get client IP
            client_ip = self.client_socket.getpeername()[0] if self.client_socket else self.host
            
            # Use configured UDP port or default to same port as TCP
            if self.receiver_settings['udp_port']:
                udp_port = self.receiver_settings['udp_port']
            else:
                udp_port = self.port  # CloudSDR uses same port for UDP as TCP
            
            udp_addr = (client_ip, udp_port)
            self.logger.info(f"Sending UDP data to {udp_addr[0]}:{udp_addr[1]}")
            
            sequence = 0
            packet_count = 0
            start_time = time.time()
            
            while self.capturing and self.running:
                # Get current receiver configuration
                data_type = self.receiver_settings.get('data_type', 0x80)
                capture_mode = self.receiver_settings.get('capture_mode', 0x80)
                sample_rate = self.receiver_settings['sample_rate']
                packet_size_setting = self.receiver_settings['packet_size']
                
                # Determine if this is complex I/Q or real data
                is_complex = bool(data_type & 0x80)
                is_24bit = bool(capture_mode & 0x80)
                
                # Determine packet size and header based on format
                if is_24bit:
                    if packet_size_setting == 1:  # Small packets
                        header_bytes = b'\x84\x81'  # 24-bit small packet
                        samples_per_packet = 64
                        data_size = 384  # 64 samples * 6 bytes/sample
                    else:  # Large packets
                        header_bytes = b'\xA4\x85'  # 24-bit large packet
                        samples_per_packet = 240
                        data_size = 1440  # 240 samples * 6 bytes/sample
                    bytes_per_sample = 6
                else:  # 16-bit
                    if packet_size_setting == 1:  # Small packets
                        header_bytes = b'\x04\x82'  # 16-bit small packet
                        samples_per_packet = 128
                        data_size = 512  # 128 samples * 4 bytes/sample
                    else:  # Large packets
                        header_bytes = b'\x04\x84'  # 16-bit large packet
                        samples_per_packet = 256
                        data_size = 1024  # 256 samples * 4 bytes/sample
                    bytes_per_sample = 4
                
                # Create packet header
                header = header_bytes + struct.pack('<H', sequence)
                
                # Generate I/Q data with carrier and noise
                data = bytearray(data_size)
                
                freq = self.receiver_settings['frequency']
                time_offset = packet_count * samples_per_packet / sample_rate
                
                # Simple carrier frequency at 20% of sample rate
                carrier_freq = sample_rate * 0.2
                
                for i in range(samples_per_packet):
                    t = time_offset + i / sample_rate
                    
                    if is_24bit:
                        amplitude = 1000000
                        noise_level = 10000
                        max_val = 8388607
                        min_val = -8388608
                    else:
                        amplitude = 16000
                        noise_level = 500
                        max_val = 32767
                        min_val = -32768
                    
                    # Generate I and Q components with carrier and noise
                    i_val = int(amplitude * math.cos(2 * math.pi * carrier_freq * t))
                    q_val = int(amplitude * math.sin(2 * math.pi * carrier_freq * t))
                    
                    # Add noise
                    i_val += random.randint(-noise_level, noise_level)
                    q_val += random.randint(-noise_level, noise_level)
                    
                    # Clamp values
                    i_val = max(min_val, min(max_val, i_val))
                    q_val = max(min_val, min(max_val, q_val))
                    
                    # Pack data
                    sample_idx = i * bytes_per_sample
                    
                    if is_24bit:
                        i_bytes = struct.pack('<i', i_val)[:3]
                        q_bytes = struct.pack('<i', q_val)[:3]
                        data[sample_idx:sample_idx+3] = i_bytes
                        data[sample_idx+3:sample_idx+6] = q_bytes
                    else:
                        struct.pack_into('<hh', data, sample_idx, i_val, q_val)
                
                packet = header + data
                
                try:
                    bytes_sent = self.udp_socket.sendto(packet, udp_addr)
                    packet_count += 1
                    
                    # Log first few packets
                    if packet_count <= 3 and self.verbose >= 1:
                        self.logger.info(f"UDP packet #{packet_count}: {bytes_sent} bytes sent")
                    
                    # Log occasionally
                    if packet_count % 100 == 0 and self.verbose >= 2:
                        mode_str = f"24-bit" if is_24bit else "16-bit"
                        size_str = "Large" if packet_size_setting == 0 else "Small"
                        self.logger.debug(f"UDP: {packet_count} packets sent ({mode_str} {size_str})")
                        
                except Exception as e:
                    self.logger.error(f"Error sending UDP data: {e}")
                    break
                
                # Update sequence number
                sequence = (sequence + 1) % 65536
                if sequence == 0:
                    sequence = 1  # Skip 0 after wrap
                
                # Timing control
                packet_rate = sample_rate / samples_per_packet
                packet_interval = 1.0 / packet_rate
                
                # Precise timing
                expected_time = packet_count * packet_interval
                current_time = time.time()
                
                if packet_count > 1:
                    elapsed_time = current_time - start_time
                    sleep_time = expected_time - elapsed_time
                    if sleep_time > 0:
                        time.sleep(sleep_time)
                else:
                    time.sleep(packet_interval)
                
        except Exception as e:
            self.logger.error(f"UDP sender error: {e}")
        finally:
            if self.udp_socket:
                self.udp_socket.close()
                self.udp_socket = None


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='CloudSDR Radio Emulator by Pieter Ibelings')
    parser.add_argument('-v', '--verbose', type=int, default=1, choices=[0,1,2,3],
                      help='Verbosity level: 0=quiet, 1=normal, 2=debug, 3=trace')
    parser.add_argument('-p', '--port', type=int, default=50000,
                      help='TCP port to listen on (default: 50000)')
    args = parser.parse_args()
    
    print("CloudSDR Radio Emulator")
    print("Author: Pieter Ibelings | License: MIT")
    print(f"Listening on localhost:{args.port}")
    print("✅ Frequency range: 0-2 GHz")
    print("✅ UDP data: Noise + carrier generation")
    print("✅ Protocol: Real hardware responses")
    print()
    
    emulator = CloudSDREmulator(port=args.port, verbose=args.verbose)
    
    try:
        emulator.start()
    except KeyboardInterrupt:
        print("\nShutting down...")
        emulator.stop()


if __name__ == "__main__":
    main()