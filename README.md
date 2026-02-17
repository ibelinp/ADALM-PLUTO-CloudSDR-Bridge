# ADALM-PLUTO CloudSDR Bridge

A bridge that connects the [ADALM-PLUTO](https://www.analog.com/en/resources/evaluation-hardware-and-software/evaluation-boards-kits/adalm-pluto.html) SDR to [SpectraVue](http://www.moetronix.com/spectravue.htm) by emulating the [RFspace CloudSDR](https://www.rfspace.com/RFSPACE/CloudSDR.html) network protocol. SpectraVue sees the PLUTO as if it were a CloudSDR receiver.

## How It Works

```
ADALM-PLUTO  <--USB/Network-->  Bridge  <--TCP/UDP-->  SpectraVue
 (hardware)                   (this code)             (spectrum analyzer)
```

The bridge listens on a TCP port for CloudSDR protocol commands from SpectraVue (frequency, sample rate, gain, state control), translates them into PLUTO hardware calls via `pyadi-iio`, and streams IQ data back over UDP.

## Features

- **Contiguous streaming** — continuous IQ data flow for demodulation and spectrum display
- **FIFO block capture** — discrete blocks of IQ samples for non-continuous spectrum display modes (no demod)
- **Command debouncing** — rapid frequency changes (e.g. scroll wheel) are coalesced so only the final value is sent to hardware, avoiding long tune/IQ-balance cascades
- **Frequency offset** — access the full PLUTO tuning range (70 MHz - 6 GHz) beyond SpectraVue's 2 GHz display limit
- **Gain mapping** — SpectraVue's gain steps are mapped to appropriate PLUTO hardware gain values
- **Adaptive master clock** — selects the optimal PLUTO master clock and decimation for exact CloudSDR sample rates
- **Simulation mode** — runs without hardware for development and testing (auto-enabled if `pyadi-iio` is not installed)

## Requirements

- Python 3.7+
- [pyadi-iio](https://github.com/analogdevicesinc/pyadi-iio) — `pip install pyadi-iio`
- numpy — `pip install numpy`
- [SpectraVue](http://www.moetronix.com/spectravue.htm) v3.44 or later
- ADALM-PLUTO with USB or network connectivity

## Usage

```
python ADALM-PLUTO-CloudSDR-Bridge.py [options]
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `-v`, `--verbose` | 1 | Verbosity: 0=quiet, 1=normal, 2=debug, 3=trace |
| `-p`, `--port` | 50000 | TCP port to listen on |
| `--host` | localhost | Host address to bind to |
| `--pluto-uri` | ip:192.168.2.1 | PLUTO URI (`ip:192.168.2.1` for network, `usb:` for auto-detect) |
| `--offset` | 0 | Frequency offset in Hz |

### Examples

Basic usage (PLUTO at default IP):
```
python ADALM-PLUTO-CloudSDR-Bridge.py
```

PLUTO over USB:
```
python ADALM-PLUTO-CloudSDR-Bridge.py --pluto-uri usb:
```

Access 2-4 GHz range (SpectraVue shows 0-2 GHz, PLUTO tunes 2-4 GHz):
```
python ADALM-PLUTO-CloudSDR-Bridge.py --offset 2e9
```

Access 2.4 GHz ISM band (SpectraVue 0 MHz = PLUTO 2.4 GHz):
```
python ADALM-PLUTO-CloudSDR-Bridge.py --offset 2.4e9
```

## SpectraVue Setup

1. Start the bridge
2. Open SpectraVue
3. Go to **Settings > Radio** and select **CloudSDR** as the radio type
4. Set the IP address to `127.0.0.1` and port to `50000`
5. Click Connect

### Supported SpectraVue Modes with ADALM-PLUTO

The Continuous IQ modes in SpectraVue are the ones that work with the PLUTO bridge for streaming demodulation and spectrum display:

| SpectraVue Mode | Sample Rate | Description |
|-----------------|------------|-------------|
| 500k Continuous IQ | 614.4 kHz | Narrowband — good for SSB, AM, CW demod |
| 1M Continuous IQ | 1.2288 MHz | Medium bandwidth — FM broadcast, digital modes |
| 2M Continuous IQ | 2.048 MHz | Widest continuous mode — full CloudSDR bandwidth |

These modes provide continuous, phase-coherent IQ data suitable for demodulation. SpectraVue handles all demod (AM, SSB, FM, etc.) and spectrum display.

FIFO block capture modes are also supported. In FIFO mode, SpectraVue requests a block of samples at a time for spectrum-only display without demodulation. The IQ data is not continuous between blocks.

## SpectraVue Gain Mapping

SpectraVue's gain control sends discrete levels that are mapped to PLUTO hardware gain (0-73 dB range):

| SpectraVue | PLUTO | Use Case |
|-----------|-------|----------|
| 0 dB | 60 dB | Weak signals |
| -10 dB | 45 dB | General use |
| -20 dB | 30 dB | Average signals |
| -30 dB | 15 dB | Strong signals |

These mappings can be adjusted by editing the `PLUTO_GAIN_MAP` dictionary in the source.

## Frequency Offset

SpectraVue can only display 0-2 GHz. The PLUTO can tune 70 MHz - 6 GHz (with firmware mod). The `--offset` option adds a fixed offset to every frequency command from SpectraVue:

```
Actual PLUTO frequency = SpectraVue frequency + offset
```

The bridge validates that the resulting frequency stays within the PLUTO's range and clamps if necessary.

## Architecture

The bridge has two main components:

**PlutoInterface** — Handles hardware communication:
- Numpy ring buffer pipeline (primary buffer absorbs USB bursts, secondary buffer provides smooth delivery) for contiguous streaming
- Single-shot block capture for FIFO mode
- Dynamic RX buffer sizing scaled to sample rate
- Adaptive master clock selection for exact CloudSDR sample rates

**CloudSDREmulator** — Handles the network protocol:
- TCP server for CloudSDR control messages (SET, REQUEST, REQUEST_RANGE)
- UDP sender for IQ data packets (16-bit or 24-bit, small or large packets)
- Command debouncing thread to coalesce rapid frequency/gain changes
- FIFO capture thread for block-mode operation

## Supported Sample Rates

| Rate | Master Clock | Decimation |
|------|-------------|------------|
| 2.048 MHz | 61.44 MHz | 30 |
| 1.2288 MHz | 61.44 MHz | 50 |
| 614.4 kHz | 61.44 MHz | 100 |

## Capture Modes

| Mode | capture_mode bits [1:0] | Description |
|------|------------------------|-------------|
| Contiguous | 0x00 | Continuous IQ streaming via double-buffered ring buffer pipeline |
| FIFO | 0x01 | Block capture of `fifo_count * 4096` samples, repeated |

In FIFO mode, each block is captured independently — there is no phase continuity between blocks. SpectraVue uses this for spectrum-only display without demodulation.

## Related Files

| File | Description |
|------|-------------|
| `ADALM-PLUTO-CloudSDR-Bridge.py` | PLUTO bridge (this project) |
| `USRP-B210-CloudSDR-Bridge.py` | Equivalent bridge for USRP B210 hardware |
| `CloudSDR-Radio-Emulator.py` | Standalone CloudSDR emulator (no hardware) |
| `CloudSDR-Radio-Emulator_with_FIFO_modes.py` | Emulator with FIFO mode support |

## Protocol Reference

The CloudSDR protocol is based on the RFspace NetSDR interface. Key control items used:

| CI Code | Name | Purpose |
|---------|------|---------|
| 0x0018 | CI_RX_STATE | Start/stop, data type, capture mode |
| 0x0020 | CI_RX_FREQUENCY | Center frequency |
| 0x0038 | CI_RX_RF_GAIN | RF gain level |
| 0x00B8 | CI_RX_OUT_SAMPLE_RATE | Output sample rate |
| 0x00C4 | CI_RX_DATA_OUTPUT_PACKET_SIZE | UDP packet size |
| 0x00C5 | CI_RX_DATA_OUTPUT_UDP_IP_PORT | UDP destination |

See the [NetSDR Interface Specification](https://www.moetronix.com/files/NetSdrInterfaceSpec105.pdf) for full protocol details.

## License

MIT License
