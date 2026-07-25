# Kia Trip Logger

Self-hosted trip logging and analytics for Kia EV/hybrid vehicles, built on the
unofficial Kia/Hyundai Bluelink API. Polls your car's live location and
telemetry, detects trips automatically, stores everything in SQLite, and shows
it all on a map-based web dashboard — no cloud dependency, runs entirely on
your own hardware (tested on Raspberry Pi 4, arm64).

![Build status](https://github.com/chersoph/kia-tracker/actions/workflows/docker-publish.yml/badge.svg)
![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)

## Features

- Automatic trip detection from GPS + odometer data (configurable gap/movement thresholds)
- Adaptive polling — slower when parked, faster while driving, slower again overnight
- Reverse geocoding of trip start/end points (via Nominatim)
- Web dashboard with map view, per-day/per-trip breakdown
- Runs fully self-hosted in Docker — your data never leaves your own server
- Multi-architecture image: `amd64` and `arm64` (Raspberry Pi 4/5 with 64-bit OS)

## Disclaimer

This project uses an **unofficial** reverse-engineered Kia/Hyundai Bluelink API.
It is not affiliated with, endorsed by, or supported by Kia or Hyundai. Use at
your own risk — API changes on Kia's end may break functionality without
notice, and excessive polling could affect your account or vehicle battery.

## Quick start

**Prerequisites:** Docker and Docker Compose installed, a Kia Connect/UVO
account with your vehicle registered.

1. Create a working directory and download the example files:
   ```bash
   mkdir kia-trip-logger && cd kia-trip-logger
   curl -O https://raw.githubusercontent.com/chersoph/kia-tracker/main/sidecar/.env.example
   cp .env.example .env
   ```

2. Edit `.env` with your Kia account credentials and preferences:
   ```bash
   nano .env
   ```

3. Create a `docker-compose.yml`:
   ```yaml
   services:
     kia-tracker:
       image: ghcr.io/chersoph/kia-tracker:latest
       container_name: kia-tracker
       restart: unless-stopped
       env_file:
         - .env
       volumes:
         - ./data:/data
       ports:
         - "8130:8000"
   ```

4. Start it:
   ```bash
   docker compose up -d
   ```

5. Open `http://<your-server-ip>:8130` in a browser.

> **Important:** Keep the `./data` volume outside any directory you plan to
> put under version control — this holds your live SQLite database.

## Configuration

All settings are controlled via environment variables — see
[`sidecar/.env.example`](sidecar/.env.example) for the full list with
descriptions, covering:

- Kia account credentials and region/brand
- Polling intervals (parked, moving, night, highway)
- Trip detection thresholds (movement distance, odometer delta, time gap)
- Reverse geocoding options

## Building locally

```bash
git clone https://github.com/chersoph/kia-tracker.git
cd kia-tracker/sidecar
docker build -t kia-tracker .
```

## Contributing

Issues and pull requests are welcome. Please don't include real credentials,
database files, or personal vehicle data in any PR.

## License

[MIT](LICENSE)
