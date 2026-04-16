# Feeder

Docker Compose stack for the Raspberry Pi ADS-B feeder, deployed via Coolify
with the Pi configured as a remote server.

## Services

| Service | Image | Role |
|---------|-------|------|
| `readsb` | `sdr-enthusiasts/docker-readsb-protobuf` | ADS-B decoder, talks to RTL-SDR dongle |
| `tar1090` | `sdr-enthusiasts/docker-tar1090` | Web UI + aircraft JSON API at `:8080` |
| `fr24feed` | `sdr-enthusiasts/docker-flightradar24` | FlightRadar24 feeder |

## Environment variables

Set these in Coolify:

| Variable | Description |
|----------|-------------|
| `READSB_LAT` | Receiver latitude |
| `READSB_LON` | Receiver longitude |
| `FR24KEY` | FlightRadar24 sharing key |

## Deploy

Deploy as a Docker Compose application in Coolify, pointed at the Pi as a remote server,
with compose file path set to `feeder/docker-compose.yml`.

tar1090 web UI: `http://flighttracker.local:8080`
