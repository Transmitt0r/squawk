# Feeder

Dockerised ADS-B feeder stack running on the Raspberry Pi (`tracker@flighttracker.local`).

## Structure

```
feeder/
  docker-compose.yml  ← readsb + tar1090 + fr24feed
  README.md
```

## Services

| Service    | Image                                       | Role |
|------------|---------------------------------------------|------|
| `readsb`   | `sdr-enthusiasts/docker-readsb-protobuf`    | ADS-B decoder, talks to RTL-SDR dongle |
| `tar1090`  | `sdr-enthusiasts/docker-tar1090`            | Web UI + aircraft JSON API at `/data/aircraft.json` |
| `fr24feed` | `sdr-enthusiasts/docker-flightradar24`      | FlightRadar24 feeder |

## Key facts

- RTL-SDR dongle is passed through via `devices: [/dev/bus/usb:/dev/bus/usb]`
- tar1090 serves at `http://flighttracker.local/data/aircraft.json` (no `/tar1090/` prefix)
- `dns: [127.0.0.11, <router-ip>]` is required for inter-container name resolution on this Pi
- Coordinates and FR24 key are set as env vars in Coolify (`READSB_LAT`, `READSB_LON`, `FR24KEY`) — never commit these
- Deployed via Coolify with the Pi configured as a remote server, compose file path `feeder/docker-compose.yml`
