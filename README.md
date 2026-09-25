# Smart Trash Bin – Assignment 1

Edge node firmware for a 3-compartment smart trash bin, built on 
Raspberry Pi Pico W in MicroPython. Simulated in Wokwi with 
telemetry published to a HiveMQ Cloud MQTT broker.

## Files
- `main.py` – Firmware for the Pi Pico W

## Features
- Ultrasonic fill-level detection with moving-average filter
- MQTT telemetry with smart publish trigger
- 30-second heartbeat on dedicated topic
- Command deduplication and expiry validation
- Bin-full alert with hysteresis (90% trigger, 85% reset)
- Per-type pending buffer for network outage resilience

## Team
- Shrena Bajiko
- Nell Ehrlinger

## Assessment
Southern Cross University – IoT Unit – Assignment 1
