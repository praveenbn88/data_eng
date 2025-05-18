# Think Home - Smart Home Automation System

This Arduino-based project implements a smart home automation system that controls lighting and fan systems using PIR (Passive Infrared) and IR (Infrared) sensors. The system is designed to automatically manage room lighting and fan control based on occupancy detection.

## Hardware Requirements

### Components
- Arduino Board (compatible with Arduino IDE)
- 2 PIR Sensors (for Bedroom 1 and Bedroom 2)
- 2 IR Sensors (for Bathroom 1 and Bathroom 2)
- 4 Relay Modules (for controlling lights)
- 1 Fan Control Module
- Jumper Wires
- Power Supply

### Pin Connections

#### PIR Sensors
- Bedroom 1 PIR:
  - Input Pin: 3
  - Output Pin: 5
- Bedroom 2 PIR:
  - Input Pin: 4
  - Output Pin: 6

#### IR Sensors
- Bathroom 1 IR:
  - Input Pin: 7
  - Output Pin: 9
- Bathroom 2 IR:
  - Input Pin: 8
  - Output Pin: 10

#### Fan Control
- Input Pin: 12
- Output Pin: 11

## Features

1. **Automatic Room Lighting**
   - Bedroom 1: Controlled by PIR sensor
   - Bedroom 2: Controlled by PIR sensor
   - Bathroom 1: Controlled by IR sensor
   - Bathroom 2: Controlled by IR sensor

2. **Fan Control**
   - Manual control through input pin 12
   - Automatic relay control through output pin 11

3. **Serial Monitoring**
   - System status messages are sent via Serial at 9600 baud rate
   - Room entry/exit notifications
   - Debug information

## Operation

### Room Lighting Control
- Each room's lighting is controlled independently
- Lights turn ON when motion is detected
- Lights turn OFF after 4 seconds of no motion
- Status changes are logged via Serial monitor

### Fan Control
- Fan can be controlled manually through input pin 12
- HIGH signal turns the fan ON
- LOW signal turns the fan OFF

## Setup Instructions

1. Connect all components according to the pin connections listed above
2. Upload the code to your Arduino board using the Arduino IDE
3. Open Serial Monitor (9600 baud) to view system status
4. Power on the system

## Code Structure

The code is organized into several sections:
- Pin definitions and variable declarations
- Setup function for pin initialization
- Main loop containing:
  - Fan control logic
  - PIR sensor control for bedrooms
  - IR sensor control for bathrooms
  - Serial communication for status updates

## Notes

- The system uses a 4-second delay for light control to prevent rapid switching
- All status changes are logged to the Serial monitor for debugging
- The system is designed to handle multiple rooms independently
- PIR sensors are used for larger spaces (bedrooms)
- IR sensors are used for smaller spaces (bathrooms)

## Troubleshooting

1. If sensors are not responding:
   - Check power supply
   - Verify pin connections
   - Ensure proper sensor placement

2. If lights are not turning ON/OFF:
   - Check relay connections
   - Verify output pin connections
   - Test relay module independently

3. If Serial monitor shows no output:
   - Verify correct baud rate (9600)
   - Check USB connection
   - Ensure proper code upload

## Safety Precautions

- Always disconnect power before making any connections
- Use appropriate voltage levels for your components
- Ensure proper insulation of all electrical connections
- Follow local electrical safety regulations
- Use appropriate relay ratings for your lighting system 
