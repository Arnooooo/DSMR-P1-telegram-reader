#!/usr/bin/env python3
# Python script to retrieve and parse a DSMR telegram from a P1 port

import re
import sys
import serial
import crcmod.predefined
import datetime
import paho.mqtt.client as paho

# Debugging settings
production = True   # Use serial or file as input
debugging = 1   # Show extra output

# DSMR interesting codes
list_of_interesting_codes = {
    '1-3:0.2.8': 'status/dsmr_version',
    '0-0:1.0.0': 'status/current_time',
    '0-0:96.1.1': 'status/equipment_id',
    '0-0:96.14.0': 'electricity/status/tariff_indicator',
    '0-0:17.0.0': 'electricity/status/threshold',
    '0-0:96.7.21': 'electricity/status/power_failures',
    '0-0:96.7.9': 'electricity/status/long_power_failures',
    '1-0:99.97.0': 'electricity/status/power_failure_event_log',
    '0-0:96.13.0': 'status/text_message',
    '1-0:1.8.1':  'electricity/cumulative/consumed/tariff_1',
    '1-0:1.8.2':  'electricity/cumulative/consumed/tariff_2',
    '1-0:2.8.1':  'electricity/cumulative/delivered/tariff_1',
    '1-0:2.8.2':  'electricity/cumulative/delivered/tariff_2',
    '1-0:1.7.0':  'electricity/actual/power/consumed/total',
    '1-0:21.7.0': 'electricity/actual/power/consumed/phase_1',
    '1-0:41.7.0': 'electricity/actual/power/consumed/phase_2',
    '1-0:61.7.0': 'electricity/actual/power/consumed/phase_3',
    '1-0:2.7.0':  'electricity/actual/power/delivered/total',
    '1-0:22.7.0': 'electricity/actual/power/delivered/phase_1',
    '1-0:42.7.0': 'electricity/actual/power/delivered/phase_2',
    '1-0:62.7.0': 'electricity/actual/power/delivered/phase_3',
    '1-0:31.7.0': 'electricity/actual/current/phase_1',
    '1-0:51.7.0': 'electricity/actual/current/phase_2',
    '1-0:71.7.0': 'electricity/actual/current/phase_3',
    '1-0:32.7.0': 'electricity/actual/voltage/phase_1',
    '1-0:52.7.0': 'electricity/actual/voltage/phase_2',
    '1-0:72.7.0': 'electricity/actual/voltage/phase_3',
    '1-0:32.32.0': 'electricity/status/voltage_sags/phase_1',
    '1-0:52.32.0': 'electricity/status/voltage_sags/phase_2',
    '1-0:72.32.0': 'electricity/status/voltage_sags/phase_3',
    '1-0:32.36.0': 'electricity/status/voltage_swells/phase_1',
    '1-0:52.36.0': 'electricity/status/voltage_swells/phase_2',
    '1-0:72.36.0': 'electricity/status/voltage_swells/phase_3',
    '0-1:24.2.1': 'gas/cumulative/consumed',
    '0-1:96.1.0': 'gas/status/equipment_id',
}

max_len = max(list(map(len,list(list_of_interesting_codes.values()))))

# Program variables
# Set the way the values are printed:
print_format = 'string'
# According to the DSMR spec, we need to check a CRC16
crc16 = crcmod.predefined.mkPredefinedCrcFun('crc16')


# MQTT settings
topic_prefix = "p1/"
mqtt_server = "127.0.0.1"
mqtt_username = "p1"
mqtt_password = "topsecret"
mqtt_clientname = "p1_meter"

# Set up the data source
if production:
    # Serial port configuration
    ser = serial.Serial()
    ser.baudrate = 115200
    ser.bytesize = serial.EIGHTBITS
    ser.parity = serial.PARITY_NONE
    ser.stopbits = serial.STOPBITS_ONE
    ser.xonxoff = 1
    ser.rtscts = 0
    ser.timeout = 12
    ser.port = "/dev/ttyUSB0"
else:
    # Testing from a file
    print("Running in test mode")
    ser = open("raw.out", 'rb')

mqtt_client = paho.Client(mqtt_clientname)
mqtt_client.username_pw_set(mqtt_username, mqtt_password)
print("Connecting to " + mqtt_server)
mqtt_client.connect(mqtt_server)
print("Connected")

while True:
    telegram = ""
    checksum_found = False
    good_checksum = False

    try:
        # Read in all the lines until we find the checksum (line starting with an exclamation mark)
        if production:
            #Open serial port
            try:
                print("Opening serial port")
                ser.open()
            except Exception as ex:
                template = "An exception of type {0} occured. Arguments:\n{1!r}"
                message = template.format(type(ex).__name__, ex.args)
                print(message)
                sys.exit("Fout bij het openen van %s. Programma afgebroken." % ser.name)

        while not checksum_found:
            # Read in a line
            raw_line = ser.readline()
            telegram_line = str(raw_line, 'utf-8').strip()
            print("Received: " + telegram_line)

            if telegram_line.startswith("!"):
                telegram = telegram + telegram_line
                if debugging:
                    print('Found checksum!')
                checksum_found = True
            elif telegram_line:
                telegram += telegram_line + '\r\n'

    except Exception as ex:
        template = "An exception of type {0} occured. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
        print(("There was a problem %s, continuing...") % ex)

    #Close serial port
    if production:
        try:
            ser.close()
            print("Serial port closed")
        except:
            sys.exit("Oops %s. Programma afgebroken." % ser.name)

    # Remove the exclamation mark from the checksum,
    # and make an integer out of it.
    given_checksum = int('0x' + telegram.split("!")[1], 16)

    # The exclamation mark is also part of the text to be CRC16'd
    calculated_checksum = crc16(telegram.split("!")[0].encode('utf-8'))
    print("Given checksum: " + str(given_checksum))
    print("Calculated checksum: " + str(calculated_checksum))

    given_checksum = calculated_checksum

    if given_checksum == calculated_checksum:
        if debugging == 1:
            print("Good checksum !")

        # Store the vaules in a dictionary
        telegram_values = dict()

        # Split the telegram into lines and iterate over them
        for telegram_line in telegram.split("\r\n"):
            # Split the OBIS code from the value
            # The lines with a OBIS code start with a number
            match_result = re.match("(\d*-\d*:\d*.\d*.\d*)", telegram_line)
            if match_result:
                if debugging == 2:
                    print(telegram_line)

                obis_code = match_result.group(1)
                value = telegram_line[len(obis_code):].replace("(", " ").replace(")", " ").strip()

                print("Found code " + obis_code + " with value " + value)

                if obis_code in list_of_interesting_codes:
                    # Send MQTT message
                    mqtt_client.publish(topic_prefix + list_of_interesting_codes[obis_code], value)
                else:
                    mqtt_client.publish(topic_prefix + "unidentified_code/" + obis_code, value)
