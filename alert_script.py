import os
import csv
import json
import time
import logging
import schedule
import socketio
import requests
import paho.mqtt.client as mqtt
from datetime import datetime, timedelta


#________________extra data
TOKEN = '7560275252:AAHz2K_5pyyCdnGcFLa2iv-S9bp4Z3z16Nw'
retention_days = 1
time_of_remove_logs = "00:00"
MQTT_BROKER = '192.168.1.175'
MQTT_PORT = 1883
MQTT_TOPIC = 'alerts/my_alerts'


# Configure logging
logging.basicConfig(level=logging.ERROR)

#Send Telegram Alert
# CHAT_ID = '7990074653'

def send_telegram_alert(machine_id, alert_id, tag_values, alert_name, chat_id):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    
    message = (
        f"<b>🚨 Alert Notification 🚨</b>\n\n"
        f"<b>Machine:</b> {machine_id}\n"
        f"<b>Alert ID:</b> {alert_id}\n"
        f"<b>Alert Name:</b> {alert_name}\n"
        f"<b>Alert Values:</b> {tag_values}\n\n"
        f"Alert regarding: <b>{alert_name}</b>."
    )
    payload = {
        'chat_id': chat_id,
        'text': message,
        'parse_mode': 'HTML'  # You can use HTML to format the message
    }
    response = requests.post(url, data=payload)
    if response.status_code == 200:
        # print("Alert sent successfully.")
        return True
    else:
        print(f"Failed to send alert. Status code: {response.status_code}, Error: {response.text}")
        return False



def setup_mqtt_clients():
    mqtt_client_pub = mqtt.Client(client_id="mqtt_client_pub", protocol=mqtt.MQTTv311)
    mqtt_client_sub = mqtt.Client(client_id="mqtt_client_sub", protocol=mqtt.MQTTv311)

    # Define on_connect and on_message callbacks
    mqtt_client_pub.on_connect = lambda client, userdata, flags, rc: print(f"Publisher connected: {rc}")
    mqtt_client_sub.on_connect = lambda client, userdata, flags, rc: print(f"Subscriber connected: {rc}")
    mqtt_client_sub.on_message = on_message

    # Connect to the broker with QoS 1 for both publishing and subscribing
    mqtt_client_pub.connect(MQTT_BROKER, MQTT_PORT)
    mqtt_client_sub.connect(MQTT_BROKER, MQTT_PORT)

    # Subscribe with QoS 1 to receive messages at least once
    mqtt_client_sub.subscribe(MQTT_TOPIC, qos=1)  # QoS 1 ensures at least once delivery

    mqtt_client_sub.loop_start()  # Start the loop for receiving messages

    return mqtt_client_pub



#_________store alert log in csv file____________
LOG_FOLDER = 'alerts_logs'
CSV_FILE = os.path.join(LOG_FOLDER, 'alerts_log.csv')
MQTT_ERROR_FILE = os.path.join(LOG_FOLDER, 'mqtt_error_log.csv')
TELEGRAM_ERROR_FILE = os.path.join(LOG_FOLDER, 'telegram_error_log.csv')

def initialize_csv():
    """Initialize the log files with headers inside the alerts_logs folder."""
    # Create the folder if it doesn't exist
    if not os.path.exists(LOG_FOLDER):
        os.makedirs(LOG_FOLDER)
        print(f"Folder '{LOG_FOLDER}' created.")

    # Initialize CSV files with headers if they don't exist
    for file_path, headers in [
        (CSV_FILE, ['Timestamp (ms)', 'Date & Time', 'Machine Id', 'Alert ID', 'Value', 'Alert Name','Flag']),
        (MQTT_ERROR_FILE, ['Timestamp (ms)', 'Date & Time', 'Machine Id', 'Alert ID', 'Value', 'Alert Name','Flag']),
        (TELEGRAM_ERROR_FILE, ['Timestamp (ms)', 'Date & Time', 'Machine Id', 'Alert ID', 'Value', 'Alert Name','Flag'])
    ]:
        if not os.path.exists(file_path):
            with open(file_path, mode='w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(headers)
            print(f"{file_path} initialized.")

try:
    initialize_csv()
except Exception as e:
    print(f"Failed to initialize CSV file: {e}")

def telegram_error_log(machine_id, alert_id, value, alert_name, flag):
    """Log Telegram errors to a CSV file, including the flag."""
    with open(TELEGRAM_ERROR_FILE, mode='a', newline='') as file:
        writer = csv.writer(file)
        timestamp = int(time.time() * 1000)
        writer.writerow([timestamp, datetime.now(), machine_id, alert_id, value, alert_name, flag])
    print(f"Error logged in {TELEGRAM_ERROR_FILE}.")

def mqtt_error_log(machine_id, alert_id, value, alert_name, flag):
    """Log MQTT errors to a CSV file, including the flag."""
    with open(MQTT_ERROR_FILE, mode='a', newline='') as file:
        writer = csv.writer(file)
        timestamp = int(time.time() * 1000)
        writer.writerow([timestamp, datetime.now(), machine_id, alert_id, value, alert_name, flag])
    print(f"Error logged in {MQTT_ERROR_FILE}.")

def log_alert_to_csv(machine_id, alert_id, value, alert_name, flag):
    """Log an alert to the CSV file, including the flag."""
    with open(CSV_FILE, mode='a', newline='') as file:
        writer = csv.writer(file)
        timestamp = int(time.time() * 1000)
        # Write a row with timestamp, flag, and alert information
        writer.writerow([timestamp, datetime.now(), machine_id, alert_id, value, alert_name, flag])
    print("Alert logged to CSV.")



# ----------clean csv logs---------------
def cleanup_old_logs():
    # Calculate the cutoff timestamp for log retention
    cutoff_date = datetime.now() - timedelta(days=retention_days)
    
    # Read existing data
    with open(CSV_FILE, 'r') as file:
        reader = csv.reader(file)
        header = next(reader)
        rows = [row for row in reader if datetime.fromtimestamp(int(row[0])/1000) >= cutoff_date]

    # Write back only recent rows
    with open(CSV_FILE, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(header)
        writer.writerows(rows)



# Initialize Socket.IO client
sio = socketio.Client()

import os
import json

# Global variable to track the last modification time of the config file
last_config_timestamp = None
cached_config = None

def load_config():
    """Load configuration from 'config.json' only if the file has changed."""
    global last_config_timestamp, cached_config

    # Get the directory where the script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(script_dir, 'config.json')

    # Define default configuration
    default_config = []

    try:
        # Check if the config file exists
        if not os.path.exists(config_file):
            # Create a new config file with default content
            with open(config_file, 'w') as f:
                json.dump(default_config, f, indent=4)
            print(f"Default configuration file created at '{config_file}'.")

        # Get the current modification timestamp of the file
        current_timestamp = os.path.getmtime(config_file)

        # Check if the file has been modified since the last check
        if last_config_timestamp is None or current_timestamp > last_config_timestamp:
            # Update the last known timestamp
            last_config_timestamp = current_timestamp

            # Reload the configuration
            with open(config_file, 'r') as f:
                cached_config = json.load(f)
            print("__________________Configuration file updated and reloaded.___________________")

        return cached_config

    except Exception as e:
        print(f"Error handling configuration: {e}")
        return default_config




from collections import defaultdict, deque

# Store the last 50 values of each tag for spike detection

def detect_spike(alert_id,tag_id, current_value, window_size):
    """
    Detect if there is a spike in the current value compared to the average of the last 'window_size' values.
    Triggers an alert if the change is 3% or more.
    """
    print("check for spike in current....")
    previous_values = defaultdict(lambda: defaultdict(lambda: deque(maxlen=50)))
    # global previous_values
    
    # Initialize deque for the tag if it doesn't exist
    if tag_id not in previous_values:
        previous_values[tag_id] = deque(maxlen=window_size)

    # Get the deque of previous values for this tag
    values = previous_values[alert_id][tag_id]

    # Add the current value to the deque
    values.append(current_value)
    

    # Calculate the average of the last 50 values
    if len(values) < 14:
        # Not enough data points yet
        return False

    average_value = sum(values) / len(values)
    

    # Avoid division by zero
    if average_value == 0:
        return False

    # Calculate the percentage change
    percentage_change = abs((current_value - average_value) / average_value) * 100
    
    print("change in percentage:",percentage_change)
    
    # Check if the percentage change is 3% or more
    if percentage_change >= 3:
        print(f"Spike detected: {tag_id} changed by {percentage_change:.2f}% compared to the average of last {window_size} values.")
        return True

    return False


def detect_voltage_spike(alert_id, tag_id, current_value, window_size):
    """
    Detect if there is a spike in the current voltage value compared to the average of the last 'window_size' values.
    Triggers an alert if the change is 3% or more.
    """
    print(f"Checking for voltage spike on {tag_id}...")
    previous_voltage_values = defaultdict(lambda: defaultdict(lambda: deque(maxlen=50)))
    
    # Initialize deque for the tag if it doesn't exist
    if tag_id not in previous_voltage_values[alert_id]:
        previous_voltage_values[alert_id][tag_id] = deque(maxlen=window_size)

    # Get the deque of previous voltage values for this tag
    values = previous_voltage_values[alert_id][tag_id]

    # Add the current voltage value to the deque
    values.append(current_value)
    
    # Calculate the average of the last 'window_size' values
    if len(values) < window_size:
        # Not enough data points yet
        return False

    average_value = sum(values) / len(values)
    
    # Avoid division by zero
    if average_value == 0:
        return False

    # Calculate the percentage change in voltage
    percentage_change = abs((current_value - average_value) / average_value) * 100
    
    print(f"Voltage change in percentage: {percentage_change}%")
    
    # Check if the percentage change is 3% or more
    if percentage_change >= 3:
        print(f"Voltage spike detected: {tag_id} changed by {percentage_change:.2f}% compared to the average of the last {window_size} values.")
        return True

    return False


def calculate_voltage_imbalance(b,y,r):
    """
    Calculate the voltage imbalance percentage.
    
    Args:
        phase_voltages (dict): A dictionary containing phase voltages with keys 'V_A', 'V_B', and 'V_C'.
    
    Returns:
        float: The voltage imbalance percentage.
    """
    print("Calculating voltage imbalance...")

    # Put phase voltages in a dictionary (you can use the variable names directly)
    phase_voltages = {"R": r, "Y": y, "B": b}

    # Extract phase voltages as a list
    voltages = list(phase_voltages.values())
    
    # Ensure there are at least two voltages to compare
    if len(voltages) < 2:
        print("Error: Insufficient data for imbalance calculation.")
        return False
    
    # Calculate the average voltage
    avg_voltage = sum(voltages) / len(voltages)
    
    # Find the maximum and minimum voltages
    max_voltage = max(voltages)
    min_voltage = min(voltages)
    
    # Calculate the imbalance percentage based on the average voltage
    imbalance_percentage = ((max_voltage - min_voltage) / avg_voltage) * 100
    
    print("Imbalance percentage:", imbalance_percentage)
    
    # Check if imbalance exceeds 5%
    if imbalance_percentage > 5:
        print(f"Imbalance voltage exceeds 5%.")
        return True
    
    return False

     
def calculate_current_imbalance(b,y,r):
    """
    Calculate the current imbalance percentage.
    
    Args:
        phase_currents (dict): A dictionary containing current values for each phase.
    
    Returns:
        bool: True if imbalance exceeds threshold, False otherwise.
    """
    print("calculate current imbalance....")
    phase_currents = {"R": r, "Y": y, "B": b}
    # print("Phase current data:", phase_currents)
    # Extract phase current values
    # print("Phase current data:", phase_currents)
    currents = list(phase_currents.values())
    
    # Ensure there are at least two current values to compare
    if len(currents) < 2:
        print("Error: Insufficient data for current imbalance calculation.")
        return False

    # Find the maximum and minimum current values
    max_current = max(currents)
    # print("max current",max_current)
    min_current = min(currents)
    
    # Calculate the percentage imbalance
    imbalance_percentage = ((max_current - min_current) / min_current)
    
    print("imbalance_percentage",imbalance_percentage)
    
    # Set the imbalance threshold (5% by default, configurable)
    threshold = 5
    if imbalance_percentage > threshold:
        print(f"Current imbalance detected: {imbalance_percentage:.2f}% (Threshold: {threshold})")
        return True
    
    return False


def is_voltage_unstable(V_LN,V_BR,V_YB,V_RY,threshold):
    """
    Check if the three-phase voltage system is unstable.
    
    :param data: Dictionary containing voltage values.
    :return: True if unstable, False if stable.
    """
    print("calculate voltage unstable....")
    # Nominal voltages
    nominal_V_LN = 230  # Adjust as per your system
    nominal_V_LL = 400  # Adjust as per your system

    # Calculate Line-to-Line (LL) average
    V_LL_avg = (V_BR + V_YB + V_RY) / 3

    # Check imbalance
    deviations = [abs(V_BR - V_LL_avg), abs(V_YB - V_LL_avg), abs(V_RY - V_LL_avg)]
    max_deviation = max(deviations)
    imbalance_percentage = (max_deviation / V_LL_avg) * 100
    
    print("imbalance_percentage :",imbalance_percentage)

    # Unstable if imbalance exceeds threshold
    if imbalance_percentage > threshold:  # Imbalance threshold
        return True

    # Check nominal voltage deviations
    if abs(V_LN - nominal_V_LN) > nominal_V_LN * 0.05:
        return True
    if any(abs(phase - nominal_V_LL) > nominal_V_LL * 0.05 for phase in [V_BR, V_YB, V_RY]):
        return True

    # Stable if no conditions for instability are met
    return False





message_received = False
# Handle received MQTT messages
def on_message(client, userdata, message):
    global message_received
    # print(f"Received message: '{message.payload.decode()}' on topic '{message.topic}'")
    if message.payload.decode():
        message_received = True
        # print("Message not confirmed as received")
        # return False
    else:
        pass
        print("Received empty message or message not confirmed.")
        
        
def check_alert(data, config):
    """Evaluate alert conditions based on dynamic tag_ids and execute rule."""
    if not isinstance(config, dict) or not isinstance(data, dict):
        logging.error("Invalid input: 'config' and 'data' must be dictionaries.")
        return False

    tag_ids = config.get("tag_id", {})
    alert_id = config.get("_id")
    rule = config.get("rule")

    if not rule:
        logging.error("No rule provided in configuration.")
        return False

    # Base local variables for all alert types
    base_functions = {
        "alert_id": alert_id,
        "calculate_current_imbalance": calculate_current_imbalance,
        "calculate_voltage_imbalance": calculate_voltage_imbalance,
        "detect_spike": detect_spike,
        "is_voltage_unstable": is_voltage_unstable,
        "detect_voltage_spike":detect_voltage_spike,
        "datetime": datetime
    }

    local_vars = {"result": False}

    # Populate `local_vars` with dynamic tags from `tag_id`
    for var_name, tag_id in tag_ids.items():
        if tag_id in data:
            local_vars[var_name] = data[tag_id]
        else:
            logging.error(f"Tag ID '{tag_id}' not found in incoming data.")
            return False

    # Execute rule logic
    try:
        exec(rule, base_functions, local_vars)
        
        print("result is :",local_vars.get("result", False))

        logging.info(f"Execution results: {local_vars.get('result')}")
        
        # _____________for testing_________________
        # if not hasattr(check_alert, "counter"):
        #     check_alert.counter = 0
    
        # # Increment the counter every time the function is called
        # check_alert.counter += 1

        # # Return True the first time, False for the next three times
        # if check_alert.counter == 1:
        #     check= True
        # else:
        #     check= False
        #     if check_alert.counter >= 4:
        #         check_alert.counter = 0  # Reset the counter after 4 calls
        # print(check)
        # return check
        
        return local_vars.get("result", False)
    except Exception as e:
        logging.error(f"Error executing rule: {e}")
        return False

        
# Function to send alert notifications and handle potential errors
def handle_alert_notification(alert_message, machine_id, alert_id, value, alert_name,flag):
   
    # 1. Log alert in CSV
    log_alert_to_csv(machine_id, alert_id, value, alert_name, flag)

    global message_received
    # 2. Publish alert to MQTT broker and handle errors
    try:
        mqtt_client_pub.publish(MQTT_TOPIC, alert_message, qos=1)
        
        if not message_received:
            # print("No confirmation of MQTT message reception.")
            # Log the alert again if MQTT confirmation failed
            mqtt_error_log(machine_id, alert_id, value, alert_name, flag)
    except Exception as e:
        # print(f"Failed to publish to MQTT broker: {e}")
        mqtt_error_log(machine_id, alert_id, value, alert_name, flag)
    finally:
        message_received = False
    

last_alert_states = {}  # Tracks if alert was previously active
alert_sent_states = {}  # Tracks if alert was already sent once
last_alert_times = {}

# Update the `should_send_alert` function to track re-trigger logic

def should_send_alert(tag_id, alert_id, alert_triggered, periodic, frequency):
    """Determine if an alert should be sent based on periodic frequency or if re-triggered."""
    current_time = datetime.now()
    alert_key = (tag_id, alert_id)

    # Retrieve the last known alert state and time
    last_state = last_alert_states.get(alert_key, False)
    last_alert_time = last_alert_times.get(alert_key, current_time - timedelta(minutes=frequency))

    # If the alert condition is false, reset the alert state and return False
    if not alert_triggered:
        last_alert_states[alert_key] = False
        alert_sent_states[alert_key] = False  # Reset to allow re-triggering
        return False

    # Check if alert should be sent based on periodic or one-time configuration
    if periodic:
        # For periodic alerts, send if the time interval has elapsed
        if current_time - last_alert_time >= timedelta(minutes=frequency):
            last_alert_times[alert_key] = current_time  # Update the last alert time
            last_alert_states[alert_key] = True
            alert_sent_states[alert_key] = True  # Mark alert as sent
            return True
    else:
        # For one-time alerts, only re-trigger if the condition was previously false
        if alert_triggered and not last_state:
            last_alert_states[alert_key] = True  # Mark alert as triggered
            alert_sent_states[alert_key] = True
            return True

    return False


@sio.event
def connect():
    """Handle connection event."""
    print("Connected to Server.")

@sio.on('data')
def on_data(data):
    """Handle incoming data event."""
    logging.debug("Receiving data...")
    # print("receiving data:", data)
    config_list = load_config()

    for config in config_list: 
        alert_type = config.get("alert_type")
        tag_id = config.get("tag_id", {})
        rule = config.get("rule")
        machine_id = config.get("machine_id")
        alert_name = config.get("alert_name")
        periodic = config.get("periodic", False)
        alert_frequency = config.get("alert_frequency", 1)
        alert_id = config.get("_id")
        status = config.get("status")
        telegram_channels = config.get("telegram_channels", [])
        flag = config.get("flag", "red")        
        
        if status is False:
            continue
        
        alert_triggered = check_alert(data, config)
        
        # Call should_send_alert only if the alert is triggered or the condition is toggling
        should_alert = should_send_alert(
            tag_id=None,  # No single tag_id here; it's a dynamic condition
            alert_id=alert_id,
            alert_triggered=alert_triggered,
            periodic=periodic,
            frequency=alert_frequency
        )
        
        if should_alert:
            
            tag_values = {key: data.get(tag) for key, tag in tag_id.items()}
            tag_values_text = ', '.join([f"{key}: {value}" for key, value in tag_values.items()])
            

            alert_message = f"Alert generated for machine {machine_id}:(ID: {alert_id})Alert values:{tag_values_text} Alert Name: {alert_name}"
            # print(tag_values)
            print(alert_message)
            
            
            # for chat_id in telegram_channels:
            #     telegram_sent = send_telegram_alert(machine_id, alert_id, tag_values_text, alert_name, chat_id)
            #     if not telegram_sent:
            #         telegram_error_log(machine_id, alert_id, tag_values_text, alert_name,flag)
            #         print("Message not sent to Telegram",chat_id)


            handle_alert_notification(alert_message, machine_id, alert_id, tag_values_text, alert_name, flag)
            
CONNECTION_TOKEN = 'yqjdHAvNM2y6kB2r2Lq7PLEXEtXTnPF6'
URL = 'http://localhost:3001'

def main():
    """Main function to connect to the server and maintain the connection."""
    global mqtt_client_pub
    
    try:
        mqtt_client_pub = setup_mqtt_clients()
    except Exception as e:
        pass
        # print(f"Failed to connect to MQTT broker: {e}")
        
    schedule.every().day.at(time_of_remove_logs).do(cleanup_old_logs)
    
    try:
        sio.connect(f'{URL}?token={CONNECTION_TOKEN}', transports='websocket')
        while True:
            schedule.run_pending()
            logging.debug("Running...")
            time.sleep(1)
    except Exception as e:
        logging.error(f"Connection failed: {e}")
    except KeyboardInterrupt:
        logging.info("Exiting...")
    finally:
        mqtt_client_pub.loop_stop()
        sio.disconnect()

if __name__ == "__main__":
    main()