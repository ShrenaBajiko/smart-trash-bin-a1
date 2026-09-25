import time
import network
from machine import Pin
from utime import sleep
from machine import PWM
from time import sleep_us
from machine import time_pulse_us
from machine import ADC  #new
from umqttsimple import MQTTClient
import ntptime  #new
import json

#MQTT connection
MQTT_CLIENT_ID = "picow-01"
MQTT_BROKER = "9a8151b644b74dc1b579dc3e05d4f35e.s1.eu.hivemq.cloud"
MQTT_USER = "user1"
MQTT_PASSWORD = "Password"
MQTT_HEARTBEAT_TOPIC = "bins/heartbeat" 
MQTT_TOPIC_LID_COMMAND = "bins/commands/lid"
MQTT_TOPIC_STATUS = "bins/status"
MQTT_TOPIC_WARNING = "bins/warnings"

#bin config values
BIN_ID = "bin-01"
BIN_DEPTH_CM = 100
PUBLISH_THRESHOLD = 5
HEARTBEAT_INTERVAL_S = 30
LID_OPEN_DURATION_S = 10
STATUS_QUEUE_S = 60
WARNING_QUEUE_S = 24 * 60 * 60 #24 hours 60 minutes and 60 seconds per hour
FULL_THRESHOLD = 90 #less than 10% is considered full
FULL_RESET_THRESHOLD = 85 #more than 15% not considered full anymore

trigger = Pin(28, Pin.OUT) #sends signal
echo = Pin(26, Pin.IN) #retrieves signal 

servo = PWM(Pin(11))
servo.freq(50)

cpu_sensor = ADC(4)  #new
time.sleep(0.1) # Wait for USB to become ready

pos = 0
mqtt_offline_queue = []

#moving average state
distance_history = []
HISTORY_SIZE = 5
average_distance = 0

#publish tracking
last_publish_time = 0
last_capacity = 100
warning_id = 0

#heartbeat and lid state
last_heartbeat_time = 0
lid_open_until = 0
last_command_ids = []
start_time = time.time()
last_capacity_sent = None

# Keep only the newest pending status and one pending full warning.
pending_status = None
pending_warning = None
full_alert_active = False

mqtt_connected = False
last_reconnect_attempt = 0

def rotate_servo(servo, angle): 
    duty = int(1638 + (angle/180.0) * (8192-1638)) 
    servo.duty_u16(duty)

#new format timestamp
def get_timestamp():
    t = time.localtime()
    return "{:04d}-{:02d}-{:02d}T{:02d}:{:02d}:{:02d}Z".format(t[0], t[1], t[2], t[3], t[4], t[5])

#new read cpu temp, clamp because wokwi returns wrong values
def read_cpu_temp():
    reading = cpu_sensor.read_u16() * (3.3 / 65535)
    real_temp = 27 - (reading - 0.706) / 0.001721
    if real_temp < 0 or real_temp > 100:
        return 35.0
    return real_temp

def read_distance():
    trigger.value(0)
    sleep_us(2)
    trigger.value(1)
    sleep_us(10)
    trigger.value(0)
    duration = time_pulse_us(echo, 1, 300000)
    if duration > 0:
        return duration / 58.2
    else:
        print("cannot read the distance")
        return -1

#check if an ISO timestamp has already passed
def is_expired(iso_timestamp):
    date_part, time_part = iso_timestamp.split("T")
    year, month, day = date_part.split("-")
    time_part = time_part.rstrip("Z")
    hour, minute, second = time_part.split(":")
    expires_epoch = time.mktime((int(year), int(month), int(day),
                                 int(hour), int(minute), int(second), 0, 0))
    return time.time() > expires_epoch

#callback function to receiver lid open commands from MQTT server/tablet
def sub_callback(topic, msg):
    global lid_open_until, last_command_ids, pos

    if topic != MQTT_TOPIC_LID_COMMAND.encode():
        return

    try:
        command = json.loads(msg.decode())
        command_id = command.get("command_id")
        action = command.get("action")
        expires_at = command.get("expires_at")  #new

        #ignore expired commands
        if expires_at and is_expired(expires_at):
            print("Command expired, ignored:", command_id)
            return

        #ignore command if invalid ( command always needs timestamp, command_id, and open action)
        if not command_id or not expires_at or action != "open":
            print("Invalid lid command")
            return
            
        #ignore command if command_id already appeared
        if command_id in last_command_ids:
            print("Duplicate command ignored")
            return

        #add command id to the list
        last_command_ids.append(command_id)
        if len(last_command_ids) > 10:
            last_command_ids.pop(0)

        #open bin if comamnd is valid
        pos = 90
        rotate_servo(servo, 90)
        print("lid opened")
        lid_open_until = time.time() + LID_OPEN_DURATION_S

    except Exception as e:
        print("Bad lid command:", e)

#connect to WIFI
print("Connecting to Wifi", end="")
wlan = network.WLAN(network.STA_IF)
wlan.active(True)
wlan.connect("Wokwi-GUEST", "")
while not wlan.isconnected():
    print(".", end="")
    time.sleep(0.1)
print(wlan.ifconfig())
print("Wifi connected")

try:
    ntptime.settime()
    clock_synced = True
    print("Time synced:", time.localtime())
except Exception as e:
    print("Time sync failed:", repr(e))

#connecting to MQTT client
print("Connecting to MQTT server...", end="")
client = MQTTClient(
    client_id = MQTT_CLIENT_ID,
    server = MQTT_BROKER, 
    user = MQTT_USER,
    password = MQTT_PASSWORD,
    keepalive = 7200,
    ssl = True,
    ssl_params={"server_hostname": MQTT_BROKER})
client.set_callback(sub_callback)  #new

def connect_mqtt():
    global mqtt_connected, last_capacity_sent

    try:
        client.connect()
        client.subscribe(MQTT_TOPIC_LID_COMMAND, qos=1)
        mqtt_connected = True

        # Send a fresh status reading after reconnection.
        last_capacity_sent = None
        print("MQTT connected; subscribed to", MQTT_TOPIC_LID_COMMAND)
        return True

    except Exception as e:
        mqtt_connected = False
        print("MQTT connection failed:", e)
        return False

def publish_or_queue(topic, payload, qos, message_type):
    global mqtt_connected, pending_status, pending_warning

    queued_item = {
        "topic": topic,
        "payload": json.dumps(payload),
        "qos": qos,
        "created_at": time.time(),
    }

    if mqtt_connected:
        try:
            client.publish(topic, queued_item["payload"], qos=qos)
            print("Published:", topic, queued_item["payload"])
            return True
        except Exception as e:
            mqtt_connected = False
            print("Publish failed:", e)

    if message_type == "status":
        pending_status = queued_item  # replace any older status
    elif message_type == "warning":
        pending_warning = queued_item

    return False


def send_pending(current_capacity):
    global pending_status, pending_warning, mqtt_connected

    if not mqtt_connected:
        return

    now = time.time()

    # Do not send an old "bin full" warning if the bin was emptied.
    if pending_warning:
        age = now - pending_warning["created_at"]
        if age > WARNING_QUEUE_S or current_capacity < FULL_RESET_THRESHOLD:
            print("Discarding expired or no-longer-relevant warning")
            pending_warning = None
        else:
            try:
                client.publish(
                    pending_warning["topic"],
                    pending_warning["payload"],
                    qos=1,
                )
                print("Queued warning sent")
                pending_warning = None
            except Exception as e:
                mqtt_connected = False
                print("Warning still queued:", e)
                return

    if pending_status:
        age = now - pending_status["created_at"]
        if age > STATUS_QUEUE_TTL_S:
            print("Discarding stale status")
            pending_status = None
        else:
            try:
                client.publish(
                    pending_status["topic"],
                    pending_status["payload"],
                    qos=0,
                )
                print("Queued status sent")
                pending_status = None
            except Exception as e:
                mqtt_connected = False
                print("Status still queued:", e)

connect_mqtt()


while True:
    now = time.time()
    distance = read_distance()

    # Receive lid commands and detect a broken MQTT connection.
    if mqtt_connected:
        try:
            client.check_msg()
        except Exception as e:
            mqtt_connected = False
            print("MQTT polling failed:", e)

    # Close the lid after 10 seconds.
    if (lid_open_until > 0 and now >= lid_open_until):
        rotate_servo(servo, 0)
        pos = 0
        lid_open_until = 0
        distance_history = []
        print("Lid auto-closed")

    # Measure fill level while the lid is closed and ignore invalid readings
    if (pos == 0 and distance is not None):
        fill_level = max(0,min(100, (BIN_DEPTH_CM - distance) / BIN_DEPTH_CM * 100),)

        # Reset bin state to not full if fill_level falls below 85%.
        if fill_level < FULL_RESET_THRESHOLD:
            full_alert_active = False
            pending_warning = None

        # Warning when the fill level reaches 90% (only once)
        if fill_level >= FULL_THRESHOLD and not full_alert_active:
            #print("warning send")
            #print(round(fill_level, 1))
            warning = {
                "event_id": "{}-{}".format(BIN_ID, now),
                "bin_id": BIN_ID,
                "event": "bin_full",
                "severity": "warning",
                "fill_pct": round(fill_level, 1),
                "threshold_pct": FULL_THRESHOLD,
                "timestamp": get_timestamp(),
            }
            publish_or_queue(MQTT_TOPIC_WARNING, warning, qos=1, message_type="warning")
            full_alert_active = True
        
        #if full_alert_active:
            #print("no warning send again as already send")
            #print(round(fill_level, 1))

        #QoS 0: Keep latest udate while offline as if there is another one the old one becomes invalid
        if last_capacity_sent is None or abs(fill_level - last_capacity_sent) >= PUBLISH_THRESHOLD:
            status = {
                "device": MQTT_CLIENT_ID,
                "bin_id": BIN_ID,
                "servo_angle": pos,
                "fill_level_pct": round(fill_level, 1),
                "cpu_temp_c": round(read_cpu_temp(), 1),
                "uptime_s": now - start_time,
                "timestamp": get_timestamp(),
            }
            sent = publish_or_queue(
                MQTT_TOPIC_STATUS, status, qos=0, message_type="status"
            )
            if sent:
                last_capacity_sent = fill_level

        send_pending(fill_level)

    else:
        distance_history.append(distance)

        if len(distance_history) > HISTORY_SIZE:
            distance_history.pop(0)

        average_distance = sum(distance_history) / len(distance_history)

        if average_distance > 70:
            rotate_servo(servo, 0)
            pos = 0
            lid_open_until = 0
            distance_history = []
            print("Lid auto-closed by distance")

    #heartbeat every 30s
    if (now - last_heartbeat_time) >= HEARTBEAT_INTERVAL_S:
        if mqtt_connected:
            heartbeat = {
                "bin_id": BIN_ID,
                "uptime_s": now - start_time,
                "timestamp": get_timestamp(),
            }
            try:
                client.publish(MQTT_TOPIC_STATUS, json.dumps({"type": "heartbeat"}),qos=0,)
                print("Heartbeat sent")
                last_heartbeat_time = now
            except Exception:
                mqtt_connected = False
                print("Heartbeat failed:", e)

    #testing
    time.sleep(1)  #new
