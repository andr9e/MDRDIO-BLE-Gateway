import requests
import time
import os
import subprocess
import json
import paho.mqtt.client as mqtt #import the mqtt Client

# start sdk console app
subprocess.Popen(['sh',"/home/pi/bin/start_sdk"])
time.sleep(30) # wait 30 seconds for console Dongle Console app to load
broker_address="localhost"

# The callback for when a client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
	print ("Connected with result code "+str (rc))
	client.subscribe("dataTX") #subscribe the data strean topic upon connected

# The callback for when a Published message is received from the server
def on_message(client, userdata, msg):
	selectTagToPub(json.loads(msg.payload)["tagType"],client,msg.payload)

def is_EnvironmentSensor(client,msg):
	print ("environmentSensor detected...")
	timestampUTC = str(json.loads(msg)["timestampUTC"])
	router_mac =str(json.loads(msg)["router_mac"])
	router_lat=float(json.loads(msg)["router_lat"])
	router_long=float(json.loads(msg)["router_long"])
	rssi=int(json.loads(msg)["rssi"])
	Temperature =int(json.loads(msg)["Temperature"])
	Humidity =int(json.loads(msg)["Humidity"])
	Lux = float(json.loads(msg)["VisibleLightPower"])
	uvPower = float(json.loads(msg)["uvPower"])
	pressure=float(json.loads(msg)["Pressure"])
	deviceAddr=str(json.loads(msg)["deviceAddr"])
	MrapFrameCount= int(json.loads(msg)["MrapFrameCount"])
	router_deviceCount=int(json.loads(msg)["router_deviceCount"])
	router_major=int(json.loads(msg)["router_major"])
	router_minor=int(json.loads(msg)["router_minor"])
	#version=int(json.loads(msg)["version"])
	temp=Temperature
	print (type(temp))
	client.publish("EnvironmentSensor/Temperature",Temperature)
	client.publish("EnvironmentSensor/Humidity",Humidity)
	client.publish("EnvironmentSensor/VisibleLightPower",Lux)
	print ("Published Enviornment Temperature Data:"+ Temperature)
	#db.close()

def is_SmartMoistureProbe(client, msg):
	print (msg)
	Index = json.loads(msg)["Index"]
	client.publish("SmartMoistureProbe/Index", Index)

def is_EnvironmentMonitor(client, msg):
	print (msg)
	BatteryVoltage = json.loads(msg)["BatteryVoltage"]
	Temperature = json.loads(msg)["Temperature"]
	Humidity =  json.loads(msg)["Humidity"]
	MAC =  str(json.loads(msg)["deviceAddr"])
	rssi = json.loads(msg)["rssi"]
	client.publish("EnvironmentMonitor/Temperature", Temperature)
	POST_URL= "https://dweet.io:443/dweet/for/"+MAC
	POST_DATA ={"Temperature": Temperature,"Humidity": Humidity,"RSSI": rssi,"BatteryVoltage":BatteryVoltage}
	r = requests.post (url = POST_URL,data = POST_DATA)
	print ("responose web:"+ r.text)

def selectTagToPub(tagType,client,msg):
	tagList[tagType](client,msg)

client = mqtt.Client()

tagList = {
        "EnvironmentSensor": is_EnvironmentSensor,
        "SmartMoistureProbe": is_SmartMoistureProbe,
		"GARDTagV2": is_EnvironmentMonitor
}
client.on_connect = on_connect
client.on_message = on_message
print ("connecting to broker")
client.connect(broker_address) #connect to broker
print ("Publishing message to topic","console/cmd")
postInterval_scanDuration="{\"postInterval\":\"60000\",\"scanDuration\":\"60000\"}"
client.publish("console/cmd",postInterval_scanDuration)
print ("Updated Scan Duration Interval to 1 minutes")
client.loop_forever()
