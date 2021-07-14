import mysql.connector
import time
import os
import subprocess
import json
import paho.mqtt.client as mqtt #import the mqtt Client

# start sdk console app
subprocess.Popen(['sh',"/home/pi/bin/net472/start_sdk"])
time.sleep(15) # wait 15 seconds for console Dongle Console app to load
broker_address="localhost"

# The callback for when a client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
	print ("Connected with result code "+str (rc))
	client.subscribe("dataTX") #subscribe the data strean topic upon connected

# The callback for when a Published message is received from the server
def on_message(client, userdata, msg):
	print ("inside on_message function")
	selectTagToPub(json.loads(msg.payload)["tagType"],client,msg.payload)

def is_EnvironmentSensor(client,msg):
	print ("inside is_environmentSensor")
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
	#db=mysql.connector.connect(host="db-mdrdio-test.cei6tu7boufa.us-east-1.rds.amazonaws.com",port=3300,user="admin",passwd="hemlock2",db="modern_radio_sensors")	
	#if (db):
	#       print ("connection was successfull")
                #db.close()
	#else:
	#       print ("connection failed")
	#cursor = db.cursor()
	#sql ""INSERT INTO environmentsensors (router_mac,rssi,Temperature,Humidity,deviceAddr,router_lat,router_long,VisibleLightPower,Pressure,MrapFrameCount,uvPower,router_deviceCount,router_major,router_minor,timestampUTC) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
	#data=(router_mac,rssi,Temperature,Humidity,deviceAddr,router_lat,router_long,Lux,pressure,MrapFrameCount,uvPower,router_deviceCount,router_major,router_minor,timestampUTC)
	#cursor.execute(sql,data)
	#db.commit()
	#print (cursor.rowcount, "record inserted.")
	client.publish("EnvironmentSensor/Temperature",Temperature)
	client.publish("EnvironmentSensor/Humidity",Humidity)
	client.publish("EnvironmentSensor/VisibleLightPower",Lux)
	print ("Published Enviornment Temperature Data:"+ Temperature)
	#db.close()

def is_SmartMoistureProbe(client, msg):
	Index = json.loads(msg)["Index"]
	client.publish("SmartMoistureProbe/Index", Index)

def selectTagToPub(tagType,client,msg):
	tagList[tagType](client,msg)

client = mqtt.Client()

tagList = {
        "EnvironmentSensor": is_EnvironmentSensor,
        "SmartMoistureProbe": is_SmartMoistureProbe
}
client.on_connect = on_connect
client.on_message = on_message
print ("connecting to broker")
client.connect(broker_address) #connect to broker
print ("Publishing message to topic","console/cmd")
postInterval_scanDuration="{\"postInterval\":\"120000\",\"scanDuration\":\"120000\"}"
client.publish("console/cmd",postInterval_scanDuration)
client.loop_forever()
