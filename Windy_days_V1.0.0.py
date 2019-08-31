#!/usr/bin/env python
"""
Version:1.0.0
Date: 27.06.2019 12:17:00
Author: Kilian Schneiders
Contact: kilian.schneiders@mailbox.org
Python3 script for reading data from HY-WDS9E Weather Station by Modbus-RTU protocol and RS485 physical layer using Raspberry Pi3 as Master.  
"""

import array
import binascii
from collections import OrderedDict
import csv
import datetime
import math
import minimalmodbus
import os
import serial
import struct
import time

#-------------------------------Settings - Configuration mode----------------------------------------
StationName='DAB-1'

Baudrate = 9600
DataBits = 8
Parity = 'E'
StopBit = 1

AddressSlave = 1
AvgPeriodLong = 600         #unit = seconds; range = (1-600)
AvgPeriodShort = 60         #unit = seconds; range = (1-120)
WindSpeedUnit = 0           #unit: 0=m/s; 1=knots; 2=mph; 3=kph; 4=ft/min
GustPeriod = 60             #unit = seconds; range = (1-600)

N_1 = 0 
N_2 = 0

#---------------------------------------Functions----------------------------------------------------
def entersettingmode():
    instrument._communicate('>*\r\n',50)
    time.sleep(0.5)
    return

def configserialport(baud,databit,parity,stopbit):
    instrument._communicate('>CUS ' + str(baud) + ' ' + str(databit) + '-' + str(parity) + '-' + str(stopbit) + '\r\n',100)
    time.sleep(1)
    return

def setaddress(address):
    instrument._communicate('>ID ' + str(address) + '\r\n',100)
    time.sleep(1)
    return
    
def softreset():
    instrument._communicate('>RESET\r\n',100)
    time.sleep(8)
    return

def saveandexit():
    instrument._communicate('>!\r\n',100)
    time.sleep(4)
    return

def setavgtimelong(period):              #Set 0-10 min Wind Speed Direction Average Period                      
    instrument._communicate('>ASDS ' + str(period) + '\r\n',100)    
    time.sleep(0.3)
    return

def setavgtimeshort(period):             #Set 0-2 min Wind Speed Direction Average Period
    instrument._communicate('>ASDM ' + str(period) + '\r\n',100)    
    time.sleep(0.3)
    return

def setwindspeedunit(unit):
    instrument._communicate('>WSUS ' + str(unit) + '\r\n',100)
    time.sleep(0.3)
    return

def setgustperiod(period):
    instrument._communicate('>ASGS ' + str(period) + '\r\n',100)
    time.sleep(0.3)
    return

def getdictionaryRAW(signal,exacttime):
    dictionaryRAW=OrderedDict()
    dictionaryRAW['time']=exacttime
    dictionaryRAW['ByteArray']=signal
    return dictionaryRAW

def getdictionary(signal,exacttime):
    dictionary=OrderedDict()
    dictionary['TimeSinceEpoch']=exacttime
    dictionary['DateTime/UTC']=datetime.datetime.fromtimestamp(exacttime).strftime('%Y-%m-%d %H:%M:%S')
    dictionary['BytesQuantity']=struct.unpack('B',signal[0:1])[0]
    dictionary['DeviceState']=struct.unpack('4s',signal[1:5])[0]
    dictionary['WindDirection']=struct.unpack('>H',signal[5:7])[0]
    dictionary['WindSpeed']=struct.unpack('!f',signal[9:11]+signal[7:9])[0]
    dictionary['Temperature']=struct.unpack('!f',signal[13:15]+signal[11:13])[0]
    dictionary['Humidity']=struct.unpack('!f',signal[17:19]+signal[15:17])[0]
    dictionary['AirPressure']=struct.unpack('!f',signal[21:23]+signal[19:21])[0]
    dictionary['CompassHeading']=struct.unpack('>H',signal[23:25])[0]
    dictionary['PrecepitationType']=struct.unpack('>H',signal[25:27])[0]
    dictionary['PrecipitationIntensity']=struct.unpack('!f',signal[29:31]+signal[27:29])[0]
    dictionary['AccumulatedPrecipitation']=struct.unpack('!f',signal[33:35]+signal[31:33])[0]
    dictionary['IntensityUnit']=struct.unpack('B',signal[35:36])[0]
    dictionary['GPSPositioningStatus']=struct.unpack('>H',signal[37:39])[0]
    dictionary['TravelingSpeed']=struct.unpack('!f',signal[41:43]+signal[39:41])[0]
    dictionary['TravelingDirection']=struct.unpack('>H',signal[43:45])[0]
    dictionary['Longitude']=struct.unpack('!f',signal[47:49]+signal[45:47])[0]
    dictionary['Latitude']=struct.unpack('!f',signal[51:53]+signal[49:51])[0]
    dictionary['PM2.5Concentration']=struct.unpack('!f',signal[55:57]+signal[53:55])[0]
    dictionary['Visibility']=struct.unpack('!f',signal[59:61]+signal[57:59])[0]
    dictionary['RadiationIlluminance']=struct.unpack('!f',signal[63:65]+signal[61:63])[0]
    dictionary['ReservedforInternal']=struct.unpack('!f',signal[67:69]+signal[65:67])[0]
    dictionary['SolarRadiationPower']=struct.unpack('!f',signal[71:73]+signal[69:71])[0]
    dictionary['CompassCorrectedWindDirection']=struct.unpack('!f',signal[75:77]+signal[73:75])[0]
    dictionary['Altitude']=struct.unpack('!f',signal[79:81]+signal[77:79])[0]
    dictionary['GPSCorrectedWindSpeed']=struct.unpack('!f',signal[83:85]+signal[81:83])[0]
    dictionary['AccumulatedSnowThickness']=struct.unpack('!f',signal[87:89]+signal[85:87])[0]
    dictionary['UVRadiation']=struct.unpack('!f',signal[91:93]+signal[89:91])[0]
    dictionary['PM1Concentration']=struct.unpack('!f',signal[95:97]+signal[93:95])[0]
    dictionary['PM10Concentration']=struct.unpack('>f',signal[99:101]+signal[97:99])[0]
    dictionary['ColorTemp']=struct.unpack('>f',signal[103:105]+signal[101:103])[0]
    dictionary[str(AvgPeriodLong)+'secAvgRelWindSpeed']=struct.unpack('>f',signal[107:109]+signal[105:107])[0]
    dictionary[str(AvgPeriodLong)+'secMaxRelWindSpeed']=struct.unpack('>f',signal[111:113]+signal[109:111])[0]
    dictionary[str(AvgPeriodLong)+'secMinRelWindSpeed']=struct.unpack('>f',signal[115:117]+signal[113:115])[0]
    dictionary[str(AvgPeriodLong)+'secRelWindDirection']=struct.unpack('>H',signal[117:119])[0]
    dictionary[str(AvgPeriodLong)+'secMaxRelWindDirection']=struct.unpack('>H',signal[119:121])[0]
    dictionary[str(AvgPeriodLong)+'secMinRelWindDirection']=struct.unpack('>H',signal[121:123])[0]
    dictionary[str(AvgPeriodLong)+'secAvgTrueWindSpeed']=struct.unpack('>f',signal[125:127]+signal[123:125])[0]
    dictionary[str(AvgPeriodLong)+'secMaxTrueWindSpeed']=struct.unpack('>f',signal[129:131]+signal[127:129])[0]
    dictionary[str(AvgPeriodLong)+'secMinTrueWindSpeed']=struct.unpack('>f',signal[133:135]+signal[131:133])[0]
    dictionary[str(AvgPeriodLong)+'secAvgTrueWindDirection']=struct.unpack('>f',signal[137:139]+signal[135:137])[0]
    dictionary[str(AvgPeriodLong)+'secMaxTrueWindDirection']=struct.unpack('>f',signal[141:143]+signal[139:141])[0]
    dictionary[str(AvgPeriodLong)+'secMinTrueWindDirection']=struct.unpack('>f',signal[145:147]+signal[143:145])[0]
    dictionary['Gust3sMaxWindSpeed']=struct.unpack('>f',signal[149:151]+signal[147:149])[0]
    dictionary[str(AvgPeriodShort)+'secAvgRelWindSpeed']=struct.unpack('>f',signal[153:155]+signal[151:153])[0]
    dictionary[str(AvgPeriodShort)+'secMaxRelWindSpeed']=struct.unpack('>f',signal[157:159]+signal[155:157])[0]
    dictionary[str(AvgPeriodShort)+'secMinRelWindSpeed']=struct.unpack('>f',signal[161:163]+signal[159:161])[0]
    dictionary[str(AvgPeriodShort)+'secAvgRelWindDirection']=struct.unpack('>H',signal[163:165])[0]
    dictionary[str(AvgPeriodShort)+'secMaxRelWindDirection']=struct.unpack('>H',signal[165:167])[0]
    dictionary[str(AvgPeriodShort)+'secMinRelWindDirection']=struct.unpack('>H',signal[167:169])[0]
    dictionary[str(AvgPeriodShort)+'secAvgTrueWindSpeed']=struct.unpack('>f',signal[171:173]+signal[169:171])[0]
    dictionary[str(AvgPeriodShort)+'secMaxTrueWindSpeed']=struct.unpack('>f',signal[175:177]+signal[173:175])[0]
    dictionary[str(AvgPeriodShort)+'secMinTrueWindSpeed']=struct.unpack('>f',signal[179:181]+signal[177:179])[0]
    dictionary[str(AvgPeriodShort)+'secAvgTrueWindDirection']=struct.unpack('>f',signal[183:185]+signal[181:183])[0]
    dictionary[str(AvgPeriodShort)+'secMaxTrueWindDirection']=struct.unpack('>f',signal[187:189]+signal[185:187])[0]
    dictionary[str(AvgPeriodShort)+'secMinTrueWindDirection']=struct.unpack('>f',signal[191:193]+signal[189:191])[0]
    return dictionary

#----------------------Communication settings using minimalmodbus library------------------------------
instrument = minimalmodbus.Instrument('/dev/ttyUSB0',1)

instrument.serial.timeout=0.4
instrument.serial.baudrate=9600
instrument.mode=minimalmodbus.MODE_RTU
instrument.serial.parity=serial.PARITY_EVEN

instrument.debug=False

#---------------------------------------Main-----------------------------------------------------------
if __name__=="__main__":
    timestart=time.time()
    
    with open('./Logfile/logfile.txt','a') as logfile:                                                                               #log: system start
        logfile.write(datetime.datetime.fromtimestamp(timestart).strftime('%Y-%m-%d %H:%M:%S')+ '     Start main-section Python script ' + '\n')      

    while True:             #Loop repeating daily
        try:
            
            datestamp = datetime.datetime.now()

            i_var=0
            i_telegram=0
            while os.path.isfile('./MeasuredData/' + StationName + '_' + datestamp.strftime("%Y-%m-%d") + '_' + str(i_var)):       #initialise present files same day, in case electricity loss
                i_var = i_var+1    
    
            signal0=instrument._performCommand(3, '\x00\x00\x00\x60')
            signal0b=bytes(signal0,'Latin1')

            datadict = getdictionary(signal0b,0)
            datadictRAW = getdictionaryRAW(signal0,0)
            print(datadict)    
    
            with open('./MeasuredData/' + StationName + '_' + datestamp.strftime("%Y-%m-%d") + '_' + str(i_var)+'.txt','a') as outfile:      #write headers csv (float values)
                csvfile=csv.DictWriter(outfile,datadict.keys())        
                csvfile.writeheader()
             

            starttime = time.time()
            next_stamp = math.ceil(starttime)
            time.sleep(next_stamp - starttime)                    #make sure next_stamp - time.time() in loop cannot become negative, start at even second
        
        
        
        
            while datestamp.strftime("%Y-%m-%d")==datetime.datetime.now().strftime("%Y-%m-%d"):
                try:
                    #Loop repeating every second
                    timenow=time.time()  
                    signal0=instrument._performCommand(3, '\x00\x00\x00\x60')
                    signal0b=bytes(signal0,'Latin1')
                    signal1=minimalmodbus._hexlify(signal0) 
            
                    datadict = getdictionary(signal0b,timenow)
                    datadictRAW = getdictionaryRAW(repr(signal0),timenow)
            
            
            
                    #print(datadict)
    
                    with open('./MeasuredData/' + StationName + '_' + datestamp.strftime("%Y-%m-%d") + '_' + str(i_var)+'.txt','a') as outfile:
                        csvfile=csv.DictWriter(outfile,datadict.keys())        
                        csvfile.writerow(datadict)
                        
                    with open('./MeasuredData/' + StationName + '_' + datestamp.strftime("%Y-%m-%d") + '_' + str(i_var)+'.raw', "ab") as binary_file:
                        binary_file.write(struct.pack('>d',timenow))
                        binary_file.write(signal0b)
                
                         
                    with open('./MeasuredData/' + StationName + '_' + datestamp.strftime("%Y-%m-%d") + '_'+ str(i_var) + '.hex','a') as outfile:       #only ment to control writing as bytes in Raw
                        outfile.write(signal1 + '\n')
            
            
                    i_telegram = i_telegram + 1
                    if i_telegram >= 60:
                        i_telegram = 0
                        with open('./Telegram/telegram.txt','w') as outfile:
                            csvfile=csv.DictWriter(outfile,datadict.keys())
                            #csvfile.writeheader()
                            csvfile.writerow(datadict)
                
            
                    next_stamp = next_stamp + 1
                    time.sleep(next_stamp - time.time())
              
                except:
                    N_2 = N_2 + 1
                    with open('./Logfile/logfile.txt','a') as logfile:                                                                               #log: system start
                        logfile.write(datetime.datetime.fromtimestamp(timestart).strftime('%Y-%m-%d %H:%M:%S') + '     An unexpected error has been occured'+'\n')
                    time.sleep(1)    
                    #if N_2 > 100:
                        #with open('./Logfile/logfile.txt','a') as logfile:                                                                               #log: system start
                        #    logfile.write(datetime.datetime.fromtimestamp(timestart).strftime('%Y-%m-%d %H:%M:%S') + '     ' + 'Error threshold has been reached: Initialize reboot RaspberryPi')+'\n') 
                                     
#TO DO: implement reboot after 100 errors, fully log kind of error 

        except:
            N_1 = N_1 + 1
            with open('./Logfile/logfile.txt','a') as logfile:                                                                               #log: system start
                logfile.write(datetime.datetime.fromtimestamp(timestart).strftime('%Y-%m-%d %H:%M:%S') +  '     An unexpected error has been occured in the first loop'+'\n') 
               
                     
 
 
 
 
 
#signal1=minimalmodbus._hexlify(signal0)          #-> change signal to hex bytearray with spaces
#signal2=minimalmodbus._hexencode(signal0)        #-> change signal to hex bytearray 
  






















