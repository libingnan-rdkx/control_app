import json
# import struct as st
import pandas as pd
import numpy as np
import paho.mqtt.client as mqtt
from datetime import datetime, timedelta

from influxdb import InfluxDBClient

client = InfluxDBClient('192.168.1.56', 8086, 'root', 'root', 'apc_db')

jianwen_var_names = (['AI'+str(i)+'_APC' for i in range(33,45)] +
    ['AI'+str(i)+'_APC' for i in range(49,59)])
xietiao_var_names = ['AI'+str(i)+'_APC' for i in range(1,33)]
tuoxiao_var_names = ['AI'+str(i)+'_APC' for i in range(61,75)]
es_var_names = ['AO'+str(i)+'_APC' for i in range(17,26)]

class MqttMessageBus():
    def __init__(self):
        self.mqttc = mqtt.Client(client_id='save_data_periodically')
        self.counter = 0
        self.saveDataOn = 0
        self.flag = True
        self.duration = 480 # minutes

    def on_connect(self, mqttc, obj, flags, rc):
        print("Client is connected to mqtt broker, rc: " + str(rc))

    def on_message(self, mqttc, obj, msg):
        dcsmsg = json.loads(msg.payload.decode('utf-8'))
        self.saveDataOn = self.get_bit_val(dcsmsg['DI3'], 15)
        if self.saveDataOn:
            now = datetime.utcnow()
            minutes_num = now.hour * 60 + now.minute
            if self.flag and minutes_num % self.duration == 0:
                print('save data:', now + timedelta(hours=8))
                self.saveData(now)
                self.flag = False
            if minutes_num % self.duration == 1:
                self.flag = True
        
    def on_publish(self, mqttc, obj, mid):
        print("on_publish, mid: " + str(mid))

    def on_subscribe(self, mqttc, obj, mid, granted_qos):
        print("on_subscribe: " + str(mid) + " " + str(granted_qos))

    def on_log(self, mqttc, obj, level, string):
        print('on_log:', string)

    def connect(self, host, port=1883, keepalive=60):
        self.mqttc.on_connect = self.on_connect
        self.mqttc.on_message = self.on_message
        # self.mqttc.on_publish = self.on_publish
        self.mqttc.on_subscribe = self.on_subscribe
        # Uncomment to enable debug messages
        # mqttc.on_log = on_log
        self.mqttc.connect(host, port, keepalive)

    def subscribe(self, topic, qos=0):
        self.mqttc.subscribe(topic, qos)
        self.mqttc.loop_forever()

    def saveData(self, now):
        self.saveDataToCSV(jianwen_var_names, now, 'dcs_data', 'jianwen')
        self.saveDataToCSV(xietiao_var_names, now, 'dcs_data', 'xietiao')
        self.saveDataToCSV(tuoxiao_var_names, now, 'dcs_data', 'tuoxiao')
        self.saveDataToCSV(es_var_names, now, 'command_data', 'exci_sig')

    def saveDataToCSV(self, var_names, now, measurement, fnsuffix):
        datetimestr = []
        col_data = []
        var_str = ','.join(var_names)
        where_str = "where time>=$startTime and time<=$endTime"
        query = "select {} from {} {}".format(var_str, measurement, where_str)
        
        timeZone = 8 # 8 hours
        duration = self.duration
        endTimeStamp = now
        startTimeStamp = endTimeStamp - timedelta(minutes=duration)
        startTime = startTimeStamp.strftime('%Y-%m-%dT%H:%M:%SZ')
        endTime = endTimeStamp.strftime('%Y-%m-%dT%H:%M:%SZ')
        startTime = startTime[:17]+'00Z'
        endTime = endTime[:17]+'00Z'

        # startTime = '2020-06-11T12:00:00Z'
        # endTime = '2020-06-12T00:00:00Z'

        bind_params = {'startTime': startTime, 'endTime': endTime}
        results = client.query(query, bind_params=bind_params).get_points() # result is an iterator

        for rs in results:
            datetimestr.append(rs['time'])
            for name in var_names:
                col_data.append(round(rs[name]))

        data = np.array(col_data).reshape(-1, len(var_names))

        df = pd.DataFrame(data,columns=var_names, index=datetimestr)
        print('data length',len(data))
        st = (startTimeStamp + timedelta(hours=timeZone)).strftime('%Y%m%d-%H%M%S')
        et = (endTimeStamp + timedelta(hours=timeZone)).strftime('%Y%m%d-%H%M%S')
        fn = '/home/czyd/data/{}_{}_{}.csv'.format(st, et, fnsuffix)
        # fn = '{}_{}_{}.csv'.format(st, et, fnsuffix)
        df.to_csv(fn, sep=',', header=True, index=True)

    def get_bit_val(self, byte, index):
        """
        get the bit value of an integer at index from right to left starting from 0
        """
        if byte & (1 << index):
            return 1
        else:
            return 0

if __name__ == "__main__":
    mmb = MqttMessageBus()
    mqtthost = '192.168.1.56'
    port = 1883
    keepalive = 60
    mmb.connect(mqtthost, port, keepalive)
    mmb.subscribe('apcdata')
    