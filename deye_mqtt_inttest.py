# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import unittest
import os
import time
from unittest.mock import patch
from datetime import datetime
import paho.mqtt.client as paho

from deye_observation import Observation
from deye_sensors import string_dc_power_sensor
from deye_mqtt import DeyeMqttClient
from deye_config import DeyeConfig, DeyeMqttConfig, DeyeLoggerConfig

import sys
import logging

log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(stream=sys.stdout, format=log_format, level=logging.DEBUG)


class DeyeMqttClientIntegrationTest(unittest.TestCase):

    mqtt_broker_port = 9883

    def __start_broker(self):
        self.mosquitto_pid = os.spawnl(os.P_NOWAIT, '/usr/sbin/mosquitto',
                                       '/usr/sbin/mosquitto', '-p', str(self.mqtt_broker_port))
        time.sleep(2)

    def __stop_broker(self):
        os.kill(self.mosquitto_pid, 9)
        time.sleep(2)

    def __connect_test_client(self):
        self.test_mqtt_client.connect('localhost', port=self.mqtt_broker_port)

    def setUp(self):
        self.received_messages = []
        self.config = DeyeConfig(
            logger_config=DeyeLoggerConfig('123456', '192.168.0.1', 9090),
            mqtt=DeyeMqttConfig('localhost', self.mqtt_broker_port, '', '', 'deye')
        )
        self.test_mqtt_client = paho.Client("test_client")

        def on_message(client, userdata, msg):
            self.received_messages.append(msg)
        self.test_mqtt_client.on_message = on_message

    def tearDown(self):
        self.test_mqtt_client.disconnect()
        self.__stop_broker()

    def test_publish_message(self):
        # given
        self.__start_broker()
        self.__connect_test_client()

        # and
        mqtt = DeyeMqttClient(self.config)

        # and
        timestamp = datetime.now()
        observation = Observation(string_dc_power_sensor, timestamp, 1.2)

        # and
        self.test_mqtt_client.subscribe(f'deye/{string_dc_power_sensor.mqtt_topic_suffix}')

        # when
        self.test_mqtt_client.loop_start()
        mqtt.publish_observation(observation)
        self.test_mqtt_client.loop_stop()

        # and
        mqtt.disconnect()

        # then
        self.assertEqual(len(self.received_messages), 1)

        # and
        received_message = self.received_messages[0]
        self.assertEqual(received_message.topic, 'deye/dc/total_power')
        self.assertEqual(received_message.payload, b'1.2')

    def test_reconnect_on_broker_restart(self):
        # given
        self.__start_broker()

        # and: connect
        mqtt = DeyeMqttClient(self.config)

        # and: restart broker
        self.__stop_broker()
        self.__start_broker()

        # and
        timestamp = datetime.now()
        observation = Observation(string_dc_power_sensor, timestamp, 1.2)

        # and
        self.__connect_test_client()
        self.test_mqtt_client.subscribe(f'deye/{string_dc_power_sensor.mqtt_topic_suffix}')

        # when
        self.test_mqtt_client.loop_start()
        mqtt.publish_observation(observation)
        self.test_mqtt_client.loop_stop()

        # and
        mqtt.disconnect()

        # then
        self.assertEqual(len(self.received_messages), 1)

        # and
        received_message = self.received_messages[0]
        self.assertEqual(received_message.topic, 'deye/dc/total_power')
        self.assertEqual(received_message.payload, b'1.2')


if __name__ == '__main__':
    unittest.main()
