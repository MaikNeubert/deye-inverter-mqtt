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

import sys
from typing import List
from deye_config import DeyeConfig, DeyeEnv
from deye_plugin_loader import DeyePluginContext
from deye_events import DeyeEventList, DeyeEventProcessor, DeyeObservationEvent
from influxdb import InfluxDBClient

class DeyeInfluxDBConfig:
    def __init__(
        self,
        host='localhost',
        port=8086,
        username='root',
        password='root',
        database=None,
        ssl=False,
        verify_ssl=False,
        timeout=None,
        retries=3,
        use_udp=False,
        udp_port=4444,
        proxies=None,
        pool_size=10,
        path='',
        cert=None,
        gzip=False,
        session=None,
        headers=None,
        socket_options=None,
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.ssl = ssl
        self.verify_ssl = verify_ssl
        self.timeout = timeout
        self.retries = retries
        self.use_udp = use_udp
        self.udp_port = udp_port
        self.proxies = proxies
        self.pool_size = pool_size
        self.path = path
        self.cert = cert
        self.gzip = gzip
        self.session = session
        self.headers = headers
        self.socket_options = socket_options

    @staticmethod
    def from_env():

            
        try:
            return DeyeInfluxDBConfig(
                host=DeyeEnv.string("INFLUX_HOST", 'localhost'),
                port=DeyeEnv.integer("INFLUX_PORT", 8086),
                username=DeyeEnv.string("INFLUX_USERNAME", 'root'),
                password=DeyeEnv.string("INFLUX_PASSWORD", 'root'),
                database=DeyeEnv.string("INFLUX_DATABASE", 'default'),
                ssl=DeyeEnv.boolean("INFLUX_SSL", False),
                verify_ssl=DeyeEnv.boolean("INFLUX_VERIFY_SSL", False),
                timeout=DeyeEnv.integer("INFLUX_TIMEOUT", 10000),
                retries=DeyeEnv.integer("INFLUX_RETRIES", 3),
                use_udp=DeyeEnv.string("INFLUX_USE_UDP", False),
                udp_port=DeyeEnv.integer("INFLUX_UDP_PORT", 4444),
                pool_size=DeyeEnv.integer("INFLUX_POOL_SIZE", 10),
                path=DeyeEnv.string("INFLUX_PATH", ''),
                gzip=DeyeEnv.boolean("INFLUX_GZIP", False),
            )
        except Exception as e:
            print(e)
            sys.exit(1)

class DeyeInfluxDBPublisher(DeyeEventProcessor):


    def __init__(self, config: DeyeConfig):
        self.__config = config
        self.influx_config = DeyeInfluxDBConfig.from_env()
        self.influx_client = InfluxDBClient(
            host=self.influx_config.host,
            port=self.influx_config.port,
            username=self.influx_config.username,
            password=self.influx_config.password,
            database=self.influx_config.database,
            ssl=self.influx_config.ssl,
            verify_ssl=self.influx_config.verify_ssl,
            timeout=self.influx_config.timeout,
            retries=self.influx_config.retries,
            use_udp=self.influx_config.use_udp,
            udp_port=self.influx_config.udp_port,
            proxies=self.influx_config.proxies,
            pool_size=self.influx_config.pool_size,
            path=self.influx_config.path,
            cert=self.influx_config.cert,
            gzip=self.influx_config.gzip,
            session=self.influx_config.session,
            headers=self.influx_config.headers,
            socket_options=self.influx_config.socket_options,
        )

    """An example of custom DeyeEventProcessor implementation
    """
    def get_id(self):
        return "sample_publisher"

    def process(self, events: DeyeEventList):
        print(f"Processing events from logger: {events.logger_index}")

        fields = {}
        for event in events:
            if isinstance(event, DeyeObservationEvent):
                observation_event: DeyeObservationEvent = event

                field_name = observation_event.observation.sensor.name.replace(' ', '_')
                fields[field_name] = observation_event.observation.value

        point = {
            "measurement": "logger",
            "time": int(observation_event.observation.timestamp.timestamp()),
            "fields": fields
        }
        
        print(point)
        self.influx_client.write_points(
            [point]
        )
        

class DeyePlugin:
    """Plugin entrypoint

    The plugin loader first instantiates DeyePlugin class, and then gets event processors from it. 
    """
    def __init__(self, plugin_context: DeyePluginContext):
        """Initializes the plugin
        Args:
            plugin_context (DeyePluginContext): provides access to core service components, e.g. config
        """
        print("test")
        self.publisher = DeyeInfluxDBPublisher(plugin_context.config)

    def get_event_processors(self) -> List[DeyeEventProcessor]:
        """Provides a list of custom event processors 
        """
        print("aasdf")
        return [self.publisher]
