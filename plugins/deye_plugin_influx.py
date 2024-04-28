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
from influxdb_client import InfluxDBClient, Point
from influxdb_client .client.write_api import SYNCHRONOUS

class DeyeInfluxDBConfig:
    def __init__(
        self,
        database=None,
    ):
        self.database = database

    @staticmethod
    def from_env():
        try:
            return DeyeInfluxDBConfig(
                database=DeyeEnv.string("INFLUX_DATABASE", 'default'),
            )
        except Exception as e:
            print(e)
            sys.exit(1)

class DeyeInfluxDBPublisher(DeyeEventProcessor):


    def __init__(self, config: DeyeConfig):
        self.__config = config
        self.influx_config = DeyeInfluxDBConfig.from_env()
        self.influx_client = InfluxDBClient.from_env_properties()

    """An example of custom DeyeEventProcessor implementation
    """
    def get_id(self):
        return "sample_publisher"

    def process(self, events: DeyeEventList):
        print(f"Processing events from logger: {events.logger_index}")

        point = Point("deye_wr")
        for event in events:
            if isinstance(event, DeyeObservationEvent):
                observation_event: DeyeObservationEvent = event
                field = observation_event.observation.sensor.name.replace(' ', '_')
                point.field(field, observation_event.observation.value)
        
        write_api = self.influx_client.write_api(write_options=SYNCHRONOUS)
        write_api.write(bucket=self.influx_config.database, record=point)

       

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
