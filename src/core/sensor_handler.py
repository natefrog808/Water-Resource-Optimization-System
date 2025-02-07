"""
Water Resource Optimization System
File: sensor_handler.py
Purpose: Handles sensor data processing and validation
Author: [Natefrog]
Created: 2025-02-07
"""
import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional
import paho.mqtt.client as mqtt
import numpy as np
from dataclasses import dataclass, asdict
import pandas as pd
from collections import deque
import threading
from concurrent.futures import ThreadPoolExecutor

@dataclass
class SensorConfig:
    sensor_id: str = "water_meter_001"
    z_score_threshold: float = 2.5
    quality_threshold: float = 0.8
    window_size: int = 600  # 10 minutes in seconds
    buffer_size: int = 1000
    data_topic: str = "sensor/water_meter_001/data"
    status_topic: str = "sensor/water_meter_001/status"
    alert_topic: str = "sensor/water_meter_001/alerts"

class PerformanceMonitor:
    def __init__(self):
        self.processing_times = deque(maxlen=1000)
        self.buffer_sizes = deque(maxlen=1000)
        self.error_count = 0
        self.processed_count = 0
        self.last_report_time = datetime.now()
        self.report_interval = 60  # seconds

    def add_processing_time(self, time_ms: float):
        self.processing_times.append(time_ms)
        self.processed_count += 1

    def add_buffer_size(self, size: int):
        self.buffer_sizes.append(size)

    def increment_error(self):
        self.error_count += 1

    def get_stats(self) -> Dict:
        now = datetime.now()
        if len(self.processing_times) > 0:
            avg_processing = np.mean(self.processing_times)
            max_processing = max(self.processing_times)
            p95_processing = np.percentile(self.processing_times, 95)
        else:
            avg_processing = max_processing = p95_processing = 0

        return {
            "timestamp": now.isoformat(),
            "processed_messages": self.processed_count,
            "error_count": self.error_count,
            "avg_processing_time_ms": avg_processing,
            "max_processing_time_ms": max_processing,
            "p95_processing_time_ms": p95_processing,
            "current_buffer_size": len(self.buffer_sizes),
            "error_rate": self.error_count / max(self.processed_count, 1)
        }

class WaterMeterTestRunner:
    def __init__(self, config: SensorConfig):
        self.config = config
        self.monitor = PerformanceMonitor()
        self.data_buffer = deque(maxlen=config.buffer_size)
        self.running = False
        self.logger = logging.getLogger("WaterMeterTest")
        self.setup_logging()
        
        # Initialize MQTT client
        self.client = mqtt.Client()
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect
        
        # Thread pool for async processing
        self.executor = ThreadPoolExecutor(max_workers=4)
        
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f'water_meter_test_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
                logging.StreamHandler()
            ]
        )

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.logger.info("Connected to MQTT broker")
            client.subscribe([
                (self.config.data_topic, 1),
                (self.config.status_topic, 1)
            ])
        else:
            self.logger.error(f"Failed to connect to MQTT broker with code: {rc}")

    def _on_disconnect(self, client, userdata, rc):
        self.logger.warning(f"Disconnected from MQTT broker with code: {rc}")
        if rc != 0:
            self.logger.info("Attempting to reconnect...")
            client.reconnect()

    async def _process_message(self, message) -> Optional[Dict]:
        start_time = datetime.now()
        try:
            payload = json.loads(message.payload)
            
            # Validate message structure
            required_fields = ['timestamp', 'value', 'quality_score']
            if not all(field in payload for field in required_fields):
                raise ValueError(f"Missing required fields: {required_fields}")
            
            # Process data
            self.data_buffer.append(payload['value'])
            if len(self.data_buffer) >= 2:
                z_score = self._calculate_z_score(payload['value'])
                
                # Check thresholds
                if abs(z_score) > self.config.z_score_threshold:
                    await self._publish_alert({
                        "type": "anomaly",
                        "z_score": z_score,
                        "value": payload['value'],
                        "timestamp": payload['timestamp']
                    })
                
                if payload['quality_score'] < self.config.quality_threshold:
                    await self._publish_alert({
                        "type": "quality",
                        "score": payload['quality_score'],
                        "threshold": self.config.quality_threshold,
                        "timestamp": payload['timestamp']
                    })
            
            processing_time = (datetime.now() - start_time).total_seconds() * 1000
            self.monitor.add_processing_time(processing_time)
            return payload
            
        except Exception as e:
            self.logger.error(f"Error processing message: {str(e)}")
            self.monitor.increment_error()
            return None

    def _calculate_z_score(self, value: float) -> float:
        if len(self.data_buffer) < 2:
            return 0
        return (value - np.mean(self.data_buffer)) / np.std(self.data_buffer)

    async def _publish_alert(self, alert_data: Dict):
        try:
            self.client.publish(
                self.config.alert_topic,
                json.dumps(alert_data),
                qos=1
            )
        except Exception as e:
            self.logger.error(f"Error publishing alert: {str(e)}")

    def _on_message(self, client, userdata, message):
        asyncio.run(self._process_message(message))
        
    async def start_test(self, duration_seconds: int):
        self.running = True
        self.logger.info(f"Starting test for {duration_seconds} seconds")
        
        try:
            self.client.connect("localhost", 1883, 60)
            self.client.loop_start()
            
            # Start performance monitoring
            monitor_task = asyncio.create_task(self._monitor_performance())
            
            # Wait for test duration
            await asyncio.sleep(duration_seconds)
            
            self.running = False
            await monitor_task
            
            # Generate final report
            final_stats = self.monitor.get_stats()
            self.logger.info(f"Final test statistics: {json.dumps(final_stats, indent=2)}")
            
        except Exception as e:
            self.logger.error(f"Error during test: {str(e)}")
        finally:
            self.client.loop_stop()
            self.client.disconnect()
            self.executor.shutdown()

    async def _monitor_performance(self):
        while self.running:
            stats = self.monitor.get_stats()
            self.logger.info(f"Performance stats: {json.dumps(stats, indent=2)}")
            await asyncio.sleep(self.monitor.report_interval)

if __name__ == "__main__":
    # Initialize with configuration
    config = SensorConfig()
    runner = WaterMeterTestRunner(config)
    
    # Run test for 1 hour
    asyncio.run(runner.start_test(3600))
