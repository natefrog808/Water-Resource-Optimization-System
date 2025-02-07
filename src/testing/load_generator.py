"""
Water Resource Optimization System
File: sensor_handler.py
Purpose: Handles sensor data processing and validation
Author: [natefrog]
Created: 2025-02-07
"""
import asyncio
import json
import random
import time
from datetime import datetime
import paho.mqtt.client as mqtt
import numpy as np

class LoadTestGenerator:
    def __init__(self, broker="localhost", port=1883):
        self.client = mqtt.Client()
        self.broker = broker
        self.port = port
        self.running = False
        self.base_flow_rate = 50.0
        self.noise_factor = 0.1
        
    async def start(self, duration_seconds: int = 300):
        """Run load test for specified duration"""
        print(f"Starting load test for {duration_seconds} seconds...")
        self.client.connect(self.broker, self.port)
        self.client.loop_start()
        self.running = True
        
        try:
            start_time = time.time()
            while time.time() - start_time < duration_seconds:
                await self._generate_test_data()
                await asyncio.sleep(0.1)  # 10 messages per second
                
        except Exception as e:
            print(f"Error during load test: {e}")
        finally:
            self.running = False
            self.client.loop_stop()
            self.client.disconnect()
            
    async def _generate_test_data(self):
        """Generate test data with various patterns"""
        timestamp = datetime.utcnow().isoformat() + "Z"
        
        # More aggressive test pattern
        message_type = np.random.choice([
            "normal",
            "anomaly",
            "noise",
            "missing_data",
            "corrupt_data",
            "burst"  # Added burst type
        ], p=[0.5, 0.15, 0.15, 0.1, 0.05, 0.05])  # Adjusted probabilities
        
        # Simulate periodic data bursts
        if message_type == "burst":
            # Generate burst of 50 messages in quick succession
            for _ in range(50):
                burst_value = self.base_flow_rate * random.uniform(0.5, 1.5)
                burst_message = {
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                    "value": burst_value,
                    "quality_score": random.uniform(0.7, 0.9),
                    "sensor_id": "water_meter_001",
                    "type": "burst"
                }
                self.client.publish(
                    "sensor/water_meter_001/data",
                    json.dumps(burst_message),
                    qos=1
                )
        
        if message_type == "normal":
            value = self.base_flow_rate + np.random.normal(0, self.noise_factor * self.base_flow_rate)
            quality_score = random.uniform(0.8, 1.0)
            
        elif message_type == "anomaly":
            # Generate significant deviation
            # More extreme anomalies
            value = self.base_flow_rate * random.uniform(8, 15)  # Increased range
            # Occasionally inject rapid anomaly sequences
            if random.random() < 0.2:  # 20% chance of anomaly sequence
                for _ in range(5):
                    spike_value = self.base_flow_rate * random.uniform(5, 20)
                    self.client.publish(
                        "sensor/water_meter_001/data",
                        json.dumps({
                            "timestamp": datetime.utcnow().isoformat() + "Z",
                            "value": spike_value,
                            "quality_score": random.uniform(0.8, 1.0),
                            "sensor_id": "water_meter_001",
                            "type": "anomaly_sequence"
                        }),
                        qos=1
                    )
            quality_score = random.uniform(0.8, 1.0)
            
        elif message_type == "noise":
            # Generate noisy but valid data
            value = self.base_flow_rate + np.random.normal(0, self.base_flow_rate)
            quality_score = random.uniform(0.6, 0.8)
            
        elif message_type == "missing_data":
            value = None
            quality_score = 0.0
            
        else:  # corrupt_data
            value = "invalid_value"
            quality_score = random.uniform(0, 0.5)
            
        message = {
            "timestamp": timestamp,
            "value": value,
            "quality_score": quality_score,
            "sensor_id": "water_meter_001",
            "type": message_type
        }
        
        try:
            self.client.publish(
                "sensor/water_meter_001/data",
                json.dumps(message),
                qos=1
            )
            print(f"Published {message_type} message: {json.dumps(message)}")
            
        except Exception as e:
            print(f"Error publishing message: {e}")

if __name__ == "__main__":
    generator = LoadTestGenerator()
    asyncio.run(generator.start(300))  # Run for 5 minutes
