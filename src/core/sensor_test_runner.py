import paho.mqtt.client as mqtt
import numpy as np
from datetime import datetime
import json
import asyncio
from typing import Dict, List, Optional
import logging
from dataclasses import dataclass
import pandas as pd
from scipy import stats

@dataclass
class SensorReading:
    timestamp: float
    value: float
    sensor_id: str
    quality_score: float
    metadata: Dict

class SensorValidator:
    def __init__(self, config: Dict):
        self.moving_window: int = config.get('moving_window', 100)
        self.z_score_threshold: float = config.get('z_score_threshold', 3.0)
        self.min_quality_score: float = config.get('min_quality_score', 0.7)
        self.history: Dict[str, List[float]] = {}
        
    def validate_reading(self, reading: SensorReading) -> tuple[bool, str]:
        """Validates sensor reading using multiple criteria."""
        if reading.sensor_id not in self.history:
            self.history[reading.sensor_id] = []
            
        history = self.history[reading.sensor_id]
        history.append(reading.value)
        
        if len(history) > self.moving_window:
            history.pop(0)
            
        # Statistical validation
        if len(history) >= 3:
            z_score = stats.zscore(history)[-1]
            if abs(z_score) > self.z_score_threshold:
                return False, f"Z-score {z_score:.2f} exceeds threshold"
        
        # Quality score check
        if reading.quality_score < self.min_quality_score:
            return False, f"Quality score {reading.quality_score} below threshold"
            
        return True, "Valid reading"

class SensorDataProcessor:
    def __init__(self):
        self.validator = SensorValidator({
            'moving_window': 100,
            'z_score_threshold': 3.0,
            'min_quality_score': 0.7
        })
        self.logger = logging.getLogger('SensorDataProcessor')
        self.data_buffer: Dict[str, List[SensorReading]] = {}
        
    async def process_reading(self, reading: SensorReading) -> Optional[Dict]:
        """Process and validate a single sensor reading."""
        try:
            # Validate reading
            is_valid, message = self.validator.validate_reading(reading)
            
            if not is_valid:
                self.logger.warning(f"Invalid reading from sensor {reading.sensor_id}: {message}")
                return None
                
            # Store in buffer
            if reading.sensor_id not in self.data_buffer:
                self.data_buffer[reading.sensor_id] = []
            self.data_buffer[reading.sensor_id].append(reading)
            
            # Process buffer if enough readings
            if len(self.data_buffer[reading.sensor_id]) >= 10:
                processed_data = await self._process_buffer(reading.sensor_id)
                self.data_buffer[reading.sensor_id] = []
                return processed_data
                
            return None
            
        except Exception as e:
            self.logger.error(f"Error processing reading: {str(e)}")
            return None
            
    async def _process_buffer(self, sensor_id: str) -> Dict:
        """Process buffered readings for a sensor."""
        readings = self.data_buffer[sensor_id]
        
        # Calculate statistics
        values = [r.value for r in readings]
        timestamps = [r.timestamp for r in readings]
        
        return {
            'sensor_id': sensor_id,
            'timestamp_start': min(timestamps),
            'timestamp_end': max(timestamps),
            'mean': np.mean(values),
            'std': np.std(values),
            'min': min(values),
            'max': max(values),
            'reading_count': len(readings)
        }

class SensorTestHarness:
    def __init__(self, broker_address: str, port: int):
        self.client = mqtt.Client()
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.processor = SensorDataProcessor()
        self.broker_address = broker_address
        self.port = port
        
    def _on_connect(self, client, userdata, flags, rc):
        self.logger.info(f"Connected to MQTT broker with result code {rc}")
        # Subscribe to test sensor topics
        client.subscribe("sensors/water/flow/test/#")
        
    async def _on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload)
            reading = SensorReading(
                timestamp=payload['timestamp'],
                value=payload['value'],
                sensor_id=payload['sensor_id'],
                quality_score=payload['quality_score'],
                metadata=payload.get('metadata', {})
            )
            
            processed_data = await self.processor.process_reading(reading)
            if processed_data:
                await self._handle_processed_data(processed_data)
                
        except json.JSONDecodeError:
            self.logger.error(f"Invalid JSON payload: {msg.payload}")
        except Exception as e:
            self.logger.error(f"Error processing message: {str(e)}")
            
    async def _handle_processed_data(self, data: Dict):
        """Handle processed sensor data."""
        # Log processed data
        self.logger.info(f"Processed data: {json.dumps(data, indent=2)}")
        
        # Simulate blockchain logging
        await self._log_to_blockchain(data)
        
    async def _log_to_blockchain(self, data: Dict):
        """Simulate blockchain logging for testing."""
        # This would be replaced with actual blockchain implementation
        self.logger.info(f"Logging to blockchain: {json.dumps(data, indent=2)}")
        
    async def run_test(self, duration_seconds: int):
        """Run test harness for specified duration."""
        try:
            self.client.connect(self.broker_address, self.port, 60)
            self.client.loop_start()
            
            await asyncio.sleep(duration_seconds)
            
            self.client.loop_stop()
            self.client.disconnect()
            
        except Exception as e:
            self.logger.error(f"Error running test: {str(e)}")
            raise

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run test harness
    harness = SensorTestHarness("localhost", 1883)
    asyncio.run(harness.run_test(300))  # Run for 5 minutes
