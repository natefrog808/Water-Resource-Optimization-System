"""
Water Resource Optimization System
File: sensor_handler.py
Purpose: Handles sensor data processing and validation
Author: [natefrog]
Created: 2025-02-07
"""
import asyncio
import time
from collections import deque
import numpy as np
from datetime import datetime
import logging
import json

class PerformanceMonitor:
    def __init__(self):
        self.buffer_sizes = deque(maxlen=1000)
        self.processing_times = deque(maxlen=1000)
        self.error_counts = deque(maxlen=1000)
        self.anomaly_counts = deque(maxlen=1000)
        self.last_alert = None
        self.alert_cooldown = 5  # seconds
        
        # Configure logging
        self.logger = logging.getLogger('PerformanceMonitor')
        handler = logging.FileHandler(f'performance_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

    async def start_monitoring(self):
        while True:
            try:
                stats = self.get_current_stats()
                self.check_thresholds(stats)
                self.log_stats(stats)
                await asyncio.sleep(1)
            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")

    def add_metric(self, metric_type, value):
        timestamp = time.time()
        if metric_type == 'buffer_size':
            self.buffer_sizes.append((timestamp, value))
        elif metric_type == 'processing_time':
            self.processing_times.append((timestamp, value))
        elif metric_type == 'error_count':
            self.error_counts.append((timestamp, value))
        elif metric_type == 'anomaly_count':
            self.anomaly_counts.append((timestamp, value))

    def get_current_stats(self):
        now = time.time()
        window = 10  # 10 second window for calculations
        
        # Filter metrics within window
        recent_buffer = [(t, v) for t, v in self.buffer_sizes if now - t <= window]
        recent_processing = [(t, v) for t, v in self.processing_times if now - t <= window]
        recent_errors = [(t, v) for t, v in self.error_counts if now - t <= window]
        recent_anomalies = [(t, v) for t, v in self.anomaly_counts if now - t <= window]

        return {
            'timestamp': datetime.now().isoformat(),
            'buffer_size': {
                'current': recent_buffer[-1][1] if recent_buffer else 0,
                'max': max([v for _, v in recent_buffer]) if recent_buffer else 0,
                'avg': np.mean([v for _, v in recent_buffer]) if recent_buffer else 0
            },
            'processing_time': {
                'current': recent_processing[-1][1] if recent_processing else 0,
                'max': max([v for _, v in recent_processing]) if recent_processing else 0,
                'avg': np.mean([v for _, v in recent_processing]) if recent_processing else 0,
                'p95': np.percentile([v for _, v in recent_processing], 95) if recent_processing else 0
            },
            'error_rate': len(recent_errors) / window if recent_errors else 0,
            'anomaly_rate': len(recent_anomalies) / window if recent_anomalies else 0
        }

    def check_thresholds(self, stats):
        now = time.time()
        
        # Only alert if cooldown period has passed
        if self.last_alert and now - self.last_alert < self.alert_cooldown:
            return

        alerts = []
        
        # Check processing time
        if stats['processing_time']['current'] > 120:
            alerts.append(f"High processing time: {stats['processing_time']['current']:.2f}ms")
        
        # Check buffer size
        if stats['buffer_size']['current'] > 800:
            alerts.append(f"Buffer approaching capacity: {stats['buffer_size']['current']}/1000")
        
        # Check error rate
        if stats['error_rate'] > 0.02:
            alerts.append(f"High error rate: {stats['error_rate']*100:.2f}%")

        if alerts:
            self.last_alert = now
            alert_msg = " | ".join(alerts)
            self.logger.warning(f"ALERT: {alert_msg}")
            print(f"\033[91mALERT: {alert_msg}\033[0m")  # Red color in terminal

    def log_stats(self, stats):
        self.logger.info(json.dumps(stats))
        
        # Print summary to console
        print(f"\033[94m=== Performance Summary ===\033[0m")  # Blue color
        print(f"Buffer Size: {stats['buffer_size']['current']}/{1000}")
        print(f"Processing Time: {stats['processing_time']['current']:.2f}ms")
        print(f"Error Rate: {stats['error_rate']*100:.2f}%")
        print(f"Anomaly Rate: {stats['anomaly_rate']*100:.2f}%")
        print("-" * 30)

async def main():
    monitor = PerformanceMonitor()
    await monitor.start_monitoring()

if __name__ == "__main__":
    asyncio.run(main())
