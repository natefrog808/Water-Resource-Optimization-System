"""
Water Resource Optimization System
File: sensor_handler.py
Purpose: Handles sensor data processing and validation
Author: [natefrog]
Created: 2025-02-07
"""
import React, { useState, useEffect } from 'react';
import { LineChart, Line, AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Droplet, AlertTriangle, TrendingUp } from 'lucide-react';

const WaterResourceDashboard = () => {
  const [data, setData] = useState([]);
  const [alerts, setAlerts] = useState([]);

  useEffect(() => {
    // Simulate real-time data updates
    const generateData = () => {
      const newData = [];
      const now = new Date();
      
      for (let i = 0; i < 24; i++) {
        const time = new Date(now.getTime() - (23 - i) * 3600000);
        newData.push({
          timestamp: time.toLocaleTimeString(),
          consumption: Math.random() * 100 + 50,
          availability: Math.random() * 150 + 100,
          efficiency: Math.random() * 20 + 80,
          renewable: Math.random() * 40 + 30
        });
      }
      setData(newData);
      
      // Generate alerts based on thresholds
      const latestData = newData[newData.length - 1];
      const newAlerts = [];
      
      if (latestData.consumption > latestData.availability * 0.8) {
        newAlerts.push("High consumption alert: Approaching available capacity");
      }
      if (latestData.efficiency < 85) {
        newAlerts.push("Efficiency drop detected: Check distribution network");
      }
      setAlerts(newAlerts);
    };

    generateData();
    const interval = setInterval(generateData, 5000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="w-full space-y-4 p-4">
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Droplet className="h-5 w-5" />
              Water Consumption vs Availability
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={data}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="timestamp" />
                  <YAxis />
                  <Tooltip />
                  <Legend />
                  <Area type="monotone" dataKey="consumption" stroke="#8884d8" fill="#8884d8" fillOpacity={0.3} name="Consumption" />
                  <Area type="monotone" dataKey="availability" stroke="#82ca9d" fill="#82ca9d" fillOpacity={0.3} name="Availability" />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <TrendingUp className="h-5 w-5" />
              System Efficiency
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={data}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="timestamp" />
                  <YAxis />
                  <Tooltip />
                  <Legend />
                  <Line type="monotone" dataKey="efficiency" stroke="#ff7300" name="Efficiency %" />
                  <Line type="monotone" dataKey="renewable" stroke="#82ca9d" name="Renewable %" />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </CardContent>
        </Card>
      </div>

      {alerts.length > 0 && (
        <div className="space-y-2">
          {alerts.map((alert, index) => (
            <Alert key={index} variant="destructive">
              <AlertTriangle className="h-4 w-4" />
              <AlertDescription>{alert}</AlertDescription>
            </Alert>
          ))}
        </div>
      )}
    </div>
  );
};

export default WaterResourceDashboard;
