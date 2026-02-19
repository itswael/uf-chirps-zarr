'use client';

import { useState } from 'react';
import {
  Paper,
  Box,
  Typography,
  ToggleButton,
  ToggleButtonGroup,
  CircularProgress,
} from '@mui/material';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  BarChart,
  Bar,
} from 'recharts';
import appConfig from '@/config/app.config';

interface ChartData {
  time: string[];
  precipitation: (number | null)[];
  units: string;
  aggregation: string;
}

interface PrecipitationChartProps {
  data: ChartData | null;
  loading: boolean;
  aggregation: string;
  onAggregationChange: (aggregation: string) => void;
}

export default function PrecipitationChart({
  data,
  loading,
  aggregation,
  onAggregationChange,
}: PrecipitationChartProps) {
  const handleAggregationChange = (
    event: React.MouseEvent<HTMLElement>,
    newAggregation: string | null
  ) => {
    if (newAggregation !== null) {
      onAggregationChange(newAggregation);
    }
  };

  // Transform data for Recharts
  const chartData = data
    ? data.time.map((time, index) => ({
        date: formatDate(time, aggregation),
        precipitation: data.precipitation[index],
      }))
    : [];

  return (
    <Paper elevation={3} sx={{ p: 2, height: '100%' }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
        <Typography variant="h6">Precipitation Time Series</Typography>
        <ToggleButtonGroup
          value={aggregation}
          exclusive
          onChange={handleAggregationChange}
          size="small"
          aria-label="aggregation level"
        >
          {appConfig.visualization.aggregationLevels.map((level) => (
            <ToggleButton key={level.value} value={level.value} aria-label={level.label}>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                <span>{level.icon}</span>
                <span>{level.label}</span>
              </Box>
            </ToggleButton>
          ))}
        </ToggleButtonGroup>
      </Box>

      {loading ? (
        <Box
          sx={{
            display: 'flex',
            justifyContent: 'center',
            alignItems: 'center',
            height: appConfig.visualization.chart.height,
          }}
        >
          <CircularProgress size={appConfig.ui.loading.spinnerSize} />
        </Box>
      ) : data && chartData.length > 0 ? (
        <ResponsiveContainer width="100%" height={appConfig.visualization.chart.height}>
          {aggregation === 'daily' ? (
            <LineChart
              data={chartData}
              margin={appConfig.visualization.chart.margin}
            >
              <CartesianGrid strokeDasharray="3 3" stroke={appConfig.visualization.colors.gridLines} />
              <XAxis
                dataKey="date"
                tick={{ fontSize: 12 }}
                angle={-45}
                textAnchor="end"
                height={80}
              />
              <YAxis
                label={{ value: data.units, angle: -90, position: 'insideLeft' }}
                tick={{ fontSize: 12 }}
              />
              <Tooltip
                contentStyle={{
                  backgroundColor: 'rgba(255, 255, 255, 0.95)',
                  border: '1px solid #ccc',
                }}
              />
              <Legend />
              <Line
                type="monotone"
                dataKey="precipitation"
                stroke={appConfig.visualization.colors.precipitation}
                strokeWidth={2}
                dot={false}
                name="Precipitation"
              />
            </LineChart>
          ) : (
            <BarChart
              data={chartData}
              margin={appConfig.visualization.chart.margin}
            >
              <CartesianGrid strokeDasharray="3 3" stroke={appConfig.visualization.colors.gridLines} />
              <XAxis
                dataKey="date"
                tick={{ fontSize: 12 }}
                angle={-45}
                textAnchor="end"
                height={80}
              />
              <YAxis
                label={{ value: data.units, angle: -90, position: 'insideLeft' }}
                tick={{ fontSize: 12 }}
              />
              <Tooltip
                contentStyle={{
                  backgroundColor: 'rgba(255, 255, 255, 0.95)',
                  border: '1px solid #ccc',
                }}
              />
              <Legend />
              <Bar
                dataKey="precipitation"
                fill={appConfig.visualization.colors.precipitation}
                name="Precipitation"
              />
            </BarChart>
          )}
        </ResponsiveContainer>
      ) : (
        <Box
          sx={{
            display: 'flex',
            justifyContent: 'center',
            alignItems: 'center',
            height: appConfig.visualization.chart.height,
          }}
        >
          <Typography variant="body1" color="text.secondary">
            Select a location on the map to view precipitation data
          </Typography>
        </Box>
      )}
    </Paper>
  );
}

function formatDate(dateString: string, aggregation: string): string {
  const date = new Date(dateString);
  
  switch (aggregation) {
    case 'daily':
      return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
    case 'weekly':
      return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
    case 'monthly':
      return date.toLocaleDateString('en-US', { year: 'numeric', month: 'short' });
    case 'yearly':
      return date.getFullYear().toString();
    default:
      return dateString;
  }
}
