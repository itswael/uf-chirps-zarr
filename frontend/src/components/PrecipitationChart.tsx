'use client';

import { useMemo } from 'react';
import {
  Paper,
  Box,
  Typography,
  Link,
  ToggleButton,
  ToggleButtonGroup,
  CircularProgress,
  Select,
  MenuItem,
  FormControl,
  InputLabel,
  SelectChangeEvent,
} from '@mui/material';
import {
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  BarChart,
  Bar,
  LineChart,
  Line,
} from 'recharts';
import appConfig from '@/config/app.config';
import { generateWeatherSummaryWithSources } from '@/utils/weatherSummary';

interface ChartData {
  time: string[];
  precipitation?: (number | null)[];
  values?: (number | null)[];
  variable?: string;
  units: string;
  aggregation: string;
  description?: string;
}

interface PrecipitationChartProps {
  data: ChartData | null;
  loading: boolean;
  aggregation: string;
  onAggregationChange: (aggregation: string) => void;
  selectedVariable?: string;
  onVariableChange?: (variable: string) => void;
  availableVariables?: any;
}

export default function PrecipitationChart({
  data,
  loading,
  aggregation,
  onAggregationChange,
  selectedVariable = 'RAIN1',
  onVariableChange,
  availableVariables,
}: PrecipitationChartProps) {
  const handleAggregationChange = (
    event: React.MouseEvent<HTMLElement>,
    newAggregation: string | null
  ) => {
    if (newAggregation !== null) {
      onAggregationChange(newAggregation);
    }
  };

  const handleVariableChange = (event: SelectChangeEvent) => {
    if (onVariableChange) {
      onVariableChange(event.target.value);
    }
  };

  // Transform data for Recharts
  const chartData = data
    ? data.time.map((time, index) => ({
        date: formatDate(time, aggregation),
        value: data.values ? data.values[index] : data.precipitation?.[index],
      }))
    : [];

  // Determine if we should use bar or line chart
  const useLineChart = selectedVariable !== 'RAIN1' && selectedVariable !== 'RAIN2' && selectedVariable !== 'RAIN';
  
  // Get display name and color for variable
  const getVariableDisplay = () => {
    if (!selectedVariable || !availableVariables) {
      return { name: 'Precipitation', color: appConfig.visualization.colors.precipitation };
    }
    
    const varInfo = availableVariables[selectedVariable];
    if (!varInfo) {
      return { name: selectedVariable, color: appConfig.visualization.colors.precipitation };
    }
    
    // Map variables to colors
    const colorMap: { [key: string]: string } = {
      'RAIN1': '#2196f3',
      'RAIN2': '#1976d2', 
      'TMAX': '#f44336',
      'TMIN': '#2196f3',
      'T2M': '#ff9800',
      'SRAD': '#ffc107',
      'WIND': '#9c27b0',
      'TDEW': '#00bcd4',
      'RH2M': '#4caf50',
    };
    
    return {
      name: varInfo.description || selectedVariable,
      color: colorMap[selectedVariable] || appConfig.visualization.colors.precipitation
    };
  };

  const { name: variableName, color: variableColor } = getVariableDisplay();
  const rawValues = data ? (data.values || data.precipitation || []) : [];

  const summaryResult = useMemo(
    () =>
      generateWeatherSummaryWithSources({
        selectedVariable,
        variableName,
        units: data?.units || '',
        aggregation,
        values: rawValues,
      }),
    [selectedVariable, variableName, data?.units, aggregation, rawValues]
  );

  return (
    <Paper elevation={3} sx={{ p: 2, height: '100%' }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2, flexWrap: 'wrap', gap: 1 }}>
        <Typography variant="h6">Weather Data Time Series</Typography>
        
        <Box sx={{ display: 'flex', gap: 1, alignItems: 'center', flexWrap: 'wrap' }}>
          {/* Variable Selector */}
          {availableVariables && Object.keys(availableVariables).length > 0 && (
            <FormControl size="small" sx={{ minWidth: 180 }}>
              <InputLabel>Variable</InputLabel>
              <Select
                value={selectedVariable}
                onChange={handleVariableChange}
                label="Variable"
              >
                {Object.keys(availableVariables).map((varCode) => (
                  <MenuItem key={varCode} value={varCode}>
                    {availableVariables[varCode].description || varCode}
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          )}
          
          {/* Aggregation Selector */}
          <ToggleButtonGroup
            value={aggregation}
            exclusive
            onChange={handleAggregationChange}
            size="small"
            aria-label="aggregation level"
            sx={{ flexShrink: 0 }}
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
        <>
          <ResponsiveContainer width="100%" height={appConfig.visualization.chart.height}>
            {useLineChart ? (
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
                  dataKey="value"
                  stroke={variableColor}
                  strokeWidth={2}
                  dot={{ r: 3 }}
                  name={variableName}
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
                  dataKey="value"
                  fill={variableColor}
                  name={variableName}
                />
              </BarChart>
            )}
          </ResponsiveContainer>

          <Box
            sx={{
              mt: 2,
              px: 1.5,
              py: 1.25,
              borderRadius: 1,
              bgcolor: 'grey.50',
              border: '1px solid',
              borderColor: 'divider',
            }}
          >
            <Typography
              variant="body2"
              color="text.secondary"
              sx={{ lineHeight: 1.65, whiteSpace: 'pre-line' }}
            >
              {summaryResult.summary}
            </Typography>
            {summaryResult.sources.length > 0 && (
              <Box sx={{ mt: 1 }}>
                <Typography variant="caption" color="text.secondary" sx={{ display: 'block', mb: 0.5 }}>
                  Metric references:
                </Typography>
                {summaryResult.sources.map((source) => (
                  <Typography key={source.url} variant="caption" sx={{ display: 'block' }}>
                    <Link href={source.url} target="_blank" rel="noopener noreferrer">
                      {source.label}
                    </Link>
                  </Typography>
                ))}
              </Box>
            )}
          </Box>
        </>
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
            Select a location on the map to view weather data
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
