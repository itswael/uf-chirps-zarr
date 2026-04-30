'use client';

import { useState, useEffect } from 'react';
import {
  Box,
  Container,
  Grid,
  AppBar,
  Toolbar,
  Typography,
  Paper,
  Drawer,
  IconButton,
  useMediaQuery,
  useTheme,
  Alert,
  Chip,
  Stack,
  TextField,
  Button,
} from '@mui/material';
import { Menu as MenuIcon, Close as CloseIcon, Info } from '@mui/icons-material';
import dynamic from 'next/dynamic';
import Link from 'next/link';
import appConfig from '@/config/app.config';
import { apiClient } from '@/utils/api';
import DateRangeSelector from '@/components/DateRangeSelector';
import PrecipitationChart from '@/components/PrecipitationChart';
import StatisticsPanel from '@/components/StatisticsPanel';
import DownloadPanel from '@/components/DownloadPanel';

// Dynamic import to avoid SSR issues with Leaflet
const MapView = dynamic(() => import('@/components/MapView'), { ssr: false });

export default function Home() {
  const theme = useTheme();
  const isMobile = useMediaQuery(theme.breakpoints.down('md'));
  const [drawerOpen, setDrawerOpen] = useState(!isMobile);

  // State
  const [location, setLocation] = useState<{ lat: number; lon: number; zoom: number } | null>(null);
  const [metadata, setMetadata] = useState<any>(null);
  const [availableVariables, setAvailableVariables] = useState<any>(null);
  const [selectedVariable, setSelectedVariable] = useState<string>('RAIN');
  const [startDate, setStartDate] = useState(appConfig.date.defaultStartDate);
  const [endDate, setEndDate] = useState(appConfig.date.defaultEndDate);
  const [aggregation, setAggregation] = useState(appConfig.visualization.defaultAggregation);
  
  // Coordinate input state
  const [latInput, setLatInput] = useState('');
  const [lonInput, setLonInput] = useState('');
  
  // Data state
  const [chartData, setChartData] = useState<any>(null);
  const [statistics, setStatistics] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [statsLoading, setStatsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Fetch metadata and variables on mount
  useEffect(() => {
    const fetchMetadata = async () => {
      try {
        const meta = await apiClient.getMetadata();
        setMetadata(meta);
        
        // Calculate default dates from metadata
        const maxDateStr = meta.time_range.end.split('T')[0];
        const maxDate = new Date(maxDateStr);
        
        // Set end date to the last day of the month containing maxDate
        const endDateObj = new Date(maxDate.getFullYear(), maxDate.getMonth() + 1, 0);
        const defaultEnd = endDateObj.toISOString().split('T')[0];
        
        // Set start date to the first day of that same month
        const startDateObj = new Date(maxDate.getFullYear(), maxDate.getMonth(), 1);
        const startDateStr = startDateObj.toISOString().split('T')[0];
        
        setEndDate(defaultEnd);
        setStartDate(startDateStr);
        
        // Fetch available variables if NASA POWER is enabled
        if (meta.nasa_power_enabled) {
          const varsData = await apiClient.getVariables();
          setAvailableVariables(varsData.variables);
          setSelectedVariable(varsData.default_plot_variable || 'RAIN');
        }
      } catch (err) {
        console.error('Error fetching metadata:', err);
      }
    };
    fetchMetadata();
  }, []);

  useEffect(() => {
    if (!metadata?.nasa_power_enabled || !startDate || !endDate) {
      return;
    }

    let active = true;

    const preloadWeatherCache = async () => {
      try {
        await apiClient.preloadWeatherCache({
          start_date: startDate,
          end_date: endDate,
        });
      } catch (err) {
        if (active) {
          console.error('Error preloading NASA POWER cache:', err);
        }
      }
    };

    preloadWeatherCache();

    return () => {
      active = false;
    };
  }, [metadata?.nasa_power_enabled, startDate, endDate]);

  // Fetch data when location, dates, aggregation, or variable changes
  useEffect(() => {
    if (location) {
      fetchData();
    }
  }, [location, startDate, endDate, aggregation, selectedVariable]);

  const fetchData = async () => {
    if (!location) return;

    setLoading(true);
    setStatsLoading(true);
    setError(null);

    try {
      // Fetch variable-specific time series data
      const timeSeriesData = await apiClient.getTimeSeriesVariable({
        lat: location.lat,
        lon: location.lon,
        start_date: startDate,
        end_date: endDate,
        variable: selectedVariable,
        aggregation: aggregation === 'daily' ? undefined : aggregation,
      });

      setChartData(timeSeriesData);
      
      // Calculate statistics from time series data
      const values = timeSeriesData.values || timeSeriesData.precipitation || [];
      const numericValues = values
        .filter((v: any) => v !== null && !isNaN(v))
        .map((v: number) => Math.round(v * 10) / 10);
      
      if (numericValues.length > 0) {
        const sortedValues = [...numericValues].sort((a: number, b: number) => a - b);
        const mean = numericValues.reduce((a: number, b: number) => a + b, 0) / numericValues.length;
        const median = sortedValues.length % 2 === 0
          ? (sortedValues[sortedValues.length / 2 - 1] + sortedValues[sortedValues.length / 2]) / 2
          : sortedValues[Math.floor(sortedValues.length / 2)];
        const max = Math.max(...numericValues);
        const min = Math.min(...numericValues);
        const variance = numericValues.reduce((acc: number, val: number) => acc + Math.pow(val - mean, 2), 0) / numericValues.length;
        const std = Math.sqrt(variance);
        
        // For precipitation variables, calculate rain-specific stats
        const isRainVariable = selectedVariable === 'RAIN' || selectedVariable === 'RAIN1';
        const daysWithRain = isRainVariable ? numericValues.filter((v: number) => v >= 0.1).length : 0;
        const dryDays = isRainVariable ? numericValues.filter((v: number) => v < 0.1).length : 0;
        const total = isRainVariable ? numericValues.reduce((a: number, b: number) => a + b, 0) : 0;
        
        // Wet days stats (for precipitation variables only)
        const wetDayValues = isRainVariable ? numericValues.filter((v: number) => v >= 0.1) : numericValues;
        const wetDaysSorted = [...wetDayValues].sort((a: number, b: number) => a - b);
        const wetDaysMean = wetDayValues.length > 0 ? wetDayValues.reduce((a: number, b: number) => a + b, 0) / wetDayValues.length : 0;
        const wetDaysMedian = wetDaysSorted.length % 2 === 0 && wetDaysSorted.length > 0
          ? (wetDaysSorted[wetDaysSorted.length / 2 - 1] + wetDaysSorted[wetDaysSorted.length / 2]) / 2
          : wetDaysSorted[Math.floor(wetDaysSorted.length / 2)] || 0;
        const wetDaysMax = wetDayValues.length > 0 ? Math.max(...wetDayValues) : 0;
        const wetDaysMin = wetDayValues.length > 0 ? Math.min(...wetDayValues) : 0;
        const wetDaysVariance = wetDayValues.length > 0 ? wetDayValues.reduce((acc: number, val: number) => acc + Math.pow(val - wetDaysMean, 2), 0) / wetDayValues.length : 0;
        const wetDaysStd = Math.sqrt(wetDaysVariance);
        
        setStatistics({
          all_days: {
            total_precipitation: total,
            mean_daily: mean,
            median_daily: median,
            max_daily: max,
            min_daily: min,
            std_daily: std,
            days_with_rain: daysWithRain,
            dry_days: dryDays,
          },
          wet_days: {
            total_precipitation: isRainVariable ? wetDayValues.reduce((a: number, b: number) => a + b, 0) : 0,
            mean_daily: wetDaysMean,
            median_daily: wetDaysMedian,
            max_daily: wetDaysMax,
            min_daily: wetDaysMin,
            std_daily: wetDaysStd,
            days_with_rain: wetDayValues.length,
            dry_days: 0,
          },
          is_rain_variable: isRainVariable,
          variable_name: selectedVariable,
          units: timeSeriesData.units || '',
        });
      } else {
        setStatistics(null);
      }
    } catch (err: any) {
      console.error('Error fetching data:', err);
      setError(err.response?.data?.detail || err.message || 'Failed to fetch data');
    } finally {
      setLoading(false);
      setStatsLoading(false);
    }
  };

  const handleLocationSelect = (lat: number, lon: number, zoom: number) => {
    setLocation({ lat, lon, zoom });
    setLatInput(lat.toFixed(4));
    setLonInput(lon.toFixed(4));
  };

  const handleCoordinateSubmit = () => {
    const lat = parseFloat(latInput);
    const lon = parseFloat(lonInput);
    
    if (!isNaN(lat) && !isNaN(lon) && lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180) {
      setLocation({ lat, lon, zoom: location?.zoom || 6 });
    } else {
      alert('Please enter valid coordinates (Lat: -90 to 90, Lon: -180 to 180)');
    }
  };

  const handleToggleDrawer = () => {
    setDrawerOpen(!drawerOpen);
  };

  const sidebarContent = (
    <Box sx={{ width: isMobile ? '100%' : appConfig.ui.sidebarWidth, p: 2 }}>
      <Stack spacing={2}>
        {isMobile && (
          <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <Typography variant="h6">Controls</Typography>
            <IconButton onClick={handleToggleDrawer}>
              <CloseIcon />
            </IconButton>
          </Box>
        )}

        <Paper elevation={2} sx={{ p: 2, bgcolor: 'info.main', color: 'white' }}>
          <Stack direction="row" spacing={1} alignItems="center">
            <Info />
            <Typography variant="subtitle2">
              Click on the map to select a location
            </Typography>
          </Stack>
        </Paper>

        {location && (
          <Paper elevation={2} sx={{ p: 2 }}>
            <Typography variant="subtitle2" gutterBottom>
              Selected Location
            </Typography>
            <Stack spacing={1}>
              <Chip
                label={`Latitude: ${location.lat.toFixed(4)}°`}
                size="small"
                color="primary"
                variant="outlined"
              />
              <Chip
                label={`Longitude: ${location.lon.toFixed(4)}°`}
                size="small"
                color="primary"
                variant="outlined"
              />
              <Chip
                label={`Zoom Level: ${location.zoom}`}
                size="small"
                color="secondary"
                variant="outlined"
              />
            </Stack>
            
            <Typography variant="caption" sx={{ mt: 2, mb: 1, display: 'block' }}>
              Or enter coordinates manually:
            </Typography>
            <Stack spacing={1}>
              <TextField
                label="Latitude"
                value={latInput}
                onChange={(e) => setLatInput(e.target.value)}
                size="small"
                type="number"
                inputProps={{ step: 0.0001, min: -90, max: 90 }}
                fullWidth
              />
              <TextField
                label="Longitude"
                value={lonInput}
                onChange={(e) => setLonInput(e.target.value)}
                size="small"
                type="number"
                inputProps={{ step: 0.0001, min: -180, max: 180 }}
                fullWidth
              />
              <Button
                variant="contained"
                size="small"
                onClick={handleCoordinateSubmit}
                fullWidth
              >
                Apply Coordinates
              </Button>
            </Stack>
          </Paper>
        )}

        <DateRangeSelector
          startDate={startDate}
          endDate={endDate}
          onStartDateChange={setStartDate}
          onEndDateChange={setEndDate}
          minDate={metadata?.time_range?.start?.split('T')[0] || appConfig.date.minDate}
          maxDate={metadata?.time_range?.end?.split('T')[0] || appConfig.date.maxDate}
        />

        <StatisticsPanel statistics={statistics} loading={statsLoading} />

        <DownloadPanel
          location={location}
          startDate={startDate}
          endDate={endDate}
        />
      </Stack>
    </Box>
  );

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', height: '100vh' }}>
      {/* Header */}
      <AppBar position="static" elevation={2}>
        <Toolbar>
          {isMobile && (
            <IconButton
              color="inherit"
              edge="start"
              onClick={handleToggleDrawer}
              sx={{ mr: 2 }}
            >
              <MenuIcon />
            </IconButton>
          )}
          <Typography variant="h6" component="div" sx={{ flexGrow: 1 }}>
            {appConfig.title}
          </Typography>
          <Stack direction="row" spacing={1} sx={{ mr: 2 }}>
            <Button
              component={Link}
              href="/"
              color="inherit"
              variant="outlined"
              sx={{ borderColor: 'rgba(255,255,255,0.5)' }}
            >
              Home
            </Button>
            <Button
              component={Link}
              href="/comparison"
              color="inherit"
              variant="text"
            >
              Data Comparison
            </Button>
          </Stack>
        </Toolbar>
      </AppBar>

      <Box sx={{ display: 'flex', flex: 1, overflow: 'hidden' }}>
        {/* Sidebar */}
        {isMobile ? (
          <Drawer
            anchor="left"
            open={drawerOpen}
            onClose={handleToggleDrawer}
            sx={{
              '& .MuiDrawer-paper': {
                width: '90%',
                maxWidth: 400,
              },
            }}
          >
            {sidebarContent}
          </Drawer>
        ) : (
          <Paper
            elevation={3}
            sx={{
              width: appConfig.ui.sidebarWidth,
              overflow: 'auto',
              borderRadius: 0,
            }}
          >
            {sidebarContent}
          </Paper>
        )}

        {/* Main Content */}
        <Box sx={{ flex: 1, overflow: 'auto', bgcolor: 'background.default' }}>
          <Container maxWidth={false} sx={{ p: 2, height: '100%' }}>
            <Grid container spacing={2} sx={{ height: '100%' }}>
              {/* Map */}
              <Grid item xs={12} lg={6} sx={{ height: { xs: '50vh', lg: '100%' } }}>
                <MapView
                  onLocationSelect={handleLocationSelect}
                  selectedLocation={location ? { lat: location.lat, lon: location.lon } : undefined}
                />
              </Grid>

              {/* Chart */}
              <Grid item xs={12} lg={6} sx={{ height: { xs: 'auto', lg: '100%' } }}>
                {error ? (
                  <Alert severity="error" sx={{ mb: 2 }}>
                    {error}
                  </Alert>
                ) : null}
                <PrecipitationChart
                  data={chartData}
                  loading={loading}
                  aggregation={aggregation}
                  onAggregationChange={setAggregation}
                  selectedVariable={selectedVariable}
                  onVariableChange={setSelectedVariable}
                  availableVariables={availableVariables}
                />
              </Grid>
            </Grid>
          </Container>
        </Box>
      </Box>
    </Box>
  );
}
