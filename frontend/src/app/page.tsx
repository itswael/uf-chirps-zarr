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

  // Fetch data when location, dates, or aggregation changes
  useEffect(() => {
    if (location) {
      fetchData();
    }
  }, [location, startDate, endDate, aggregation]);

  const fetchData = async () => {
    if (!location) return;

    setLoading(true);
    setStatsLoading(true);
    setError(null);

    try {
      // Calculate spatial extent based on zoom level
      const resolution = appConfig.map.zoomToResolution[location.zoom as keyof typeof appConfig.map.zoomToResolution] || 0.5;
      const extent = resolution / 2;

      const bounds = {
        lon_min: location.lon - extent,
        lon_max: location.lon + extent,
        lat_min: location.lat - extent,
        lat_max: location.lat + extent,
      };

      const dateRange = {
        start_date: startDate,
        end_date: endDate,
      };

      // Fetch time series data
      const timeSeriesPromise = apiClient.getTimeSeries({
        bounds,
        date_range: dateRange,
        aggregation,
      });

      // Fetch statistics
      const statsPromise = apiClient.getStatistics({
        bounds,
        date_range: dateRange,
      });

      const [timeSeriesData, statsData] = await Promise.all([timeSeriesPromise, statsPromise]);

      setChartData(timeSeriesData);
      setStatistics(statsData);
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
          <Typography variant="caption" sx={{ display: { xs: 'none', sm: 'block' } }}>
            v{appConfig.version}
          </Typography>
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
                />
              </Grid>
            </Grid>
          </Container>
        </Box>
      </Box>
    </Box>
  );
}
