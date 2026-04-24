'use client';

import { useEffect, useMemo, useState } from 'react';
import {
  AppBar,
  Alert,
  Box,
  Button,
  Card,
  CardContent,
  Chip,
  Container,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  FormControl,
  Grid,
  InputLabel,
  MenuItem,
  Paper,
  Select,
  SelectChangeEvent,
  Stack,
  TextField,
  Toolbar,
  Typography,
  CircularProgress,
} from '@mui/material';
import {
  Download,
  LocationOn,
} from '@mui/icons-material';
import Link from 'next/link';
import dynamic from 'next/dynamic';
import axios from 'axios';
import dayjs, { Dayjs } from 'dayjs';
import { DatePicker } from '@mui/x-date-pickers/DatePicker';
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';

import appConfig from '@/config/app.config';
import { apiClient } from '@/utils/api';

const MapView = dynamic(() => import('@/components/MapView'), { ssr: false });

type SourceKey = 'chirps' | 'nasa_s3' | 'nasa_api' | 'open_meteo';
type VariableKey = 'RAIN' | 'T2M' | 'TMIN' | 'TMAX' | 'TDEW' | 'RH2M' | 'WIND' | 'SRAD';

type TimeSeriesRecord = {
  date: string;
  value: number | null;
};

type BasicStats = {
  availableCount: number;
  missingCount: number;
  min: number | null;
  max: number | null;
  mean: number | null;
  sum: number;
};

type DifferenceRow = {
  date: string;
  source1: number;
  source2: number;
  diff: number;
  absDiff: number;
};

type ComparisonPoint = {
  date: string;
  displayDate: string;
  source1: number;
  source2: number;
};

const VARIABLE_OPTIONS: Array<{ key: VariableKey; label: string; units: string }> = [
  { key: 'RAIN', label: 'Rainfall', units: 'mm/day' },
  { key: 'T2M', label: 'Average Temperature (T2M)', units: 'degC' },
  { key: 'TMIN', label: 'Minimum Temperature', units: 'degC' },
  { key: 'TMAX', label: 'Maximum Temperature', units: 'degC' },
  { key: 'TDEW', label: 'Dew Point Temperature', units: 'degC' },
  { key: 'RH2M', label: 'Relative Humidity', units: '%' },
  { key: 'WIND', label: 'Wind Speed', units: 'm/s' },
  { key: 'SRAD', label: 'Solar Radiation', units: 'MJ/m2/day' },
];

const SOURCE_OPTIONS: Array<{ key: SourceKey; label: string }> = [
  { key: 'chirps', label: 'CHIRPS (Zarr)' },
  { key: 'nasa_s3', label: 'NASA POWER S3' },
  { key: 'nasa_api', label: 'NASA POWER API' },
  { key: 'open_meteo', label: 'Open-Meteo' },
];

const VARIABLE_SOURCE_MAP: Record<VariableKey, SourceKey[]> = {
  RAIN: ['chirps', 'nasa_s3', 'nasa_api', 'open_meteo'],
  T2M: ['nasa_s3', 'nasa_api'],
  TMIN: ['nasa_s3', 'nasa_api'],
  TMAX: ['nasa_s3', 'nasa_api'],
  TDEW: ['nasa_s3', 'nasa_api'],
  RH2M: ['nasa_s3', 'nasa_api'],
  WIND: ['nasa_s3', 'nasa_api'],
  SRAD: ['nasa_s3', 'nasa_api'],
};

const NASA_API_PARAM_MAP: Record<VariableKey, string> = {
  RAIN: 'PRECTOTCORR',
  T2M: 'T2M',
  TMIN: 'T2M_MIN',
  TMAX: 'T2M_MAX',
  TDEW: 'T2MDEW',
  RH2M: 'RH2M',
  WIND: 'WS2M',
  SRAD: 'ALLSKY_SFC_SW_DWN',
};

const BACKEND_TIMEOUT_MS = 120000;
const EXTERNAL_TIMEOUT_MS = 120000;

function toNasaDate(date: string): string {
  return date.replaceAll('-', '');
}

function normalizeIsoDate(value: string): string {
  const parsed = dayjs(value);
  if (parsed.isValid()) {
    return parsed.format('YYYY-MM-DD');
  }

  if (value.includes('T')) {
    return value.split('T')[0];
  }

  if (value.includes(' ')) {
    return value.split(' ')[0];
  }

  return value;
}

function formatDateLabel(dateString: string): string {
  return dayjs(dateString).format('MMM D');
}

function computeBasicStats(series: TimeSeriesRecord[]): BasicStats {
  const numericValues = series
    .map((item) => item.value)
    .filter((value): value is number => value !== null && Number.isFinite(value));

  if (numericValues.length === 0) {
    return {
      availableCount: 0,
      missingCount: series.length,
      min: null,
      max: null,
      mean: null,
      sum: 0,
    };
  }

  const sum = numericValues.reduce((acc, current) => acc + current, 0);
  const min = Math.min(...numericValues);
  const max = Math.max(...numericValues);

  return {
    availableCount: numericValues.length,
    missingCount: Math.max(series.length - numericValues.length, 0),
    min,
    max,
    mean: sum / numericValues.length,
    sum,
  };
}

function formatNumber(value: number | null, digits = 3): string {
  if (value === null || !Number.isFinite(value)) {
    return 'NA';
  }
  return value.toFixed(digits);
}

function sourceLabel(sourceKey: SourceKey): string {
  return SOURCE_OPTIONS.find((source) => source.key === sourceKey)?.label || sourceKey;
}

function buildComparisonPoints(
  source1Series: TimeSeriesRecord[],
  source2Series: TimeSeriesRecord[]
): ComparisonPoint[] {
  const map1 = new Map<string, number>();
  const map2 = new Map<string, number>();

  source1Series.forEach((entry) => {
    const rounded = roundToOneDecimal(entry.value);
    if (rounded !== null && Number.isFinite(rounded)) {
      map1.set(entry.date, rounded);
    }
  });

  source2Series.forEach((entry) => {
    const rounded = roundToOneDecimal(entry.value);
    if (rounded !== null && Number.isFinite(rounded)) {
      map2.set(entry.date, rounded);
    }
  });

  const overlapDates = Array.from(map1.keys())
    .filter((date) => map2.has(date))
    .sort();

  return overlapDates.map((date) => ({
    date,
    displayDate: formatDateLabel(date),
    source1: map1.get(date) as number,
    source2: map2.get(date) as number,
  }));
}

function sourceTileConfig(stats: BasicStats, units: string) {
  return [
    {
      label: 'Available',
      value: `${stats.availableCount}`,
      color: '#2196f3',
    },
    {
      label: 'Missing',
      value: `${stats.missingCount}`,
      color: '#ff9800',
    },
    {
      label: 'Mean',
      value: `${formatNumber(stats.mean, 2)} ${units}`,
      color: '#4caf50',
    },
    {
      label: 'Max',
      value: `${formatNumber(stats.max, 2)} ${units}`,
      color: '#f44336',
    },
    {
      label: 'Min',
      value: `${formatNumber(stats.min, 2)} ${units}`,
      color: '#9c27b0',
    },
    {
      label: 'Sum',
      value: `${stats.sum.toFixed(2)} ${units}`,
      color: '#00bcd4',
    },
  ];
}

export default function DataComparisonPage() {
  const [metadata, setMetadata] = useState<any>(null);
  const [selectedVariable, setSelectedVariable] = useState<VariableKey>('RAIN');
  const [startDate, setStartDate] = useState(appConfig.date.defaultStartDate);
  const [endDate, setEndDate] = useState(appConfig.date.defaultEndDate);
  const [location, setLocation] = useState<{ lat: number; lon: number; zoom: number } | null>(null);

  const [source1, setSource1] = useState<SourceKey>('chirps');
  const [source2, setSource2] = useState<SourceKey>('nasa_s3');

  const [source1Series, setSource1Series] = useState<TimeSeriesRecord[]>([]);
  const [source2Series, setSource2Series] = useState<TimeSeriesRecord[]>([]);

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [locationDialogOpen, setLocationDialogOpen] = useState(false);
  const [pendingLocation, setPendingLocation] = useState<{ lat: number; lon: number; zoom: number }>({
    lat: 0,
    lon: 20,
    zoom: appConfig.map.defaultZoom,
  });
  const [latInput, setLatInput] = useState('0.0000');
  const [lonInput, setLonInput] = useState('20.0000');
  const [locationInputError, setLocationInputError] = useState<string | null>(null);

  const availableSources = useMemo(
    () => VARIABLE_SOURCE_MAP[selectedVariable],
    [selectedVariable]
  );

  useEffect(() => {
    const fetchMetadata = async () => {
      try {
        const meta = await apiClient.getMetadata();
        setMetadata(meta);

        const maxDateStr = meta.time_range.end.split('T')[0];
        const maxDate = new Date(maxDateStr);

        const endDateObj = new Date(maxDate.getFullYear(), maxDate.getMonth() + 1, 0);
        const defaultEnd = endDateObj.toISOString().split('T')[0];

        const startDateObj = new Date(maxDate.getFullYear(), maxDate.getMonth(), 1);
        const defaultStart = startDateObj.toISOString().split('T')[0];

        setStartDate(defaultStart);
        setEndDate(defaultEnd);
      } catch (fetchError) {
        console.error('Failed to fetch metadata', fetchError);
      }
    };

    fetchMetadata();
  }, []);

  useEffect(() => {
    if (!availableSources.includes(source1)) {
      setSource1(availableSources[0]);
      return;
    }

    if (!availableSources.includes(source2) || source1 === source2) {
      const fallback = availableSources.find((source) => source !== source1) || availableSources[0];
      setSource2(fallback);
    }
  }, [availableSources, source1, source2]);

  useEffect(() => {
    if (!location) {
      return;
    }

    let active = true;

    const fetchComparisonData = async () => {
      setLoading(true);
      setError(null);

      try {
        const [firstResult, secondResult] = await Promise.allSettled([
          fetchSeriesForSource(source1, selectedVariable, location.lat, location.lon, startDate, endDate),
          fetchSeriesForSource(source2, selectedVariable, location.lat, location.lon, startDate, endDate),
        ]);

        if (!active) {
          return;
        }

        const firstSeries = firstResult.status === 'fulfilled' ? firstResult.value : [];
        const secondSeries = secondResult.status === 'fulfilled' ? secondResult.value : [];

        setSource1Series(firstSeries);
        setSource2Series(secondSeries);

        const errorParts: string[] = [];
        if (firstResult.status === 'rejected') {
          errorParts.push(`Source 1 (${sourceLabel(source1)}) failed: ${humanizeFetchError(firstResult.reason)}`);
        }
        if (secondResult.status === 'rejected') {
          errorParts.push(`Source 2 (${sourceLabel(source2)}) failed: ${humanizeFetchError(secondResult.reason)}`);
        }

        if (errorParts.length > 0) {
          setError(errorParts.join(' | '));
        }
      } catch (fetchError: any) {
        if (active) {
          setError(fetchError?.message || 'Failed to fetch comparison data');
          setSource1Series([]);
          setSource2Series([]);
        }
      } finally {
        if (active) {
          setLoading(false);
        }
      }
    };

    fetchComparisonData();

    return () => {
      active = false;
    };
  }, [location, source1, source2, selectedVariable, startDate, endDate]);

  const mergedData = useMemo(
    () => buildComparisonPoints(source1Series, source2Series),
    [source1Series, source2Series]
  );

  const comparisonRows = useMemo(() => {
    const tolerance = 1e-6;
    const rows: DifferenceRow[] = [];

    mergedData.forEach((entry) => {
      const diff = entry.source1 - entry.source2;
      const absDiff = Math.abs(diff);

      if (absDiff > tolerance) {
        rows.push({
          date: entry.date,
          source1: entry.source1,
          source2: entry.source2,
          diff,
          absDiff,
        });
      }
    });

    return rows;
  }, [mergedData]);

  const source1Stats = useMemo(
    () => computeBasicStats(mergedData.map((entry) => ({ date: entry.date, value: entry.source1 }))),
    [mergedData]
  );
  const source2Stats = useMemo(
    () => computeBasicStats(mergedData.map((entry) => ({ date: entry.date, value: entry.source2 }))),
    [mergedData]
  );
  const chartKey = `${source1}-${source2}-${selectedVariable}-${startDate}-${endDate}`;

  const differenceStats = useMemo(() => {
    if (mergedData.length === 0) {
      return {
        comparedCount: 0,
        differenceCount: 0,
        sumDiff: 0,
        sumAbsDiff: 0,
        meanAbsDiff: 0,
        maxAbsDiff: 0,
      };
    }

    const tolerance = 1e-6;
    const diffs = mergedData.map((row) => {
      const diff = row.source1 - row.source2;
      return {
        diff,
        absDiff: Math.abs(diff),
      };
    });

    const differenceCount = diffs.filter((row) => row.absDiff > tolerance).length;
    const sumDiff = diffs.reduce((acc, row) => acc + row.diff, 0);
    const sumAbsDiff = diffs.reduce((acc, row) => acc + row.absDiff, 0);
    const maxAbsDiff = Math.max(...diffs.map((row) => row.absDiff));

    return {
      comparedCount: mergedData.length,
      differenceCount,
      sumDiff,
      sumAbsDiff,
      meanAbsDiff: sumAbsDiff / mergedData.length,
      maxAbsDiff,
    };
  }, [mergedData]);

  const selectedVariableMeta = VARIABLE_OPTIONS.find((variable) => variable.key === selectedVariable);

  const handleVariableChange = (event: SelectChangeEvent<VariableKey>) => {
    setSelectedVariable(event.target.value as VariableKey);
  };

  const handleStartDateChange = (value: Dayjs | null) => {
    if (value) {
      setStartDate(value.format('YYYY-MM-DD'));
    }
  };

  const handleEndDateChange = (value: Dayjs | null) => {
    if (value) {
      setEndDate(value.format('YYYY-MM-DD'));
    }
  };

  const openLocationDialog = () => {
    const initialLat = location?.lat ?? appConfig.map.defaultCenter.lat;
    const initialLon = location?.lon ?? appConfig.map.defaultCenter.lng;
    const initialZoom = location?.zoom ?? appConfig.map.defaultZoom;

    setPendingLocation({ lat: initialLat, lon: initialLon, zoom: initialZoom });
    setLatInput(initialLat.toFixed(4));
    setLonInput(initialLon.toFixed(4));
    setLocationInputError(null);
    setLocationDialogOpen(true);
  };

  const handlePendingLocationSelect = (lat: number, lon: number, zoom: number) => {
    setPendingLocation({ lat, lon, zoom });
    setLatInput(lat.toFixed(4));
    setLonInput(lon.toFixed(4));
    setLocationInputError(null);
  };

  const applyPendingCoordinates = () => {
    const lat = parseFloat(latInput);
    const lon = parseFloat(lonInput);

    if (!Number.isFinite(lat) || !Number.isFinite(lon) || lat < -90 || lat > 90 || lon < -180 || lon > 180) {
      setLocationInputError('Enter valid coordinates: latitude [-90, 90], longitude [-180, 180].');
      return;
    }

    setPendingLocation((current) => ({
      ...current,
      lat,
      lon,
    }));

    setLocationInputError(null);
  };

  const confirmLocation = () => {
    const lat = parseFloat(latInput);
    const lon = parseFloat(lonInput);

    if (!Number.isFinite(lat) || !Number.isFinite(lon) || lat < -90 || lat > 90 || lon < -180 || lon > 180) {
      setLocationInputError('Enter valid coordinates: latitude [-90, 90], longitude [-180, 180].');
      return;
    }

    setLocation({
      lat,
      lon,
      zoom: pendingLocation.zoom,
    });

    setLocationDialogOpen(false);
  };

  const handleDownloadReport = () => {
    if (!location || comparisonRows.length === 0) {
      return;
    }

    const reportLines: string[] = [];
    reportLines.push('Weather Data Comparison Report');
    reportLines.push('');
    reportLines.push(`Generated at: ${new Date().toISOString()}`);
    reportLines.push(`Variable: ${selectedVariableMeta?.label || selectedVariable}`);
    reportLines.push(`Units: ${selectedVariableMeta?.units || ''}`);
    reportLines.push(`Source 1: ${sourceLabel(source1)}`);
    reportLines.push(`Source 2: ${sourceLabel(source2)}`);
    reportLines.push(`Latitude: ${location.lat.toFixed(4)}`);
    reportLines.push(`Longitude: ${location.lon.toFixed(4)}`);
    reportLines.push(`Start Date: ${startDate}`);
    reportLines.push(`End Date: ${endDate}`);
    reportLines.push(`Number of compared days: ${mergedData.length}`);
    reportLines.push(`Number of different days: ${differenceStats.differenceCount}`);
    reportLines.push(`Sum difference (S1-S2): ${differenceStats.sumDiff.toFixed(6)}`);
    reportLines.push(`Sum absolute difference: ${differenceStats.sumAbsDiff.toFixed(6)}`);
    reportLines.push('');
    reportLines.push('Differences (Date | Source 1 | Source 2)');
    reportLines.push('-----------------------------------------');

    comparisonRows.forEach((row) => {
      reportLines.push(
        `${row.date.padEnd(12)} | ${row.source1.toFixed(6).padStart(14)} | ${row.source2.toFixed(6).padStart(14)}`
      );
    });

    const blob = new Blob([reportLines.join('\n')], { type: 'text/plain;charset=utf-8' });
    const url = window.URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `comparison_report_${selectedVariable}_${startDate}_${endDate}.txt`;
    document.body.appendChild(link);
    link.click();
    link.remove();
    window.URL.revokeObjectURL(url);
  };

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', height: '100vh' }}>
      <AppBar position="static" elevation={2}>
        <Toolbar>
          <Typography variant="h6" sx={{ flexGrow: 1 }}>
            {appConfig.title}
          </Typography>
          <Stack direction="row" spacing={1}>
            <Button
              component={Link}
              href="/"
              color="inherit"
              variant="text"
            >
              Home
            </Button>
            <Button
              component={Link}
              href="/comparison"
              color="inherit"
              variant="outlined"
              sx={{ borderColor: 'rgba(255,255,255,0.5)' }}
            >
              Data Comparison
            </Button>
          </Stack>
        </Toolbar>
      </AppBar>

      <Box sx={{ flex: 1, overflow: 'auto', bgcolor: 'background.default' }}>
        <Container maxWidth={false} sx={{ py: 2 }}>
          <Paper elevation={2} sx={{ p: 2, mb: 2 }}>
            <Stack direction={{ xs: 'column', md: 'row' }} spacing={2} alignItems="center" justifyContent="center">
              <FormControl size="small" sx={{ minWidth: 220 }}>
                <InputLabel id="variable-select-label">Variable</InputLabel>
                <Select
                  labelId="variable-select-label"
                  value={selectedVariable}
                  label="Variable"
                  onChange={handleVariableChange}
                >
                  {VARIABLE_OPTIONS.map((option) => (
                    <MenuItem key={option.key} value={option.key}>
                      {option.label}
                    </MenuItem>
                  ))}
                </Select>
              </FormControl>

              <DatePicker
                label="Start Date"
                value={dayjs(startDate)}
                onChange={handleStartDateChange}
                minDate={dayjs(metadata?.time_range?.start?.split('T')[0] || appConfig.date.minDate)}
                maxDate={dayjs(endDate)}
                format="YYYY-MM-DD"
                slotProps={{ textField: { size: 'small' } }}
              />

              <DatePicker
                label="End Date"
                value={dayjs(endDate)}
                onChange={handleEndDateChange}
                minDate={dayjs(startDate)}
                maxDate={dayjs(metadata?.time_range?.end?.split('T')[0] || appConfig.date.maxDate)}
                format="YYYY-MM-DD"
                slotProps={{ textField: { size: 'small' } }}
              />

              <Button
                variant="outlined"
                onClick={openLocationDialog}
                startIcon={<LocationOn />}
              >
                {location
                  ? `Location: ${location.lat.toFixed(3)}, ${location.lon.toFixed(3)}`
                  : 'Select Location'}
              </Button>

              <Button
                variant="contained"
                onClick={handleDownloadReport}
                startIcon={<Download />}
                disabled={!location || mergedData.length === 0 || loading}
              >
                Download Report
              </Button>
            </Stack>
            <Typography variant="caption" color="text.secondary" sx={{ mt: 1, display: 'block', textAlign: 'center' }}>
              Source choices are filtered by variable availability.
            </Typography>
          </Paper>

          {!location && (
            <Alert severity="info" sx={{ mb: 2 }}>
              Select a location to run source comparison.
            </Alert>
          )}

          {error && (
            <Alert severity="error" sx={{ mb: 2 }}>
              {error}
            </Alert>
          )}

          <Grid container spacing={2} sx={{ mb: 2 }}>
            <Grid item xs={12} md={2}>
              <Paper elevation={2} sx={{ p: 2, height: '100%' }}>
                <Typography variant="subtitle2" gutterBottom>
                  Source 1
                </Typography>
                <FormControl size="small" fullWidth>
                  <Select
                    value={source1}
                    onChange={(event) => setSource1(event.target.value as SourceKey)}
                  >
                    {SOURCE_OPTIONS.filter((option) => availableSources.includes(option.key)).map((option) => (
                      <MenuItem key={option.key} value={option.key} disabled={option.key === source2}>
                        {option.label}
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>

                <Chip label={selectedVariableMeta?.units || ''} size="small" sx={{ mt: 2, mb: 1 }} />

                <Grid container spacing={1}>
                  {sourceTileConfig(source1Stats, selectedVariableMeta?.units || '').map((tile) => (
                    <Grid item xs={6} key={`s1-${tile.label}`}>
                      <Card
                        elevation={1}
                        sx={{
                          bgcolor: `${tile.color}10`,
                          borderLeft: `4px solid ${tile.color}`,
                        }}
                      >
                        <CardContent sx={{ px: 1, py: 1 }}>
                          <Typography variant="caption" color="text.secondary" sx={{ display: 'block' }}>
                            {tile.label}
                          </Typography>
                          <Typography variant="body2" sx={{ fontWeight: 600 }}>
                            {tile.value}
                          </Typography>
                        </CardContent>
                      </Card>
                    </Grid>
                  ))}
                </Grid>
              </Paper>
            </Grid>

            <Grid item xs={12} md={8}>
              <Paper elevation={2} sx={{ p: 2, minHeight: 560 }}>
                <Typography variant="h6" gutterBottom>
                  Source Comparison Plot
                </Typography>
                {loading ? (
                  <Box sx={{ minHeight: 480, display: 'flex', justifyContent: 'center', alignItems: 'center' }}>
                    <CircularProgress />
                  </Box>
                ) : (
                  <ResponsiveContainer width="100%" height={480}>
                    <BarChart
                      key={chartKey}
                      data={mergedData}
                      margin={{ top: 12, right: 18, left: 10, bottom: 56 }}
                    >
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis
                        dataKey="displayDate"
                        angle={-45}
                        textAnchor="end"
                        height={72}
                      />
                      <YAxis
                        label={{
                          value: selectedVariableMeta?.units || '',
                          angle: -90,
                          position: 'insideLeft',
                        }}
                      />
                      <Tooltip
                        labelFormatter={(_label, payload) => {
                          if (!payload || payload.length === 0) {
                            return '';
                          }
                          return payload[0].payload.date;
                        }}
                      />
                      <Legend />
                      <Bar
                        dataKey="source1"
                        name={sourceLabel(source1)}
                        fill="#1976d2"
                        maxBarSize={14}
                      />
                      <Bar
                        dataKey="source2"
                        name={sourceLabel(source2)}
                        fill="#ef6c00"
                        maxBarSize={14}
                      />
                    </BarChart>
                  </ResponsiveContainer>
                )}
              </Paper>
            </Grid>

            <Grid item xs={12} md={2}>
              <Paper elevation={2} sx={{ p: 2, height: '100%' }}>
                <Typography variant="subtitle2" gutterBottom>
                  Source 2
                </Typography>
                <FormControl size="small" fullWidth>
                  <Select
                    value={source2}
                    onChange={(event) => setSource2(event.target.value as SourceKey)}
                  >
                    {SOURCE_OPTIONS.filter((option) => availableSources.includes(option.key)).map((option) => (
                      <MenuItem key={option.key} value={option.key} disabled={option.key === source1}>
                        {option.label}
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>

                <Chip label={selectedVariableMeta?.units || ''} size="small" sx={{ mt: 2, mb: 1 }} />

                <Grid container spacing={1}>
                  {sourceTileConfig(source2Stats, selectedVariableMeta?.units || '').map((tile) => (
                    <Grid item xs={6} key={`s2-${tile.label}`}>
                      <Card
                        elevation={1}
                        sx={{
                          bgcolor: `${tile.color}10`,
                          borderLeft: `4px solid ${tile.color}`,
                        }}
                      >
                        <CardContent sx={{ px: 1, py: 1 }}>
                          <Typography variant="caption" color="text.secondary" sx={{ display: 'block' }}>
                            {tile.label}
                          </Typography>
                          <Typography variant="body2" sx={{ fontWeight: 600 }}>
                            {tile.value}
                          </Typography>
                        </CardContent>
                      </Card>
                    </Grid>
                  ))}
                </Grid>
              </Paper>
            </Grid>
          </Grid>

          <Box sx={{ mt: 3 }}>
            <Card elevation={2}>
              <CardContent>
                <Typography variant="subtitle1" sx={{ fontWeight: 700 }} gutterBottom>
                  Differences ({sourceLabel(source1)} - {sourceLabel(source2)})
                </Typography>

                <Grid container spacing={1}>
                  {[
                    {
                      label: 'Compared Days',
                      value: `${differenceStats.comparedCount}`,
                      color: '#1976d2',
                    },
                    {
                      label: 'Different Entries',
                      value: `${differenceStats.differenceCount}`,
                      color: '#ef6c00',
                    },
                    {
                      label: 'Sum Diff',
                      value: differenceStats.sumDiff.toFixed(4),
                      color: '#8e24aa',
                    },
                    {
                      label: 'Sum Abs Diff',
                      value: differenceStats.sumAbsDiff.toFixed(4),
                      color: '#00897b',
                    },
                    {
                      label: 'Mean Abs Diff',
                      value: differenceStats.meanAbsDiff.toFixed(4),
                      color: '#43a047',
                    },
                    {
                      label: 'Max Abs Diff',
                      value: differenceStats.maxAbsDiff.toFixed(4),
                      color: '#e53935',
                    },
                  ].map((tile) => (
                    <Grid item xs={6} sm={4} md={2} key={tile.label}>
                      <Card
                        elevation={1}
                        sx={{
                          bgcolor: `${tile.color}10`,
                          borderLeft: `4px solid ${tile.color}`,
                          height: '100%',
                        }}
                      >
                        <CardContent sx={{ px: 1.5, py: 1.25 }}>
                          <Typography variant="caption" color="text.secondary" sx={{ display: 'block' }}>
                            {tile.label}
                          </Typography>
                          <Typography variant="body2" sx={{ fontWeight: 700 }}>
                            {tile.value}
                          </Typography>
                        </CardContent>
                      </Card>
                    </Grid>
                  ))}
                </Grid>
              </CardContent>
            </Card>
          </Box>
        </Container>
      </Box>

      <Dialog open={locationDialogOpen} onClose={() => setLocationDialogOpen(false)} maxWidth="md" fullWidth>
        <DialogTitle>Select Comparison Location</DialogTitle>
        <DialogContent>
          <Box sx={{ height: 360, mb: 2 }}>
            <MapView
              onLocationSelect={handlePendingLocationSelect}
              selectedLocation={{ lat: pendingLocation.lat, lon: pendingLocation.lon }}
            />
          </Box>

          <Stack direction={{ xs: 'column', sm: 'row' }} spacing={2}>
            <TextField
              label="Latitude"
              value={latInput}
              onChange={(event) => setLatInput(event.target.value)}
              type="number"
              fullWidth
              inputProps={{ step: 0.0001, min: -90, max: 90 }}
            />
            <TextField
              label="Longitude"
              value={lonInput}
              onChange={(event) => setLonInput(event.target.value)}
              type="number"
              fullWidth
              inputProps={{ step: 0.0001, min: -180, max: 180 }}
            />
            <Button variant="outlined" onClick={applyPendingCoordinates}>
              Apply
            </Button>
          </Stack>

          {locationInputError && (
            <Alert severity="error" sx={{ mt: 2 }}>
              {locationInputError}
            </Alert>
          )}
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setLocationDialogOpen(false)}>Cancel</Button>
          <Button onClick={confirmLocation} variant="contained">
            Confirm Location
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
}

async function fetchSeriesForSource(
  source: SourceKey,
  variable: VariableKey,
  lat: number,
  lon: number,
  startDate: string,
  endDate: string
): Promise<TimeSeriesRecord[]> {
  if (source === 'chirps') {
    const response = await fetchBackendVariableSeries({
      lat,
      lon,
      startDate,
      endDate,
      variable: 'RAIN',
      source: 'chirps',
    });

    return response.time.map((time: string, index: number) => ({
      date: normalizeIsoDate(time),
      value: toNumericOrNull(response.values?.[index]),
    }));
  }

  if (source === 'nasa_s3') {
    const backendVariable = variable === 'RAIN' ? 'RAIN1' : variable;
    const response = await fetchBackendVariableSeries({
      lat,
      lon,
      startDate,
      endDate,
      variable: backendVariable,
      source: 'nasa_s3',
    });

    return response.time.map((time: string, index: number) => ({
      date: normalizeIsoDate(time),
      value: toNumericOrNull(response.values?.[index]),
    }));
  }

  if (source === 'nasa_api') {
    const parameter = NASA_API_PARAM_MAP[variable];
    const response = await axios.get('https://power.larc.nasa.gov/api/temporal/daily/point', {
      timeout: EXTERNAL_TIMEOUT_MS,
      params: {
        start: toNasaDate(startDate),
        end: toNasaDate(endDate),
        latitude: lat,
        longitude: lon,
        community: 'AG',
        units: 'metric',
        parameters: parameter,
        format: 'JSON',
        theme: 'light',
        user: 'DAVE',
        'time-standard': 'LST',
      },
    });

    const paramValues = response.data?.properties?.parameter?.[parameter] || {};
    return Object.keys(paramValues)
      .sort()
      .map((yyyymmdd) => ({
        date: `${yyyymmdd.slice(0, 4)}-${yyyymmdd.slice(4, 6)}-${yyyymmdd.slice(6, 8)}`,
        value: toNumericOrNull(paramValues[yyyymmdd]),
      }));
  }

  if (source === 'open_meteo') {
    const response = await axios.get('https://archive-api.open-meteo.com/v1/archive', {
      timeout: EXTERNAL_TIMEOUT_MS,
      params: {
        latitude: lat,
        longitude: lon,
        start_date: startDate,
        end_date: endDate,
        daily: 'precipitation_sum',
        timezone: 'UTC',
      },
    });

    const times: string[] = response.data?.daily?.time || [];
    const values: Array<number | null> = response.data?.daily?.precipitation_sum || [];

    return times.map((date, index) => ({
      date,
      value: toNumericOrNull(values[index]),
    }));
  }

  return [];
}

async function fetchBackendVariableSeries(params: {
  lat: number;
  lon: number;
  startDate: string;
  endDate: string;
  variable: string;
  source?: 'chirps' | 'nasa_s3' | 'auto';
}) {
  const response = await axios.post(
    `${appConfig.api.baseUrl}/api/data/timeseries-variable`,
    null,
    {
      params: {
        lat: params.lat,
        lon: params.lon,
        start_date: params.startDate,
        end_date: params.endDate,
        variable: params.variable,
        source: params.source || 'auto',
      },
      timeout: BACKEND_TIMEOUT_MS,
    }
  );

  return response.data;
}

function humanizeFetchError(error: any): string {
  if (axios.isAxiosError(error)) {
    if (error.code === 'ECONNABORTED') {
      return 'request timed out';
    }
    return error.response?.data?.detail || error.message || 'request failed';
  }

  return error?.message || 'request failed';
}

function toNumericOrNull(value: unknown): number | null {
  if (value === null || value === undefined) {
    return null;
  }

  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return null;
  }

  return numeric;
}

function roundToOneDecimal(value: number | null): number | null {
  if (value === null) {
    return null;
  }
  return Math.round(value * 10) / 10;
}
