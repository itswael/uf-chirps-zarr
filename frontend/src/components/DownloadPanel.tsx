'use client';

import { useState } from 'react';
import {
  Paper,
  Box,
  Typography,
  Button,
  Alert,
  Snackbar,
  Stack,
  Chip,
} from '@mui/material';
import { Download, LocationOn, CalendarToday } from '@mui/icons-material';
import { apiClient } from '@/utils/api';

interface DownloadPanelProps {
  location: { lat: number; lon: number } | null;
  startDate: string;
  endDate: string;
}

export default function DownloadPanel({ location, startDate, endDate }: DownloadPanelProps) {
  const [downloading, setDownloading] = useState(false);
  const [success, setSuccess] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleDownload = async () => {
    if (!location) {
      setError('Please select a location on the map first');
      return;
    }

    setDownloading(true);
    setError(null);

    try {
      await apiClient.downloadIcasa({
        lat: location.lat,
        lon: location.lon,
        start_date: startDate,
        end_date: endDate,
      });
      setSuccess(true);
    } catch (err: any) {
      setError(err.message || 'Failed to download data');
    } finally {
      setDownloading(false);
    }
  };

  return (
    <>
      <Paper elevation={2} sx={{ p: 2 }}>
        <Typography variant="h6" gutterBottom>
          Download Data
        </Typography>
        
        <Stack spacing={2}>
          <Box>
            <Typography variant="subtitle2" gutterBottom>
              Selected Parameters:
            </Typography>
            <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap>
              {location ? (
                <Chip
                  icon={<LocationOn />}
                  label={`Lat: ${location.lat.toFixed(4)}°, Lon: ${location.lon.toFixed(4)}°`}
                  size="small"
                  color="primary"
                  variant="outlined"
                />
              ) : (
                <Chip
                  icon={<LocationOn />}
                  label="No location selected"
                  size="small"
                  color="default"
                  variant="outlined"
                />
              )}
              <Chip
                icon={<CalendarToday />}
                label={`${startDate} to ${endDate}`}
                size="small"
                color="primary"
                variant="outlined"
              />
            </Stack>
          </Box>

          <Alert severity="info" sx={{ fontSize: '0.875rem' }}>
            <Typography variant="caption" display="block" gutterBottom>
              <strong>ICASA Format Details:</strong>
            </Typography>
            <Typography variant="caption" display="block">
              • Date format: YYYYDDD (Year + Day of Year)
            </Typography>
            <Typography variant="caption" display="block">
              • Coordinates: Rounded to 1 decimal place
            </Typography>
            <Typography variant="caption" display="block">
              • Precipitation: Daily values in mm/day
            </Typography>
          </Alert>

          <Button
            variant="contained"
            color="primary"
            size="large"
            fullWidth
            onClick={handleDownload}
            disabled={!location || downloading}
            startIcon={<Download />}
          >
            {downloading ? 'Preparing Download...' : 'Download ICASA Format'}
          </Button>

          {!location && (
            <Typography variant="caption" color="text.secondary" align="center">
              Click on the map to select a location first
            </Typography>
          )}
        </Stack>
      </Paper>

      <Snackbar
        open={success}
        autoHideDuration={6000}
        onClose={() => setSuccess(false)}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
      >
        <Alert onClose={() => setSuccess(false)} severity="success" sx={{ width: '100%' }}>
          Data downloaded successfully!
        </Alert>
      </Snackbar>

      <Snackbar
        open={!!error}
        autoHideDuration={6000}
        onClose={() => setError(null)}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
      >
        <Alert onClose={() => setError(null)} severity="error" sx={{ width: '100%' }}>
          {error}
        </Alert>
      </Snackbar>
    </>
  );
}
