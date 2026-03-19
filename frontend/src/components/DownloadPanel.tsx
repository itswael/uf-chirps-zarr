'use client';

import { useMemo, useState } from 'react';
import {
  Paper,
  Box,
  Typography,
  Button,
  Alert,
  Snackbar,
  Stack,
  Chip,
  Tabs,
  Tab,
  LinearProgress,
  List,
  ListItem,
  ListItemText,
  IconButton,
  CircularProgress,
  Select,
  MenuItem,
  FormControl,
  InputLabel,
  SelectChangeEvent,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  FormGroup,
  FormControlLabel,
  Checkbox,
} from '@mui/material';
import {
  Download,
  LocationOn,
  CalendarToday,
  CloudUpload,
  Close,
  Tune,
} from '@mui/icons-material';
import { apiClient } from '@/utils/api';

interface DownloadPanelProps {
  location: { lat: number; lon: number } | null;
  startDate: string;
  endDate: string;
}

const MET_PARAMETER_OPTIONS = ['T2M', 'TMAX', 'TMIN', 'TDEW', 'RH2M', 'WIND', 'SRAD'];

function getParameterOptions(rainSource: string): string[] {
  if (rainSource === 'both') {
    return ['RAIN1', 'RAIN2', ...MET_PARAMETER_OPTIONS];
  }
  return ['RAIN', ...MET_PARAMETER_OPTIONS];
}

export default function DownloadPanel({ location, startDate, endDate }: DownloadPanelProps) {
  const [tabValue, setTabValue] = useState(0);
  const [downloading, setDownloading] = useState(false);
  const [success, setSuccess] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Default rain source is CHIRPS and all parameters are preselected.
  const [rainSource, setRainSource] = useState<string>('chirps');
  const [selectedParameters, setSelectedParameters] = useState<string[]>(getParameterOptions('chirps'));
  const [parameterDialogOpen, setParameterDialogOpen] = useState(false);

  // Multi-point download state
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [validationInfo, setValidationInfo] = useState<any>(null);
  const [validating, setValidating] = useState(false);
  const [validationError, setValidationError] = useState<string | null>(null);

  const parameterOptions = useMemo(() => getParameterOptions(rainSource), [rainSource]);
  const selectedCount = selectedParameters.length;

  const handleTabChange = (_event: React.SyntheticEvent, newValue: number) => {
    setTabValue(newValue);
    setError(null);
    setValidationError(null);
  };

  const handleRainSourceChange = (event: SelectChangeEvent) => {
    const nextSource = event.target.value;
    setRainSource(nextSource);
    setSelectedParameters(getParameterOptions(nextSource));
  };

  const handleToggleParameter = (parameter: string) => {
    setSelectedParameters((prev) => {
      if (prev.includes(parameter)) {
        return prev.filter((p) => p !== parameter);
      }
      return [...prev, parameter];
    });
  };

  const handleSelectAllParameters = () => {
    setSelectedParameters(parameterOptions);
  };

  const handleClearAllParameters = () => {
    setSelectedParameters([]);
  };

  const handleSingleDownload = async () => {
    if (!location) {
      setError('Please select a location on the map first');
      return;
    }

    if (selectedParameters.length === 0) {
      setError('Please select at least one parameter');
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
        rain_source: rainSource,
        selected_parameters: selectedParameters,
      });
      setSuccess(true);
    } catch (err: any) {
      setError(err.message || 'Failed to download data');
    } finally {
      setDownloading(false);
    }
  };

  const handleFileSelect = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const files = event.target.files;
    if (!files || files.length === 0) return;

    const file = files[0];
    const fileName = file.name.toLowerCase();

    if (!fileName.endsWith('.shp')) {
      setValidationError('Please select a .shp file');
      return;
    }

    setSelectedFile(file);
    setValidationInfo(null);
    setValidationError(null);

    setValidating(true);
    try {
      const result = await apiClient.validateShapefile(file);
      setValidationInfo(result);

      if (!result.valid) {
        setValidationError(result.message);
      }
    } catch (err: any) {
      setValidationError(err.response?.data?.detail || 'Failed to validate shapefile');
      setSelectedFile(null);
    } finally {
      setValidating(false);
    }
  };

  const handleMultiDownload = async () => {
    if (!selectedFile) {
      setError('Please select a shapefile first');
      return;
    }

    if (!validationInfo?.valid) {
      setError('Please select a valid shapefile');
      return;
    }

    if (selectedParameters.length === 0) {
      setError('Please select at least one parameter');
      return;
    }

    setDownloading(true);
    setError(null);

    try {
      await apiClient.downloadIcasaMulti({
        file: selectedFile,
        start_date: startDate,
        end_date: endDate,
        rain_source: rainSource,
        selected_parameters: selectedParameters,
      });
      setSuccess(true);
    } catch (err: any) {
      setError(err.response?.data?.detail || err.message || 'Failed to download data');
    } finally {
      setDownloading(false);
    }
  };

  const handleRemoveFile = () => {
    setSelectedFile(null);
    setValidationInfo(null);
    setValidationError(null);
  };

  return (
    <>
      <Paper elevation={2} sx={{ p: 2 }}>
        <Typography variant="h6" gutterBottom>
          Download Data
        </Typography>

        <Box sx={{ borderBottom: 1, borderColor: 'divider', mb: 2 }}>
          <Tabs value={tabValue} onChange={handleTabChange} aria-label="download options">
            <Tab label="Single Location" />
            <Tab label="Multi-Point (Shapefile)" />
          </Tabs>
        </Box>

        {tabValue === 0 && (
          <Stack spacing={2}>
            <Box>
              <Typography variant="subtitle2" gutterBottom>
                Selected Parameters:
              </Typography>
              <Stack direction="row" spacing={1} flexWrap="wrap" useFlexGap alignItems="center">
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
                <Button
                  variant="outlined"
                  size="small"
                  onClick={() => setParameterDialogOpen(true)}
                  startIcon={<Tune fontSize="small" />}
                >
                  Parameters
                </Button>
              </Stack>
            </Box>

            <Alert severity="info" sx={{ fontSize: '0.875rem' }}>
              <Typography variant="caption" display="block" gutterBottom>
                <strong>Rain Data Source Options:</strong>
              </Typography>
              <Typography variant="caption" display="block">
                • <strong>CHIRPS Only:</strong> Rain from CHIRPS (0.05° resolution), other variables from NASA POWER
              </Typography>
              <Typography variant="caption" display="block">
                • <strong>NASA POWER Only:</strong> All data from NASA POWER (0.5° resolution)
              </Typography>
              <Typography variant="caption" display="block">
                • <strong>Both:</strong> Includes both RAIN1 (CHIRPS) and RAIN2 (NASA POWER) in the file
              </Typography>
            </Alert>

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
              onClick={handleSingleDownload}
              disabled={!location || downloading || selectedParameters.length === 0}
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
        )}

        {tabValue === 1 && (
          <Stack spacing={2}>
            <Alert severity="info" sx={{ fontSize: '0.875rem' }}>
              <Typography variant="caption" display="block" gutterBottom>
                <strong>Multi-Point Download:</strong>
              </Typography>
              <Typography variant="caption" display="block">
                • Upload a shapefile (.shp) to extract multiple coordinate points
              </Typography>
              <Typography variant="caption" display="block">
                • System extracts all coordinates and creates one ICASA file per point
              </Typography>
              <Typography variant="caption" display="block">
                • Downloads as a zip package (max 1000 points)
              </Typography>
            </Alert>

            <Box>
              <Button variant="outlined" component="label" startIcon={<CloudUpload />}>
                {selectedFile ? selectedFile.name : 'Select Shapefile (.shp)'}
                <input type="file" hidden accept=".shp" onChange={handleFileSelect} />
              </Button>
              <Typography variant="caption" color="text.secondary" display="block" sx={{ mt: 0.5 }}>
                Upload just the .shp file. Missing .shx and .dbf components will be auto-generated.
              </Typography>
            </Box>

            {validating && (
              <Box>
                <Typography variant="caption" color="text.secondary">
                  Validating shapefile...
                </Typography>
                <LinearProgress />
              </Box>
            )}

            {selectedFile && !validating && (
              <Paper variant="outlined" sx={{ p: 2 }}>
                <Stack spacing={1}>
                  <Box display="flex" justifyContent="space-between" alignItems="center">
                    <Typography variant="subtitle2">{selectedFile.name}</Typography>
                    <IconButton size="small" onClick={handleRemoveFile}>
                      <Close fontSize="small" />
                    </IconButton>
                  </Box>

                  {validationInfo && (
                    <>
                      <List dense>
                        <ListItem>
                          <ListItemText primary="Total Points" secondary={validationInfo.total_points} />
                        </ListItem>
                        {validationInfo.valid_points !== undefined && (
                          <ListItem>
                            <ListItemText primary="Valid Points" secondary={validationInfo.valid_points} />
                          </ListItem>
                        )}
                      </List>

                      {validationInfo.sample_coordinates && validationInfo.sample_coordinates.length > 0 && (
                        <Box>
                          <Typography variant="caption" color="text.secondary">
                            Sample coordinates:
                          </Typography>
                          <Box sx={{ maxHeight: 100, overflow: 'auto', mt: 0.5 }}>
                            {validationInfo.sample_coordinates.map((coord: any, idx: number) => (
                              <Typography key={idx} variant="caption" display="block">
                                {idx + 1}. Lat: {coord.lat.toFixed(4)}°, Lon: {coord.lon.toFixed(4)}°
                              </Typography>
                            ))}
                          </Box>
                        </Box>
                      )}

                      {validationInfo.issues && validationInfo.issues.length > 0 && (
                        <Alert severity="warning" sx={{ fontSize: '0.75rem' }}>
                          <Typography variant="caption">{validationInfo.issues.join(', ')}</Typography>
                        </Alert>
                      )}
                    </>
                  )}
                </Stack>
              </Paper>
            )}

            {validationError && <Alert severity="error">{validationError}</Alert>}

            <Box>
              <Typography variant="subtitle2" gutterBottom>
                Date Range:
              </Typography>
              <Stack direction="row" spacing={1} alignItems="center">
                <Chip
                  icon={<CalendarToday />}
                  label={`${startDate} to ${endDate}`}
                  size="small"
                  color="primary"
                  variant="outlined"
                />
                <Button
                  variant="outlined"
                  size="small"
                  onClick={() => setParameterDialogOpen(true)}
                  startIcon={<Tune fontSize="small" />}
                >
                  Parameters
                </Button>
              </Stack>
            </Box>

            <Button
              variant="contained"
              color="primary"
              size="large"
              fullWidth
              onClick={handleMultiDownload}
              disabled={!selectedFile || !validationInfo?.valid || downloading || selectedParameters.length === 0}
              startIcon={downloading ? <CircularProgress size={20} color="inherit" /> : <Download />}
            >
              {downloading
                ? 'Generating Files...'
                : `Download ${validationInfo?.valid_points || 0} ICASA Files (ZIP)`}
            </Button>

            {downloading && (
              <Box>
                <Typography variant="caption" color="text.secondary" align="center" display="block">
                  Processing {validationInfo?.valid_points || 0} coordinates...
                </Typography>
                <LinearProgress />
              </Box>
            )}
          </Stack>
        )}
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
        autoHideDuration={10000}
        onClose={() => setError(null)}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
      >
        <Alert onClose={() => setError(null)} severity="error" sx={{ width: '100%' }}>
          {error}
        </Alert>
      </Snackbar>

      <Dialog open={parameterDialogOpen} onClose={() => setParameterDialogOpen(false)} fullWidth maxWidth="sm">
        <DialogTitle>ICASA Parameter Selection</DialogTitle>
        <DialogContent>
          <Stack spacing={2} sx={{ pt: 1 }}>
            <FormControl fullWidth size="small">
              <InputLabel>Rain Data Source</InputLabel>
              <Select value={rainSource} onChange={handleRainSourceChange} label="Rain Data Source">
                <MenuItem value="chirps">CHIRPS Only</MenuItem>
                <MenuItem value="nasa_power">NASA POWER Only</MenuItem>
                <MenuItem value="both">Both (CHIRPS + NASA POWER)</MenuItem>
              </Select>
            </FormControl>

            <Box display="flex" justifyContent="space-between" alignItems="center">
              <Typography variant="subtitle2">
                Parameters ({selectedCount}/{parameterOptions.length} selected)
              </Typography>
              <Stack direction="row" spacing={1}>
                <Button size="small" onClick={handleSelectAllParameters}>
                  Select All
                </Button>
                <Button size="small" onClick={handleClearAllParameters}>
                  Clear
                </Button>
              </Stack>
            </Box>

            <FormGroup>
              {parameterOptions.map((parameter) => (
                <FormControlLabel
                  key={parameter}
                  control={
                    <Checkbox
                      size="small"
                      checked={selectedParameters.includes(parameter)}
                      onChange={() => handleToggleParameter(parameter)}
                    />
                  }
                  label={parameter}
                />
              ))}
            </FormGroup>

            {selectedParameters.length === 0 && (
              <Alert severity="warning" sx={{ fontSize: '0.75rem' }}>
                Select at least one parameter to include in the ICASA file.
              </Alert>
            )}
          </Stack>
        </DialogContent>
        <DialogActions>
          <Button onClick={() => setParameterDialogOpen(false)}>Done</Button>
        </DialogActions>
      </Dialog>
    </>
  );
}
