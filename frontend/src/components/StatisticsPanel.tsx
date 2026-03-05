'use client';

import { useState } from 'react';
import {
  Paper,
  Box,
  Typography,
  Grid,
  Card,
  CardContent,
  CircularProgress,
  Tabs,
  Tab,
} from '@mui/material';
import {
  WaterDrop,
  Opacity,
  ShowChart,
  BarChart as BarChartIcon,
  TrendingUp,
  TrendingDown,
  Umbrella,
  WbSunny,
} from '@mui/icons-material';

interface Statistics {
  total_precipitation: number;
  mean_daily: number;
  median_daily: number;
  max_daily: number;
  min_daily: number;
  std_daily: number;
  days_with_rain: number;
  dry_days: number;
}

interface StatisticsData {
  all_days: Statistics;
  wet_days: Statistics;
  is_rain_variable?: boolean;
  variable_name?: string;
  units?: string;
}

interface StatisticsPanelProps {
  statistics: StatisticsData | null;
  loading: boolean;
}

export default function StatisticsPanel({ statistics, loading }: StatisticsPanelProps) {
  const [selectedTab, setSelectedTab] = useState(0);

  const handleTabChange = (event: React.SyntheticEvent, newValue: number) => {
    setSelectedTab(newValue);
  };

  if (loading) {
    return (
      <Paper elevation={2} sx={{ p: 3 }}>
        <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', minHeight: 200 }}>
          <CircularProgress />
        </Box>
      </Paper>
    );
  }

  if (!statistics) {
    return (
      <Paper elevation={2} sx={{ p: 3 }}>
        <Typography variant="body1" color="text.secondary" align="center">
          Select a location to view statistics
        </Typography>
      </Paper>
    );
  }

  // Select the appropriate statistics based on the tab
  const currentStats = selectedTab === 0 ? statistics.all_days : statistics.wet_days;
  const isRainVariable = statistics.is_rain_variable ?? true;
  const units = statistics.units || 'mm';
  
  // Build stats array based on variable type
  const statsArray = [];
  
  // Total (only for rain variables)
  if (isRainVariable) {
    statsArray.push({
      label: 'Total Precipitation',
      value: `${currentStats.total_precipitation.toFixed(1)} ${units}`,
      icon: <WaterDrop />,
      color: '#2196f3',
    });
  }
  
  // Mean
  statsArray.push({
    label: 'Mean Daily',
    value: `${currentStats.mean_daily.toFixed(2)} ${units}`,
    icon: <ShowChart />,
    color: '#4caf50',
  });
  
  // Median
  statsArray.push({
    label: 'Median Daily',
    value: `${currentStats.median_daily.toFixed(2)} ${units}`,
    icon: <BarChartIcon />,
    color: '#ff9800',
  });
  
  // Max
  statsArray.push({
    label: 'Max Daily',
    value: `${currentStats.max_daily.toFixed(1)} ${units}`,
    icon: <TrendingUp />,
    color: '#f44336',
  });
  
  // Min
  statsArray.push({
    label: 'Min Daily',
    value: `${currentStats.min_daily.toFixed(1)} ${units}`,
    icon: <TrendingDown />,
    color: '#9c27b0',
  });
  
  // Std Dev
  statsArray.push({
    label: 'Std. Deviation',
    value: `${currentStats.std_daily.toFixed(2)} ${units}`,
    icon: <Opacity />,
    color: '#00bcd4',
  });
  
  const stats = statsArray;

  const totalDays = currentStats.days_with_rain + currentStats.dry_days;
  const rainPercentage = totalDays > 0 ? (currentStats.days_with_rain / totalDays) * 100 : 0;

  return (
    <Paper elevation={2} sx={{ p: 2 }}>
      <Typography variant="h6" gutterBottom>
        Statistical Summary
      </Typography>
      {isRainVariable && (
        <Tabs 
          value={selectedTab} 
          onChange={handleTabChange} 
          sx={{ mb: 2, borderBottom: 1, borderColor: 'divider' }}
        >
          <Tab label="All Days" />
          <Tab label="Wet Days Only" />
        </Tabs>
      )}
      <Grid container spacing={2}>
        {stats.map((stat, index) => (
          <Grid item xs={12} sm={6} key={index}>
            <Card
              elevation={1}
              sx={{
                bgcolor: `${stat.color}10`,
                borderLeft: `4px solid ${stat.color}`,
              }}
            >
              <CardContent sx={{ py: 1.5, px: 2 }}>
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                  <Box sx={{ color: stat.color }}>{stat.icon}</Box>
                  <Box sx={{ flex: 1 }}>
                    <Typography variant="caption" color="text.secondary">
                      {stat.label}
                    </Typography>
                    <Typography variant="h6" sx={{ fontWeight: 600 }}>
                      {stat.value}
                    </Typography>
                  </Box>
                </Box>
              </CardContent>
            </Card>
          </Grid>
        ))}
        {isRainVariable && (
          <>
            <Grid item xs={12} sm={6}>
              <Card
                elevation={1}
                sx={{
                  bgcolor: '#2196f310',
                  borderLeft: `4px solid #2196f3`,
                }}
              >
                <CardContent sx={{ py: 1.5, px: 2 }}>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                    <Box sx={{ color: '#2196f3' }}><Umbrella /></Box>
                    <Box sx={{ flex: 1 }}>
                      <Typography variant="caption" color="text.secondary">
                        Days with Rain
                      </Typography>
                      <Typography variant="h6" sx={{ fontWeight: 600 }}>
                        {currentStats.days_with_rain} ({rainPercentage.toFixed(1)}%)
                      </Typography>
                    </Box>
                  </Box>
                </CardContent>
              </Card>
            </Grid>
            <Grid item xs={12} sm={6}>
              <Card
                elevation={1}
                sx={{
                  bgcolor: '#ff980810',
                  borderLeft: `4px solid #ff9800`,
                }}
              >
                <CardContent sx={{ py: 1.5, px: 2 }}>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                    <Box sx={{ color: '#ff9800' }}><WbSunny /></Box>
                    <Box sx={{ flex: 1 }}>
                      <Typography variant="caption" color="text.secondary">
                        Dry Days
                      </Typography>
                      <Typography variant="h6" sx={{ fontWeight: 600 }}>
                        {currentStats.dry_days} ({(100 - rainPercentage).toFixed(1)}%)
                      </Typography>
                    </Box>
                  </Box>
                </CardContent>
              </Card>
            </Grid>
          </>
        )}
      </Grid>
    </Paper>
  );
}
