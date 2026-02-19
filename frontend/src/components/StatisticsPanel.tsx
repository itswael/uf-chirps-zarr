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

  const stats = [
    {
      label: 'Total Precipitation',
      value: `${currentStats.total_precipitation.toFixed(1)} mm`,
      icon: <WaterDrop />,
      color: '#2196f3',
    },
    {
      label: 'Mean Daily',
      value: `${currentStats.mean_daily.toFixed(2)} mm/day`,
      icon: <ShowChart />,
      color: '#4caf50',
    },
    {
      label: 'Median Daily',
      value: `${currentStats.median_daily.toFixed(2)} mm/day`,
      icon: <BarChartIcon />,
      color: '#ff9800',
    },
    {
      label: 'Max Daily',
      value: `${currentStats.max_daily.toFixed(1)} mm/day`,
      icon: <TrendingUp />,
      color: '#f44336',
    },
    {
      label: 'Min Daily',
      value: `${currentStats.min_daily.toFixed(1)} mm/day`,
      icon: <TrendingDown />,
      color: '#9c27b0',
    },
    {
      label: 'Std. Deviation',
      value: `${currentStats.std_daily.toFixed(2)} mm/day`,
      icon: <Opacity />,
      color: '#00bcd4',
    },
  ];

  const totalDays = currentStats.days_with_rain + currentStats.dry_days;
  const rainPercentage = totalDays > 0 ? (currentStats.days_with_rain / totalDays) * 100 : 0;

  return (
    <Paper elevation={2} sx={{ p: 2 }}>
      <Typography variant="h6" gutterBottom>
        Statistical Summary
      </Typography>
      <Tabs 
        value={selectedTab} 
        onChange={handleTabChange} 
        sx={{ mb: 2, borderBottom: 1, borderColor: 'divider' }}
      >
        <Tab label="All Days" />
        <Tab label="Wet Days Only" />
      </Tabs>
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
        <Grid item xs={12}>
          <Card elevation={1}>
            <CardContent>
              <Typography variant="subtitle2" gutterBottom>
                Precipitation Days
              </Typography>
              <Grid container spacing={2}>
                <Grid item xs={6}>
                  <Typography variant="body2" color="text.secondary">
                    Days with Rain
                  </Typography>
                  <Typography variant="h6" color="primary">
                    {currentStats.days_with_rain} ({rainPercentage.toFixed(1)}%)
                  </Typography>
                </Grid>
                <Grid item xs={6}>
                  <Typography variant="body2" color="text.secondary">
                    Dry Days
                  </Typography>
                  <Typography variant="h6" color="text.primary">
                    {currentStats.dry_days} ({(100 - rainPercentage).toFixed(1)}%)
                  </Typography>
                </Grid>
              </Grid>
            </CardContent>
          </Card>
        </Grid>
      </Grid>
    </Paper>
  );
}
