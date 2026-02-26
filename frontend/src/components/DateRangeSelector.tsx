'use client';

import { Paper, Box, Typography, Stack } from '@mui/material';
import { DatePicker } from '@mui/x-date-pickers/DatePicker';
import dayjs, { Dayjs } from 'dayjs';

interface DateRangeSelectorProps {
  startDate: string;
  endDate: string;
  onStartDateChange: (date: string) => void;
  onEndDateChange: (date: string) => void;
  minDate: string;
  maxDate: string;
}

export default function DateRangeSelector({
  startDate,
  endDate,
  onStartDateChange,
  onEndDateChange,
  minDate,
  maxDate,
}: DateRangeSelectorProps) {
  const minDateObj = dayjs(minDate);
  const maxDateObj = dayjs(maxDate);

  const handleStartDateChange = (newValue: Dayjs | null) => {
    if (newValue) {
      onStartDateChange(newValue.format('YYYY-MM-DD'));
    }
  };

  const handleEndDateChange = (newValue: Dayjs | null) => {
    if (newValue) {
      onEndDateChange(newValue.format('YYYY-MM-DD'));
    }
  };

  return (
    <Paper elevation={2} sx={{ p: 2 }}>
      <Typography variant="h6" gutterBottom>
        Date Range
      </Typography>
      <Stack spacing={2}>
        <DatePicker
          label="Start Date"
          value={dayjs(startDate)}
          onChange={handleStartDateChange}
          minDate={minDateObj}
          maxDate={dayjs(endDate)}
          format="YYYY-MM-DD"
          slotProps={{
            textField: { fullWidth: true, size: 'small' },
          }}
        />
        <DatePicker
          label="End Date"
          value={dayjs(endDate)}
          onChange={handleEndDateChange}
          minDate={dayjs(startDate)}
          maxDate={maxDateObj}
          format="YYYY-MM-DD"
          slotProps={{
            textField: { fullWidth: true, size: 'small' },
          }}
        />
        <Typography variant="caption" color="text.secondary">
          Available data: {minDate} to {maxDate}
        </Typography>
      </Stack>
    </Paper>
  );
}
