'use client';

import { createTheme, ThemeProvider as MuiThemeProvider } from '@mui/material/styles';
import CssBaseline from '@mui/material/CssBaseline';
import { LocalizationProvider } from '@mui/x-date-pickers/LocalizationProvider';
import { AdapterDayjs } from '@mui/x-date-pickers/AdapterDayjs';
import appConfig from '@/config/app.config';

const theme = createTheme({
  palette: {
    mode: appConfig.theme.mode as 'light' | 'dark',
    primary: appConfig.theme.palette.primary,
    secondary: appConfig.theme.palette.secondary,
    success: appConfig.theme.palette.success,
    warning: appConfig.theme.palette.warning,
    error: appConfig.theme.palette.error,
    background: appConfig.theme.palette.background,
  },
  typography: {
    fontFamily: appConfig.theme.typography.fontFamily,
    h1: appConfig.theme.typography.h1,
    h2: appConfig.theme.typography.h2,
    h3: appConfig.theme.typography.h3,
    h4: appConfig.theme.typography.h4,
    h5: appConfig.theme.typography.h5,
    h6: appConfig.theme.typography.h6,
  },
  breakpoints: {
    values: appConfig.ui.breakpoints,
  },
});

export default function ThemeProvider({ children }: { children: React.ReactNode }) {
  return (
    <MuiThemeProvider theme={theme}>
      <CssBaseline />
      <LocalizationProvider dateAdapter={AdapterDayjs}>
        {children}
      </LocalizationProvider>
    </MuiThemeProvider>
  );
}
