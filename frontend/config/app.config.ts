/**
 * Application Configuration
 * Centralized configuration for easy customization
 */

export const appConfig = {
  // Application Info
  title: 'CHIRPS Precipitation Viewer',
  description: 'Interactive viewer for CHIRPS daily precipitation data',
  version: '1.0.0',
  
  // API Configuration
  api: {
    baseUrl: process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000',
    timeout: 30000, // 30 seconds
  },
  
  // Map Configuration
  map: {
    defaultCenter: {
      lat: 0,
      lng: 20,
    },
    defaultZoom: 4,
    minZoom: 2,
    maxZoom: 12,
    
    // Zoom level to resolution mapping (in degrees)
    zoomToResolution: {
      2: 5.0,
      3: 2.5,
      4: 1.5,
      5: 1.0,
      6: 0.5,
      7: 0.25,
      8: 0.15,
      9: 0.1,
      10: 0.05,
      11: 0.025,
      12: 0.01,
    },
    
    // Map style (can be changed to different tile providers)
    tileLayer: {
      url: 'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
    },
  },
  
  // Date Configuration
  date: {
    minDate: '2023-01-01',
    maxDate: '2025-12-31',
    defaultStartDate: '2024-01-01',
    defaultEndDate: '2024-12-31',
    format: 'YYYY-MM-DD',
  },
  
  // Visualization Configuration
  visualization: {
    // Default aggregation level
    defaultAggregation: 'monthly',
    
    // Available aggregation options
    aggregationLevels: [
      { value: 'daily', label: 'Daily', icon: '📅' },
      { value: 'weekly', label: 'Weekly', icon: '📊' },
      { value: 'monthly', label: 'Monthly', icon: '📈' },
      { value: 'yearly', label: 'Yearly', icon: '📉' },
    ],
    
    // Chart colors
    colors: {
      primary: '#1976d2',
      secondary: '#dc004e',
      precipitation: '#2196f3',
      background: '#f5f5f5',
      gridLines: '#e0e0e0',
    },
    
    // Chart dimensions
    chart: {
      height: 400,
      margin: { top: 20, right: 30, left: 50, bottom: 50 },
    },
  },
  
  // Theme Configuration
  theme: {
    mode: 'light', // 'light' or 'dark'
    primaryColor: '#1976d2',
    secondaryColor: '#dc004e',
    
    palette: {
      primary: {
        main: '#1976d2',
        light: '#42a5f5',
        dark: '#1565c0',
      },
      secondary: {
        main: '#dc004e',
        light: '#e33371',
        dark: '#9a0036',
      },
      success: {
        main: '#4caf50',
      },
      warning: {
        main: '#ff9800',
      },
      error: {
        main: '#f44336',
      },
      background: {
        default: '#fafafa',
        paper: '#ffffff',
      },
    },
    
    typography: {
      fontFamily: '"Roboto", "Helvetica", "Arial", sans-serif',
      h1: {
        fontSize: '2.5rem',
        fontWeight: 500,
      },
      h2: {
        fontSize: '2rem',
        fontWeight: 500,
      },
      h3: {
        fontSize: '1.75rem',
        fontWeight: 500,
      },
      h4: {
        fontSize: '1.5rem',
        fontWeight: 500,
      },
      h5: {
        fontSize: '1.25rem',
        fontWeight: 500,
      },
      h6: {
        fontSize: '1rem',
        fontWeight: 500,
      },
    },
  },
  
  // UI Configuration
  ui: {
    // Sidebar width
    sidebarWidth: 400,
    
    // Panel heights
    mapHeight: '50vh',
    chartHeight: 400,
    
    // Responsive breakpoints
    breakpoints: {
      xs: 0,
      sm: 600,
      md: 900,
      lg: 1200,
      xl: 1536,
    },
    
    // Loading indicators
    loading: {
      debounceDelay: 300, // ms
      spinnerSize: 40,
    },
  },
  
  // Features Configuration
  features: {
    enableDownload: true,
    enableStatistics: true,
    enableMultipleLocations: false, // Future feature
    enableComparison: false, // Future feature
  },
  
  // Data Configuration
  data: {
    // Default location for quick access
    defaultLocations: [
      { name: 'Angola', lat: -12.5, lng: 17.5 },
      { name: 'West Africa', lat: 17.5, lng: 3.5 },
      { name: 'Horn of Africa', lat: 6.5, lng: 42.5 },
      { name: 'Southern Africa', lat: -19.5, lng: 23.5 },
    ],
    
    // Precipitation thresholds for categorization
    precipitationThresholds: {
      dry: 1.0,        // < 1mm
      light: 5.0,      // 1-5mm
      moderate: 20.0,  // 5-20mm
      heavy: 50.0,     // 20-50mm
      veryHeavy: 100.0 // > 50mm
    },
  },
};

export type AppConfig = typeof appConfig;

export default appConfig;
