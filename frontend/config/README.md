# Frontend Application Configuration

This directory contains all configuration files for the CHIRPS Precipitation Viewer application.

## Files

### `app.config.ts`
Main application configuration file. Contains settings for:
- API endpoints
- Map configuration (default center, zoom levels, tile providers)
- Date ranges
- Visualization settings (colors, chart dimensions)
- Theme configuration (colors, typography)
- UI settings (sidebar width, breakpoints)
- Feature flags
- Data defaults

## How to Customize

### Changing Map Defaults
Edit the `map` section in `app.config.ts`:
```typescript
map: {
  defaultCenter: { lat: 0, lng: 20 },  // Change default map center
  defaultZoom: 4,                      // Change default zoom level
  // ...
}
```

### Changing Color Scheme
Edit the `theme.palette` section:
```typescript
theme: {
  palette: {
    primary: { main: '#1976d2' },  // Change primary color
    // ...
  }
}
```

### Changing Date Range
Edit the `date` section:
```typescript
date: {
  minDate: '2023-01-01',
  maxDate: '2025-12-31',
  // ...
}
```

### Adding Default Locations
Edit the `data.defaultLocations` array:
```typescript
data: {
  defaultLocations: [
    { name: 'Custom Location', lat: 10.0, lng: 20.0 },
    // ...
  ]
}
```

### Changing API URL
Edit `.env.local` in the frontend directory:
```
NEXT_PUBLIC_API_URL=http://your-api-url:8000
```

## Environment Variables

- `NEXT_PUBLIC_API_URL`: Backend API URL (default: http://localhost:8000)

## Notes

- All configuration is centralized for easy maintenance
- Changes to this configuration do not require code changes elsewhere
- Use TypeScript for type safety when accessing config values
