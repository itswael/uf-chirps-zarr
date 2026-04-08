# CHIRPS Precipitation Viewer

A modern, interactive web application for visualizing and downloading CHIRPS (Climate Hazards Group InfraRed Precipitation with Station data) precipitation data.

## Features

✨ **Interactive Map**
- Click anywhere to select a location
- Zoom-based resolution adjustment
- Real-time location markers

📊 **Dynamic Visualizations**
- Toggle between daily, weekly, monthly, and yearly aggregations
- Interactive charts with zoom and pan capabilities
- Automatic chart type switching (line for daily, bar for aggregated)

📈 **Statistical Insights**
- Total precipitation
- Mean, median, max, min daily values
- Standard deviation
- Days with rain vs dry days

📥 **ICASA Format Download**
- Export data in ICASA weather format
- Automatic date formatting (YYYYDDD)
- Ready for agricultural modeling
- Single-point files use deterministic 8-character hash IDs
- Multi-point ZIP supports shapefile/GeoJSON uploads and ID-aware filenames

🎨 **Modern UI**
- Material-UI design system
- Responsive layout (mobile & desktop)
- Clean, professional interface
- Configurable theme

## Architecture

```
frontend/
├── backend/              # FastAPI backend for Zarr data access
│   ├── main.py          # API endpoints
│   └── requirements.txt # Python dependencies
├── config/              # Centralized configuration
│   ├── app.config.ts   # Application settings
│   └── README.md       # Configuration guide
├── src/
│   ├── app/            # Next.js app directory
│   │   ├── layout.tsx  # Root layout
│   │   ├── page.tsx    # Main application page
│   │   └── globals.css # Global styles
│   ├── components/     # React components
│   │   ├── MapView.tsx           # Interactive Leaflet map
│   │   ├── DateRangeSelector.tsx # Date picker
│   │   ├── PrecipitationChart.tsx # Chart visualization
│   │   ├── StatisticsPanel.tsx   # Statistics display
│   │   ├── DownloadPanel.tsx     # Download functionality
│   │   └── ThemeProvider.tsx     # Material-UI theme
│   └── utils/          # Utilities
│       └── api.ts      # API client
├── package.json        # Node.js dependencies
├── tsconfig.json       # TypeScript configuration
└── next.config.js      # Next.js configuration
```

## Installation

### Prerequisites

- Node.js 18+ and npm
- Python 3.10+
- CHIRPS Zarr data store (see main project README)

### Backend Setup

1. Navigate to the backend directory:
```bash
cd frontend/backend
```

2. Install Python dependencies:
```bash
pip install -r requirements.txt
```

3. Start the FastAPI server:
```bash
uvicorn main:app --reload --port 8000
```

Or use the npm script:
```bash
cd ..
npm run backend
```

The API will be available at http://localhost:8000

### Frontend Setup

1. Navigate to the frontend directory:
```bash
cd frontend
```

2. Install Node.js dependencies:
```bash
npm install
```

3. Start the development server:
```bash
npm run dev
```

The application will be available at http://localhost:3000

## Usage

### Selecting a Location

1. Click anywhere on the interactive map to select a location
2. The selected coordinates will appear in the sidebar
3. Zoom in/out to adjust the spatial resolution of the data

### Choosing a Date Range

1. Use the date pickers in the sidebar to select start and end dates
2. Available data ranges from 2023-01-01 to 2025-12-31
3. Data will automatically refresh when dates change

### Viewing Visualizations

1. The chart shows precipitation time series for the selected location
2. Toggle between aggregation levels:
   - **Daily**: Line chart showing daily values
   - **Weekly**: Bar chart showing weekly totals
   - **Monthly**: Bar chart showing monthly totals
   - **Yearly**: Bar chart showing yearly totals
3. Hover over the chart for detailed values
4. The chart is fully interactive with zoom and pan capabilities

### Downloading Data

1. Select a location and date range
2. Click the "Download ICASA Format" button
3. The data will be downloaded as a text file in ICASA weather format
4. Single-point file naming: `{HASH8}.WTH`

For multi-point downloads:
- If uploaded features contain IDs (`id`, `point_id`, `pid`, `cell_id`), those IDs are used for filenames
- If IDs are missing, deterministic hash IDs are generated from latitude/longitude
- When fallback IDs are generated, a GeoJSON ID manifest is included under `shapefile/`

**ICASA Format Details:**
- Date format: YYYYDDD (Year + Day of Year, e.g., 2024001 for Jan 1, 2024)
- Coordinates: Rounded to 1 decimal place
- Precipitation: Daily values in mm/day
- Compatible with DSSAT and other agricultural models

## Configuration

All application settings are centralized in `config/app.config.ts`. You can easily customize:

- **Map settings**: Default center, zoom levels, tile provider
- **Date ranges**: Min/max dates, default selections
- **Colors**: Chart colors, theme palette
- **UI dimensions**: Sidebar width, chart height, breakpoints
- **Feature flags**: Enable/disable features

See [config/README.md](config/README.md) for detailed customization guide.

### Example: Changing Map Tile Provider

Edit `config/app.config.ts`:
```typescript
map: {
  tileLayer: {
    url: 'https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png',
    attribution: 'Map data: © OpenTopoMap contributors',
  },
}
```

### Example: Changing Default Location

```typescript
map: {
  defaultCenter: {
    lat: 10.0,  // Your latitude
    lng: -5.0,  // Your longitude
  },
  defaultZoom: 6,
}
```

## API Endpoints

The backend provides the following REST API endpoints:

- `POST /api/data/preload-weather-cache` - Warm NASA POWER cache for date ranges
- `GET /api/metadata` - Get Zarr store metadata
- `GET /api/variables` - Get available weather variables
- `POST /api/data/timeseries` - Get precipitation time series
- `POST /api/data/timeseries-variable` - Get time series for selected variable
- `POST /api/data/statistics` - Get statistical summary
- `POST /api/data/spatial` - Get spatial precipitation data
- `POST /api/download/icasa` - Download single-point ICASA file
- `POST /api/download/icasa-multi` - Download multi-point ICASA ZIP from spatial upload
- `POST /api/validate-shapefile` - Validate shapefile/GeoJSON upload before processing

See API documentation at http://localhost:8000/docs when the backend is running.

## Development

### Scripts

```bash
npm run dev      # Start Next.js dev server
npm run build    # Build for production
npm run start    # Start production server
npm run lint     # Run ESLint
npm run backend  # Start FastAPI backend
```

### Technology Stack

**Frontend:**
- Next.js 14 (React framework)
- TypeScript (Type safety)
- Material-UI (Component library)
- Leaflet (Interactive maps)
- Recharts (Data visualization)
- Axios (HTTP client)

**Backend:**
- FastAPI (Python web framework)
- Xarray (N-dimensional arrays)
- Zarr (Chunked array storage)
- NumPy (Numerical computing)

## Troubleshooting

### Backend won't start
- Ensure Python 3.10+ is installed
- Check that Zarr data path is correct in the client config
- Verify all Python dependencies are installed

### Frontend build errors
- Clear node_modules and reinstall: `rm -rf node_modules && npm install`
- Check that all peer dependencies are satisfied
- Ensure Node.js version is 18+

### Map not displaying
- Check browser console for errors
- Ensure Leaflet CSS is loaded in layout.tsx
- Verify network requests are not blocked

### Data not loading
- Ensure backend is running on port 8000
- Check CORS settings in backend/main.py
- Verify API_URL in .env.local

### Download not working
- Check browser's download settings
- Verify backend download endpoint is accessible
- Ensure location is selected before downloading

## Future Enhancements

Potential features for future versions:
- [ ] Multiple location comparison
- [ ] Export to additional formats (CSV, NetCDF)
- [ ] Anomaly detection and alerts
- [ ] Historical climatology comparison
- [ ] Custom spatial aggregation (polygons)
- [ ] Batch downloads
- [ ] User authentication
- [ ] Saved locations/bookmarks

## License

See main project LICENSE file.

## Support

For issues and questions:
1. Check this README and configuration docs
2. Review API documentation at /docs
3. Check browser console for errors
4. Review backend logs for API errors

## Contributing

Contributions are welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

---

Built with ❤️ using Next.js and FastAPI
