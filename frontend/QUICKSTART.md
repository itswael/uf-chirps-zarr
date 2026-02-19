# CHIRPS Precipitation Viewer - Quick Start Guide

Get up and running with the CHIRPS Precipitation Viewer in minutes!

## Prerequisites

✅ Node.js 18+ and npm installed
✅ Python 3.10+ installed
✅ CHIRPS Zarr data store created (see main project README)

## Step 1: Install Backend Dependencies

```bash
cd frontend/backend
pip install -r requirements.txt
```

## Step 2: Install Frontend Dependencies

```bash
cd ..
npm install
```

## Step 3: Configure Environment

Copy the example environment file:
```bash
cp .env.local.example .env.local
```

The default settings should work for local development.

## Step 4: Start the Backend Server

In one terminal:
```bash
cd frontend/backend
python main.py
# or
uvicorn main:app --reload --port 8000
```

The API will start at http://localhost:8000
Visit http://localhost:8000/docs for API documentation

## Step 5: Start the Frontend Server

In another terminal:
```bash
cd frontend
npm run dev
```

The application will start at http://localhost:3000

## Step 6: Start Exploring!

1. **Open your browser** to http://localhost:3000
2. **Click on the map** to select a location
3. **Adjust the date range** using the date pickers
4. **View precipitation data** in the chart
5. **Toggle aggregation levels** (daily/weekly/monthly/yearly)
6. **Download data** in ICASA format using the download button

## Using the Application

### Select a Location
- Click anywhere on the map
- Zoom in/out to change data resolution
- Selected coordinates appear in the sidebar

### Choose Date Range
- Use the date pickers to select start and end dates
- Data ranges from 2023-01-01 to 2025-12-31
- Data updates automatically when dates change

### View Visualizations
- **Daily**: Line chart showing detailed daily precipitation
- **Weekly**: Bar chart with weekly totals
- **Monthly**: Bar chart with monthly totals
- **Yearly**: Bar chart with yearly totals
- Hover over charts for exact values

### Download Data
- Select a location and date range
- Click "Download ICASA Format"
- File downloads automatically in ICASA weather format
- Use in agricultural models like DSSAT

## Customization

All settings are in `config/app.config.ts`:

### Change Default Map Location
```typescript
map: {
  defaultCenter: { lat: 10.0, lng: -5.0 },
  defaultZoom: 6,
}
```

### Change Colors
```typescript
theme: {
  palette: {
    primary: { main: '#your-color' },
  }
}
```

### Change Date Defaults
```typescript
date: {
  defaultStartDate: '2024-06-01',
  defaultEndDate: '2024-12-31',
}
```

## Troubleshooting

### Backend won't start
- Check Python version: `python --version` (should be 3.10+)
- Verify Zarr path in `client/config.py`
- Install dependencies: `pip install -r requirements.txt`

### Frontend shows errors
- Clear cache: `rm -rf .next node_modules`
- Reinstall: `npm install`
- Check Node version: `node --version` (should be 18+)

### Map doesn't appear
- Check browser console for errors
- Ensure you're accessing via http://localhost:3000
- Verify Leaflet CSS is loading

### No data loading
- Ensure backend is running on port 8000
- Check CORS settings in backend/main.py
- Verify Zarr data exists in `data/zarr/`

## Production Deployment

### Backend
```bash
# Install production server
pip install gunicorn

# Run with Gunicorn
gunicorn backend.main:app -w 4 -k uvicorn.workers.UvicornWorker
```

### Frontend
```bash
# Build for production
npm run build

# Start production server
npm start
```

### Environment Variables
Update `.env.local` with production API URL:
```
NEXT_PUBLIC_API_URL=https://your-api-domain.com
```

## Next Steps

- Explore different regions using the map
- Compare seasonal patterns with aggregation toggles
- Download data for your research area
- Customize colors and themes in config
- Add bookmarked locations in config

## Support

For issues:
1. Check [frontend/README.md](README.md) for detailed docs
2. Review API docs at http://localhost:8000/docs
3. Check browser console for errors
4. Verify backend logs for API issues

## Features Summary

✅ Interactive zoomable map
✅ Zoom-based resolution adjustment
✅ Date range selection
✅ Multi-level aggregation (daily → yearly)
✅ Interactive charts
✅ Statistical summaries
✅ ICASA format downloads
✅ Responsive mobile design
✅ Centralized configuration
✅ Production-ready architecture

Happy analyzing! 🌧️📊
