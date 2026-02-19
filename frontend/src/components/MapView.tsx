'use client';

import { useEffect, useRef, useState } from 'react';
import { Paper, Box, Typography } from '@mui/material';
import appConfig from '@/config/app.config';

interface MapViewProps {
  onLocationSelect: (lat: number, lon: number, zoom: number) => void;
  selectedLocation?: { lat: number; lon: number };
}

export default function MapView({ onLocationSelect, selectedLocation }: MapViewProps) {
  const mapRef = useRef<HTMLDivElement>(null);
  const [mapInstance, setMapInstance] = useState<any>(null);
  const markerRef = useRef<any>(null);

  useEffect(() => {
    // Dynamically load Leaflet CSS
    if (typeof window !== 'undefined' && !document.getElementById('leaflet-css')) {
      const link = document.createElement('link');
      link.id = 'leaflet-css';
      link.rel = 'stylesheet';
      link.href = 'https://unpkg.com/leaflet@1.9.4/dist/leaflet.css';
      link.integrity = 'sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY=';
      link.crossOrigin = '';
      document.head.appendChild(link);
    }
  }, []);

  useEffect(() => {
    // Dynamic import of Leaflet to avoid SSR issues
    if (typeof window !== 'undefined' && mapRef.current && !mapInstance) {
      import('leaflet').then((L) => {
        // Fix for default marker icon
        delete (L.Icon.Default.prototype as any)._getIconUrl;
        L.Icon.Default.mergeOptions({
          iconRetinaUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon-2x.png',
          iconUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon.png',
          shadowUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png',
        });

        const map = L.map(mapRef.current!, {
          center: [appConfig.map.defaultCenter.lat, appConfig.map.defaultCenter.lng],
          zoom: appConfig.map.defaultZoom,
          minZoom: appConfig.map.minZoom,
          maxZoom: appConfig.map.maxZoom,
        });

        // Use a more reliable tile provider
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
          attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
          maxZoom: 19,
        }).addTo(map);

        // Add click handler
        map.on('click', (e: any) => {
          const { lat, lng } = e.latlng;
          const zoom = map.getZoom();
          onLocationSelect(lat, lng, zoom);
        });

        // Add zoom handler to update resolution
        map.on('zoomend', () => {
          if (selectedLocation) {
            const zoom = map.getZoom();
            onLocationSelect(selectedLocation.lat, selectedLocation.lon, zoom);
          }
        });

        setMapInstance(map);
      });
    }
  }, []);

  useEffect(() => {
    if (mapInstance && selectedLocation) {
      import('leaflet').then((L) => {
        // Remove existing marker if it exists
        if (markerRef.current) {
          markerRef.current.remove();
          markerRef.current = null;
        }

        // Add new marker
        const marker = L.marker([selectedLocation.lat, selectedLocation.lon])
          .addTo(mapInstance)
          .bindPopup(
            `<b>Selected Location</b><br>
             Lat: ${selectedLocation.lat.toFixed(4)}°<br>
             Lon: ${selectedLocation.lon.toFixed(4)}°`
          )
          .openPopup();

        markerRef.current = marker;

        // Pan to location
        mapInstance.setView([selectedLocation.lat, selectedLocation.lon], mapInstance.getZoom());
      });
    }
  }, [selectedLocation, mapInstance]);

  return (
    <Paper elevation={3} sx={{ height: '100%', overflow: 'hidden' }}>
      <Box sx={{ p: 2, bgcolor: 'primary.main', color: 'white' }}>
        <Typography variant="h6">Interactive Map</Typography>
        <Typography variant="caption">
          Click anywhere to select a location. Zoom changes data resolution.
        </Typography>
      </Box>
      <Box
        ref={mapRef}
        sx={{
          height: 'calc(100% - 80px)',
          width: '100%',
          '& .leaflet-container': {
            height: '100%',
            width: '100%',
          },
        }}
      />
    </Paper>
  );
}
