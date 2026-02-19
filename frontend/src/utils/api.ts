import axios, { AxiosInstance } from 'axios';
import appConfig from '@/config/app.config';

/**
 * API Client for communicating with the CHIRPS backend
 */
class ApiClient {
  private client: AxiosInstance;

  constructor() {
    this.client = axios.create({
      baseURL: appConfig.api.baseUrl,
      timeout: appConfig.api.timeout,
      headers: {
        'Content-Type': 'application/json',
      },
    });
  }

  /**
   * Get metadata about the Zarr store
   */
  async getMetadata() {
    const response = await this.client.get('/api/metadata');
    return response.data;
  }

  /**
   * Get time series data for a location
   */
  async getTimeSeries(params: {
    bounds: {
      lon_min: number;
      lon_max: number;
      lat_min: number;
      lat_max: number;
    };
    date_range: {
      start_date: string;
      end_date: string;
    };
    aggregation?: string;
  }) {
    const response = await this.client.post('/api/data/timeseries', params);
    return response.data;
  }

  /**
   * Get statistical summary for a location
   */
  async getStatistics(params: {
    bounds: {
      lon_min: number;
      lon_max: number;
      lat_min: number;
      lat_max: number;
    };
    date_range: {
      start_date: string;
      end_date: string;
    };
  }) {
    const response = await this.client.post('/api/data/statistics', params);
    return response.data;
  }

  /**
   * Get spatial data around a point
   */
  async getSpatialData(params: {
    lat: number;
    lon: number;
    start_date: string;
    end_date: string;
    resolution?: number;
  }) {
    const response = await this.client.post('/api/data/spatial', null, {
      params,
    });
    return response.data;
  }

  /**
   * Download data in ICASA format
   */
  async downloadIcasa(params: {
    lat: number;
    lon: number;
    start_date: string;
    end_date: string;
  }) {
    const response = await this.client.post('/api/download/icasa', null, {
      params,
      responseType: 'blob',
    });
    
    // Create download link
    const url = window.URL.createObjectURL(new Blob([response.data]));
    const link = document.createElement('a');
    link.href = url;
    link.setAttribute(
      'download',
      `weather_${params.lat}_${params.lon}_${params.start_date}_${params.end_date}.txt`
    );
    document.body.appendChild(link);
    link.click();
    link.remove();
    window.URL.revokeObjectURL(url);
  }
}

export const apiClient = new ApiClient();
export default apiClient;
