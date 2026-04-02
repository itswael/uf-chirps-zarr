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

  private getDownloadFilename(response: any, fallbackName: string): string {
    const headerValue = response?.headers?.['content-disposition'];
    if (typeof headerValue === 'string') {
      const filenameMatch = /filename\*=UTF-8''([^;]+)|filename="?([^";]+)"?/i.exec(headerValue);
      const encodedName = filenameMatch?.[1] || filenameMatch?.[2];
      if (encodedName) {
        try {
          return decodeURIComponent(encodedName);
        } catch {
          return encodedName;
        }
      }
    }
    return fallbackName;
  }

  /**
   * Get metadata about the Zarr store
   */
  async getMetadata() {
    const response = await this.client.get('/api/metadata');
    return response.data;
  }

  /**
   * Get available weather variables
   */
  async getVariables() {
    const response = await this.client.get('/api/variables');
    return response.data;
  }

  /**
   * Warm backend NASA POWER cache for a date range
   */
  async preloadWeatherCache(params: {
    start_date: string;
    end_date: string;
  }) {
    const response = await this.client.post('/api/data/preload-weather-cache', null, {
      params,
    });
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
   * Get time series for a specific variable (CHIRPS or NASA POWER)
   */
  async getTimeSeriesVariable(params: {
    lat: number;
    lon: number;
    start_date: string;
    end_date: string;
    variable: string;
    aggregation?: string;
  }) {
    const response = await this.client.post('/api/data/timeseries-variable', null, {
      params,
    });
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
    rain_source?: string;
    selected_parameters?: string[];
  }) {
    const queryParams: any = {
      ...params,
      selected_parameters: params.selected_parameters?.join(','),
    };

    const response = await this.client.post('/api/download/icasa', null, {
      params: queryParams,
      responseType: 'blob',
    });
    
    const filename = this.getDownloadFilename(
      response,
      `weather_${params.lat}_${params.lon}_${params.start_date}_${params.end_date}.WTH`
    );

    // Create download link
    const url = window.URL.createObjectURL(new Blob([response.data]));
    const link = document.createElement('a');
    link.href = url;
    link.setAttribute('download', filename);
    document.body.appendChild(link);
    link.click();
    link.remove();
    window.URL.revokeObjectURL(url);
  }

  /**
   * Validate a shapefile before processing
   */
  async validateShapefile(file: File, shxFile?: File, dbfFile?: File) {
    const formData = new FormData();
    formData.append('shapefile', file);
    if (shxFile) {
      formData.append('shapefile_shx', shxFile);
    }
    if (dbfFile) {
      formData.append('shapefile_dbf', dbfFile);
    }
    
    const response = await this.client.post('/api/validate-shapefile', formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
      },
    });
    return response.data;
  }

  /**
   * Download ICASA files for multiple points from spatial file (shapefile or geojson)
   */
  async downloadIcasaMulti(params: {
    file: File;
    shx_file?: File;
    dbf_file?: File;
    start_date: string;
    end_date: string;
    rain_source?: string;
    selected_parameters?: string[];
  }) {
    const formData = new FormData();
    formData.append('shapefile', params.file);
    if (params.shx_file) {
      formData.append('shapefile_shx', params.shx_file);
    }
    if (params.dbf_file) {
      formData.append('shapefile_dbf', params.dbf_file);
    }
    formData.append('start_date', params.start_date);
    formData.append('end_date', params.end_date);
    formData.append('rain_source', params.rain_source || 'chirps');
    if (params.selected_parameters && params.selected_parameters.length > 0) {
      formData.append('selected_parameters', params.selected_parameters.join(','));
    }

    const response = await this.client.post('/api/download/icasa-multi', formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
      },
      responseType: 'blob',
      timeout: 300000, // 5 minutes for large files
    });
    
    const filename = this.getDownloadFilename(
      response,
      `weather_data_${params.start_date}_${params.end_date}.zip`
    );

    // Create download link
    const url = window.URL.createObjectURL(new Blob([response.data]));
    const link = document.createElement('a');
    link.href = url;
    link.setAttribute('download', filename);
    document.body.appendChild(link);
    link.click();
    link.remove();
    window.URL.revokeObjectURL(url);
  }
}

export const apiClient = new ApiClient();
export default apiClient;
