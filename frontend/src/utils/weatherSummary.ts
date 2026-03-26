interface WeatherSummaryInput {
  selectedVariable: string;
  variableName: string;
  units: string;
  aggregation: string;
  values: Array<number | null | undefined>;
}

export interface WeatherSummarySource {
  label: string;
  url: string;
}

export interface WeatherSummaryResult {
  summary: string;
  sources: WeatherSummarySource[];
}

const RAIN_VARIABLES = new Set(['RAIN1', 'RAIN2', 'RAIN']);

const URLS = {
  rainIntensity:
    'https://wgntv.com/weather/weather-blog/do-meteorologists-have-specific-definitions-for-drizzle-light-rain-steady-rain-heavy-rain-downpour/',
  heavyGlossary: 'https://forecast.weather.gov/glossary.php?word=heavy',
  dewPointComfort:
    'https://www.weather.gov/arx/why_dewpoint_vs_humidity#:~:text=less%20than%20or%20equal%20to,in%20the%20air%2C%20becoming%20oppressive',
  windAdvisory:
    'https://www.weather.gov/dmx/dsswind#:~:text=Wind%20Information%20Page&text=A%20Wind%20Advisory%20means%20that,power%20lines%20and%20small%20structures.&text=Click%20each%20image%20above%20to,from%206pm%20to%206am%20CST.',
  temperatureBands: 'https://thinkmetric.uk/basics/temperature/',
  solarBands: 'https://energypedia.info/wiki/SPIS_Toolbox_-_Solar_Irradiation',
  humidityDefinition: 'https://forecast.weather.gov/glossary.php?word=humidity',
};

type RainClass = 'no measurable rain' | 'light rain' | 'moderate rain' | 'heavy rain' | 'violent rain';

function formatNumber(value: number): string {
  return Number.isInteger(value) ? value.toString() : value.toFixed(1);
}

function toMph(value: number, units: string): number {
  const u = units.toLowerCase();
  if (u.includes('mph')) return value;
  if (u.includes('m/s') || u.includes('mps')) return value * 2.2369362921;
  if (u.includes('km/h') || u.includes('kph')) return value * 0.621371;
  if (u.includes('knot') || u.includes('kt')) return value * 1.150779;
  return value;
}

function toCelsius(value: number, units: string): number {
  const u = units.toLowerCase();
  if (u.includes('f')) return ((value - 32) * 5) / 9;
  return value;
}

function toFahrenheit(value: number, units: string): number {
  const u = units.toLowerCase();
  if (u.includes('c')) return (value * 9) / 5 + 32;
  return value;
}

function toKwhPerM2Day(value: number, units: string): number {
  const u = units.toLowerCase();
  if (u.includes('kwh')) return value;
  if (u.includes('mj')) return value / 3.6;
  if (u.includes('wh')) return value / 1000;
  return value;
}

function getHoursForAggregation(aggregation: string): number {
  if (aggregation === 'daily') return 24;
  if (aggregation === 'weekly') return 24 * 7;
  if (aggregation === 'monthly') return 24 * 30.4375;
  if (aggregation === 'yearly') return 24 * 365.25;
  return 24;
}

function classifyRainByRateMmHr(rateMmHr: number): RainClass {
  if (rateMmHr <= 0) return 'no measurable rain';
  if (rateMmHr <= 2.5) return 'light rain';
  if (rateMmHr <= 7.6) return 'moderate rain';
  if (rateMmHr <= 50) return 'heavy rain';
  return 'violent rain';
}

function getDominantRainClass(counts: Record<RainClass, number>): RainClass {
  const entries = Object.entries(counts) as Array<[RainClass, number]>;
  entries.sort((a, b) => b[1] - a[1]);
  return entries[0][0];
}

function getTemperatureBand(celsius: number): string {
  if (celsius <= 0) return 'freezing/icy';
  if (celsius <= 10) return 'very cold';
  if (celsius <= 15) return 'cold';
  if (celsius <= 20) return 'cool';
  if (celsius <= 25) return 'normal/warm';
  if (celsius <= 35) return 'hot';
  if (celsius <= 40) return 'very hot';
  if (celsius >= 50) return 'extreme heat';
  return 'very warm';
}

function getDewPointComfort(fahrenheit: number): string {
  if (fahrenheit <= 55) return 'dry and comfortable';
  if (fahrenheit < 65) return 'becoming sticky';
  return 'oppressive moisture';
}

function getWindHazardLabel(mph: number): string {
  if (mph >= 58) return 'high wind warning-level threshold met';
  if (mph >= 40) return 'high wind watch/warning threshold met';
  if (mph >= 30) return 'wind advisory-level threshold met';
  if (mph >= 20) return 'windy conditions';
  return 'below advisory thresholds';
}

function getSolarBand(kwhPerM2Day: number): string {
  if (kwhPerM2Day < 2.6) return 'low';
  if (kwhPerM2Day < 3) return 'moderate';
  if (kwhPerM2Day <= 4) return 'high';
  return 'very high';
}

function getRelativeHumidityState(rh: number): string {
  if (rh < 30) return 'dry';
  if (rh < 60) return 'moderate';
  if (rh < 80) return 'humid';
  return 'very humid';
}

function uniqueSources(sources: WeatherSummarySource[]): WeatherSummarySource[] {
  const seen = new Set<string>();
  return sources.filter((source) => {
    if (seen.has(source.url)) return false;
    seen.add(source.url);
    return true;
  });
}

function getPeriodLabel(aggregation: string): string {
  if (aggregation === 'daily') return 'daily';
  if (aggregation === 'weekly') return 'weekly';
  if (aggregation === 'monthly') return 'monthly';
  if (aggregation === 'yearly') return 'yearly';
  return 'selected';
}

function getAggregationUnitLabel(aggregation: string): string {
  if (aggregation === 'daily') return 'day';
  if (aggregation === 'weekly') return 'week';
  if (aggregation === 'monthly') return 'month';
  if (aggregation === 'yearly') return 'year';
  return 'period';
}

function getOverallRainCategory(wetCoveragePct: number, dominantClass: RainClass): string {
  if (wetCoveragePct < 20) return 'largely dry';
  if (wetCoveragePct < 50) return 'intermittently wet';
  if (dominantClass === 'heavy rain' || dominantClass === 'violent rain') return 'persistently heavy rainfall';
  if (dominantClass === 'moderate rain') return 'moderate rainfall';
  if (dominantClass === 'light rain') return 'light rainfall';
  return 'mixed rainfall';
}

function getRainInsight(totalIntervals: number, dryIntervals: number, counts: Record<RainClass, number>): string {
  const dryPct = (dryIntervals / totalIntervals) * 100;
  const moderatePct = (counts['moderate rain'] / totalIntervals) * 100;
  const heavyPct = ((counts['heavy rain'] + counts['violent rain']) / totalIntervals) * 100;
  const wetPct = 100 - dryPct;

  if (moderatePct > 50) {
    return 'Rainfall was predominantly moderate, with occasional heavy events.';
  }
  if (dryPct > 50) {
    return 'The period was largely dry, with limited rainfall activity.';
  }
  if (wetPct > 50) {
    return 'Frequent wet conditions indicate a consistently rainy period.';
  }
  if (heavyPct > 50) {
    return 'Rainfall intensity was frequently heavy, suggesting elevated runoff risk periods.';
  }
  return 'Rainfall distribution was mixed, with no single intensity class dominating the selected period.';
}

function buildRainSummary(
  numericValues: number[],
  units: string,
  aggregation: string,
  periodLabel: string
): WeatherSummaryResult {
  const total = numericValues.reduce((sum, value) => sum + value, 0);
  const mean = total / numericValues.length;
  const max = Math.max(...numericValues);
  const min = Math.min(...numericValues);
  const hours = getHoursForAggregation(aggregation);
  const rates = numericValues.map((value) => value / hours);

  const counts: Record<RainClass, number> = {
    'no measurable rain': 0,
    'light rain': 0,
    'moderate rain': 0,
    'heavy rain': 0,
    'violent rain': 0,
  };

  rates.forEach((rate) => {
    const rainClass = classifyRainByRateMmHr(rate);
    counts[rainClass] += 1;
  });

  const wetIntervals = numericValues.filter((value) => value > 0).length;
  const dryIntervals = numericValues.length - wetIntervals;
  const wetCoveragePct = (wetIntervals / numericValues.length) * 100;
  const dominantClass = getDominantRainClass(counts);
  const heavyOrAbove = counts['heavy rain'] + counts['violent rain'];
  const overallCategory = getOverallRainCategory(wetCoveragePct, dominantClass);
  const unitLabel = getAggregationUnitLabel(aggregation);
  const title = `${periodLabel.charAt(0).toUpperCase()}${periodLabel.slice(1)} Summary`;
  const insight = getRainInsight(numericValues.length, dryIntervals, counts);

  const heavyLine = heavyOrAbove > 0 ? `${heavyOrAbove} heavy rainfall ${unitLabel}s` : null;
  const moderateLine = counts['moderate rain'] > 0 ? `${counts['moderate rain']} moderate rainfall ${unitLabel}s` : null;
  const lightLine = counts['light rain'] > 0 ? `${counts['light rain']} light rainfall ${unitLabel}s` : null;
  const intensityLines = [heavyLine, moderateLine, lightLine].filter((line): line is string => Boolean(line));

  let openingLine = '';
  let scopeLine = '';
  let distributionHeader = '';

  if (aggregation === 'daily') {
    openingLine =
      `During the selected period, ${overallCategory} conditions were observed, with an average rainfall of ${formatNumber(mean)} ${units}.`;
    scopeLine = `Out of ${numericValues.length} days:`;
    distributionHeader = 'Rainfall intensity distribution:';
  } else if (aggregation === 'weekly') {
    openingLine =
      `Over the selected period, rainfall patterns indicate ${overallCategory} conditions, with an average weekly rainfall of ${formatNumber(mean)} ${units}.`;
    scopeLine = `Across ${numericValues.length} weeks:`;
    distributionHeader = 'Weekly intensity breakdown:';
  } else if (aggregation === 'monthly') {
    openingLine =
      `For the selected time range, the region experienced ${overallCategory} rainfall conditions, with an average of ${formatNumber(mean)} ${units}.`;
    scopeLine = `Summary across ${numericValues.length} months:`;
    distributionHeader = 'Monthly distribution:';
  } else if (aggregation === 'yearly') {
    openingLine =
      `Over the analyzed years, rainfall trends show ${overallCategory} conditions, with an annual average of ${formatNumber(mean)} ${units}.`;
    scopeLine = `Across ${numericValues.length} years:`;
    distributionHeader = 'Yearly breakdown:';
  } else {
    openingLine =
      `Across the selected period, ${overallCategory} conditions were observed, with average rainfall of ${formatNumber(mean)} ${units}.`;
    scopeLine = `Across ${numericValues.length} periods:`;
    distributionHeader = 'Intensity distribution:';
  }

  const summaryParts: string[] = [
    title,
    openingLine,
    '',
    scopeLine,
    `${wetIntervals} ${unitLabel}s recorded rainfall`,
    `${dryIntervals} ${unitLabel}s remained dry`,
    '',
    distributionHeader,
  ];

  if (intensityLines.length > 0) {
    summaryParts.push(...intensityLines);
  } else {
    summaryParts.push(`No light, moderate, or heavy rainfall ${unitLabel}s were recorded.`);
  }

  summaryParts.push(
    '',
    `1-line insight: ${insight}`,
    '',
    `Technical note: dominant interval class = ${dominantClass}; min = ${formatNumber(min)} ${units}, max = ${formatNumber(max)} ${units}, total = ${formatNumber(total)} ${units}, wet coverage = ${formatNumber(wetCoveragePct)}%.`
  );

  return {
    summary: summaryParts.join('\n'),
    sources: uniqueSources([
      { label: 'Rain Intensity Definitions', url: URLS.rainIntensity },
      { label: 'NWS Heavy Glossary', url: URLS.heavyGlossary },
    ]),
  };
}

function buildTemperatureSummary(
  variableName: string,
  units: string,
  numericValues: number[],
  periodLabel: string
): WeatherSummaryResult {
  const meanRaw = numericValues.reduce((sum, value) => sum + value, 0) / numericValues.length;
  const maxRaw = Math.max(...numericValues);
  const minRaw = Math.min(...numericValues);

  const meanC = toCelsius(meanRaw, units);
  const maxC = toCelsius(maxRaw, units);
  const minC = toCelsius(minRaw, units);

  return {
    summary:
      `At ${periodLabel} aggregation for ${variableName}, the mean was ${formatNumber(meanRaw)} ${units} ` +
      `(range ${formatNumber(minRaw)} to ${formatNumber(maxRaw)} ${units}; ${numericValues.length} intervals). ` +
      `On a Celsius reference scale, this corresponds to mean ${formatNumber(meanC)} C ` +
      `(range ${formatNumber(minC)} to ${formatNumber(maxC)} C), classified as ${getTemperatureBand(meanC)} for this aggregation.`,
    sources: [{ label: 'Temperature Category Bands', url: URLS.temperatureBands }],
  };
}

function buildWindSummary(
  variableName: string,
  units: string,
  numericValues: number[],
  periodLabel: string
): WeatherSummaryResult {
  const meanRaw = numericValues.reduce((sum, value) => sum + value, 0) / numericValues.length;
  const maxRaw = Math.max(...numericValues);
  const minRaw = Math.min(...numericValues);
  const meanMph = toMph(meanRaw, units);
  const maxMph = toMph(maxRaw, units);

  return {
    summary:
      `At ${periodLabel} aggregation for ${variableName}, the mean wind was ${formatNumber(meanRaw)} ${units} ` +
      `(range ${formatNumber(minRaw)} to ${formatNumber(maxRaw)} ${units}; ${numericValues.length} intervals). ` +
      `Converted to mph, mean was ${formatNumber(meanMph)} mph and max was ${formatNumber(maxMph)} mph; ` +
      `${getWindHazardLabel(maxMph)} based on NWS advisory/watch/warning thresholds.`,
    sources: [{ label: 'NWS Wind Advisory/Warning Thresholds', url: URLS.windAdvisory }],
  };
}

function buildDewPointSummary(
  variableName: string,
  units: string,
  numericValues: number[],
  periodLabel: string
): WeatherSummaryResult {
  const meanRaw = numericValues.reduce((sum, value) => sum + value, 0) / numericValues.length;
  const maxRaw = Math.max(...numericValues);
  const minRaw = Math.min(...numericValues);
  const meanF = toFahrenheit(meanRaw, units);

  return {
    summary:
      `At ${periodLabel} aggregation for ${variableName}, the mean dew point was ${formatNumber(meanRaw)} ${units} ` +
      `(range ${formatNumber(minRaw)} to ${formatNumber(maxRaw)} ${units}; ${numericValues.length} intervals). ` +
      `Converted to Fahrenheit, mean dew point was ${formatNumber(meanF)} F, indicating ${getDewPointComfort(meanF)} conditions for this aggregation.`,
    sources: [{ label: 'NWS Dew Point Comfort Guidance', url: URLS.dewPointComfort }],
  };
}

function buildHumiditySummary(
  variableName: string,
  units: string,
  numericValues: number[],
  periodLabel: string
): WeatherSummaryResult {
  const mean = numericValues.reduce((sum, value) => sum + value, 0) / numericValues.length;
  const max = Math.max(...numericValues);
  const min = Math.min(...numericValues);

  return {
    summary:
      `At ${periodLabel} aggregation for ${variableName}, the mean was ${formatNumber(mean)} ${units} ` +
      `(range ${formatNumber(min)} to ${formatNumber(max)} ${units}; ${numericValues.length} intervals). ` +
      `Relative humidity state for this aggregation is ${getRelativeHumidityState(mean)} based on mean RH.`,
    sources: [{ label: 'NWS Humidity Definition', url: URLS.humidityDefinition }],
  };
}

function buildSolarSummary(
  variableName: string,
  units: string,
  numericValues: number[],
  periodLabel: string
): WeatherSummaryResult {
  const meanRaw = numericValues.reduce((sum, value) => sum + value, 0) / numericValues.length;
  const maxRaw = Math.max(...numericValues);
  const minRaw = Math.min(...numericValues);
  const meanKwh = toKwhPerM2Day(meanRaw, units);
  const maxKwh = toKwhPerM2Day(maxRaw, units);
  const minKwh = toKwhPerM2Day(minRaw, units);
  const band = getSolarBand(meanKwh);

  return {
    summary:
      `At ${periodLabel} aggregation for ${variableName}, the mean was ${formatNumber(meanRaw)} ${units} ` +
      `(range ${formatNumber(minRaw)} to ${formatNumber(maxRaw)} ${units}; ${numericValues.length} intervals). ` +
      `Converted to kWh/m2/day-equivalent, mean was ${formatNumber(meanKwh)} (range ${formatNumber(minKwh)} to ${formatNumber(maxKwh)}), ` +
      `which is classified as ${band} solar irradiation for this aggregation.`,
    sources: [{ label: 'Solar Irradiation Classes', url: URLS.solarBands }],
  };
}

export function generateWeatherSummaryWithSources({
  selectedVariable,
  variableName,
  units,
  aggregation,
  values,
}: WeatherSummaryInput): WeatherSummaryResult {
  const numericValues = values.filter((value): value is number => typeof value === 'number' && Number.isFinite(value));

  if (numericValues.length === 0) {
    return {
      summary: 'No valid observations are available for this selection yet.',
      sources: [],
    };
  }

  const periodLabel = getPeriodLabel(aggregation);

  if (RAIN_VARIABLES.has(selectedVariable)) {
    return buildRainSummary(numericValues, units, aggregation, periodLabel);
  }

  if (selectedVariable === 'TMAX' || selectedVariable === 'TMIN' || selectedVariable === 'T2M') {
    return buildTemperatureSummary(variableName, units, numericValues, periodLabel);
  }

  if (selectedVariable === 'WIND' || selectedVariable === 'WS2M') {
    return buildWindSummary(variableName, units, numericValues, periodLabel);
  }

  if (selectedVariable === 'TDEW') {
    return buildDewPointSummary(variableName, units, numericValues, periodLabel);
  }

  if (selectedVariable === 'RH2M') {
    return buildHumiditySummary(variableName, units, numericValues, periodLabel);
  }

  if (selectedVariable === 'SRAD') {
    return buildSolarSummary(variableName, units, numericValues, periodLabel);
  }

  const mean = numericValues.reduce((sum, value) => sum + value, 0) / numericValues.length;
  const max = Math.max(...numericValues);
  const min = Math.min(...numericValues);

  return {
    summary:
      `At ${periodLabel} aggregation for ${variableName}, the mean was ${formatNumber(mean)} ${units} ` +
      `(range ${formatNumber(min)} to ${formatNumber(max)} ${units}; ${numericValues.length} intervals). ` +
      `No specialized meteorological threshold set is currently mapped for ${selectedVariable}.`,
    sources: [],
  };
}

export function generateWeatherSummary(input: WeatherSummaryInput): string {
  return generateWeatherSummaryWithSources(input).summary;
}
