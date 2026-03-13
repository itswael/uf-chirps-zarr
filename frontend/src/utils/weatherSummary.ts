interface WeatherSummaryInput {
  selectedVariable: string;
  variableName: string;
  units: string;
  aggregation: string;
  values: Array<number | null | undefined>;
}

const RAIN_VARIABLES = new Set(['RAIN1', 'RAIN2', 'RAIN']);

function formatNumber(value: number): string {
  return Number.isInteger(value) ? value.toString() : value.toFixed(1);
}

function getRainCondition(total: number): string {
  if (total > 20) return 'heavy rainfall';
  if (total > 5) return 'scattered showers';
  return 'mostly dry conditions';
}

function getTemperatureCondition(mean: number): string {
  if (mean >= 30) return 'consistently hot conditions';
  if (mean <= 15) return 'cool conditions';
  return 'moderate temperatures';
}

function getWindCondition(mean: number): string {
  if (mean >= 8) return 'windy conditions';
  if (mean >= 4) return 'light to moderate winds';
  return 'calm conditions';
}

function getRelativePattern(variable: string, mean: number): string {
  if (variable === 'RH2M') {
    if (mean >= 80) return 'high humidity levels';
    if (mean <= 40) return 'dry air conditions';
    return 'balanced humidity levels';
  }

  if (variable === 'SRAD') {
    if (mean >= 20) return 'strong solar radiation';
    if (mean <= 10) return 'lower solar radiation';
    return 'moderate solar radiation';
  }

  return 'stable conditions';
}

function getPeriodLabel(aggregation: string): string {
  if (aggregation === 'monthly') return 'month';
  if (aggregation === 'weekly') return 'week';
  if (aggregation === 'yearly') return 'year';
  return 'period';
}

export function generateWeatherSummary({
  selectedVariable,
  variableName,
  units,
  aggregation,
  values,
}: WeatherSummaryInput): string {
  const numericValues = values.filter((value): value is number => typeof value === 'number' && Number.isFinite(value));

  if (numericValues.length === 0) {
    return 'No valid observations are available for this selection yet.';
  }

  const mean = numericValues.reduce((sum, value) => sum + value, 0) / numericValues.length;
  const max = Math.max(...numericValues);
  const min = Math.min(...numericValues);
  const periodLabel = getPeriodLabel(aggregation);
  const isRain = RAIN_VARIABLES.has(selectedVariable);
  const sampleCount = numericValues.length;

  if (isRain) {
    const total = numericValues.reduce((sum, value) => sum + value, 0);
    const wetIntervals = numericValues.filter((value) => value >= 0.1).length;
    const rainCondition = getRainCondition(total);

    return (
      `This ${periodLabel} recorded an average precipitation of ${formatNumber(mean)} ${units}, ` +
      `with peak values reaching ${formatNumber(max)} ${units} and a minimum of ${formatNumber(min)} ${units}. ` +
      `Cumulative precipitation was ${formatNumber(total)} ${units} across ${sampleCount} observations, ` +
      `including ${wetIntervals} wet intervals. ` +
      `Overall, the period shows ${rainCondition}.`
    );
  }

  if (selectedVariable === 'TMAX' || selectedVariable === 'TMIN' || selectedVariable === 'T2M') {
    const tempCondition = getTemperatureCondition(mean);
    return (
      `For ${variableName}, the ${periodLabel} averaged ${formatNumber(mean)} ${units}. ` +
      `Values ranged from ${formatNumber(min)} ${units} to ${formatNumber(max)} ${units} ` +
      `over ${sampleCount} observations. ` +
      `This indicates ${tempCondition} for the selected location and time window.`
    );
  }

  if (selectedVariable === 'WIND' || selectedVariable === 'WS2M') {
    const windCondition = getWindCondition(mean);
    return (
      `For ${variableName}, the ${periodLabel} had an average of ${formatNumber(mean)} ${units}, ` +
      `with a range between ${formatNumber(min)} ${units} and ${formatNumber(max)} ${units}. ` +
      `Across ${sampleCount} observations, this suggests ${windCondition}.`
    );
  }

  const relativePattern = getRelativePattern(selectedVariable, mean);
  return (
    `For ${variableName}, the ${periodLabel} average was ${formatNumber(mean)} ${units}, ` +
    `with values spanning ${formatNumber(min)} to ${formatNumber(max)} ${units}. ` +
    `A total of ${sampleCount} observations were analyzed, indicating ${relativePattern}.`
  );
}
