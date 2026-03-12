import { query } from "./motherduck";
import type {
  WeeklyDownload,
  MonthlyDownload,
  RefreshDate,
  TotalDownloads,
  VersionDownload,
  PythonVersionDownload,
  CountryDownload,
  VersionAdoption,
  DailyDownload,
  DashboardData,
} from "./types";

export async function getWeeklyTimeSeries(): Promise<WeeklyDownload[]> {
  return query<WeeklyDownload>(`
    WITH last_4_weeks AS (
      SELECT DISTINCT DATE_TRUNC('week', download_date) AS week_start_date
      FROM duckdb_stats.main.pypi_daily_stats
      WHERE DATE_TRUNC('week', download_date) >= DATE_TRUNC('week', CURRENT_DATE - INTERVAL '4 weeks')
      ORDER BY week_start_date DESC
    )
    SELECT
      DATE_TRUNC('week', download_date)::VARCHAR AS week_start_date,
      SUM(daily_download_sum)::INT AS weekly_downloads
    FROM duckdb_stats.main.pypi_daily_stats
    WHERE DATE_TRUNC('week', download_date) != (SELECT week_start_date FROM last_4_weeks LIMIT 1)
    GROUP BY DATE_TRUNC('week', download_date)
    ORDER BY week_start_date ASC
  `);
}

export async function getLastTwoWeeks(): Promise<WeeklyDownload[]> {
  return query<WeeklyDownload>(`
    WITH last_4_weeks AS (
      SELECT DISTINCT DATE_TRUNC('week', download_date) AS week_start_date
      FROM duckdb_stats.main.pypi_daily_stats
      WHERE DATE_TRUNC('week', download_date) >= DATE_TRUNC('week', CURRENT_DATE - INTERVAL '4 weeks')
      ORDER BY week_start_date DESC
    ),
    weekly AS (
      SELECT
        DATE_TRUNC('week', download_date)::VARCHAR AS week_start_date,
        SUM(daily_download_sum)::INT AS weekly_downloads
      FROM duckdb_stats.main.pypi_daily_stats
      WHERE DATE_TRUNC('week', download_date) != (SELECT week_start_date FROM last_4_weeks LIMIT 1)
      GROUP BY DATE_TRUNC('week', download_date)
      ORDER BY week_start_date DESC
    )
    SELECT * FROM weekly LIMIT 2
  `);
}

export async function getMonthlyDownloads(): Promise<MonthlyDownload[]> {
  return query<MonthlyDownload>(`
    SELECT
      CONCAT(
        date_part('year', date_trunc('month', download_date)),
        '-',
        LPAD(CAST(date_part('month', date_trunc('month', download_date)) AS VARCHAR), 2, '0')
      ) AS year_month,
      SUM(daily_download_sum)::INT AS monthly_downloads
    FROM duckdb_stats.main.pypi_daily_stats
    GROUP BY date_trunc('month', download_date)
    ORDER BY date_trunc('month', download_date) DESC
    LIMIT 6
  `);
}

export async function getLastTwoMonths(): Promise<MonthlyDownload[]> {
  return query<MonthlyDownload>(`
    SELECT
      CONCAT(
        date_part('year', date_trunc('month', download_date)),
        '-',
        LPAD(CAST(date_part('month', date_trunc('month', download_date)) AS VARCHAR), 2, '0')
      ) AS year_month,
      SUM(daily_download_sum)::INT AS monthly_downloads
    FROM duckdb_stats.main.pypi_daily_stats
    GROUP BY date_trunc('month', download_date)
    ORDER BY date_trunc('month', download_date) DESC
    LIMIT 2 OFFSET 1
  `);
}

export async function getTotalDownloads(): Promise<number> {
  const rows = await query<TotalDownloads>(`
    SELECT SUM(daily_download_sum)::BIGINT AS total_downloads
    FROM duckdb_stats.main.pypi_daily_stats
  `);
  return Number(rows[0]?.total_downloads ?? 0);
}

export async function getRefreshDate(): Promise<string> {
  const rows = await query<RefreshDate>(`
    SELECT MAX(download_date)::VARCHAR AS max_date
    FROM duckdb_stats.main.pypi_daily_stats
  `);
  return rows[0]?.max_date ?? "";
}

function dateFilter(days: number): string {
  return days > 0 ? `WHERE download_date >= CURRENT_DATE - INTERVAL '${days} days'` : "";
}

export async function getDuckDBVersions(days: number): Promise<VersionDownload[]> {
  return query<VersionDownload>(`
    SELECT
      version AS duckdb_version,
      SUM(daily_download_sum)::INT AS total_downloads
    FROM duckdb_stats.main.pypi_daily_stats
    ${dateFilter(days)}
    GROUP BY version
    ORDER BY total_downloads DESC
    LIMIT 10
  `);
}

export async function getPythonVersions(days: number): Promise<PythonVersionDownload[]> {
  return query<PythonVersionDownload>(`
    SELECT
      python_version,
      SUM(daily_download_sum)::INT AS total_downloads
    FROM duckdb_stats.main.pypi_daily_stats
    ${dateFilter(days)}
    GROUP BY python_version
    ORDER BY total_downloads DESC
    LIMIT 10
  `);
}

export async function getTopCountries(days: number): Promise<CountryDownload[]> {
  return query<CountryDownload>(`
    SELECT
      country_code,
      SUM(daily_download_sum)::INT AS total_downloads
    FROM duckdb_stats.main.pypi_daily_stats
    ${dateFilter(days)}
    GROUP BY country_code
    ORDER BY total_downloads DESC
    LIMIT 10
  `);
}

export async function getVersionAdoption(): Promise<VersionAdoption[]> {
  return query<VersionAdoption>(`
    SELECT
      download_date::VARCHAR AS download_date,
      version,
      SUM(daily_download_sum)::INT AS downloads
    FROM duckdb_stats.main.pypi_daily_stats
    WHERE version NOT LIKE '%dev%'
      AND (version LIKE '1.2%' OR version LIKE '1.3%' OR version LIKE '1.4%')
    GROUP BY download_date, version
    ORDER BY download_date ASC
  `);
}

export async function getDailyDownloads(days: number): Promise<DailyDownload[]> {
  return query<DailyDownload>(`
    SELECT
      download_date::VARCHAR AS download_date,
      SUM(daily_download_sum)::INT AS daily_downloads
    FROM duckdb_stats.main.pypi_daily_stats
    ${dateFilter(days)}
    GROUP BY download_date
    ORDER BY download_date ASC
  `);
}

export async function getAllDashboardData(days: number = 0): Promise<DashboardData> {
  const [
    weeklyTimeSeries,
    lastTwoWeeks,
    monthlyRecent,
    lastTwoMonths,
    totalDownloads,
    refreshDate,
    duckdbVersions,
    pythonVersions,
    topCountries,
    versionAdoption,
    dailyDownloads,
  ] = await Promise.all([
    getWeeklyTimeSeries(),
    getLastTwoWeeks(),
    getMonthlyDownloads(),
    getLastTwoMonths(),
    getTotalDownloads(),
    getRefreshDate(),
    getDuckDBVersions(days),
    getPythonVersions(days),
    getTopCountries(days),
    getVersionAdoption(),
    getDailyDownloads(days),
  ]);

  return {
    lastWeekDownloads: lastTwoWeeks[0] ?? null,
    previousWeekDownloads: lastTwoWeeks[1] ?? null,
    lastMonthDownloads: lastTwoMonths[0] ?? null,
    previousMonthDownloads: lastTwoMonths[1] ?? null,
    totalDownloads,
    refreshDate,
    weeklyTimeSeries,
    monthlyRecent,
    duckdbVersions,
    pythonVersions,
    topCountries,
    versionAdoption,
    dailyDownloads,
    periodDays: days,
  };
}
