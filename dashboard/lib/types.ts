export interface WeeklyDownload {
  week_start_date: string;
  weekly_downloads: number;
}

export interface MonthlyDownload {
  year_month: string;
  monthly_downloads: number;
}

export interface RefreshDate {
  max_date: string;
}

export interface TotalDownloads {
  total_downloads: number;
}

export interface VersionDownload {
  duckdb_version: string;
  total_downloads: number;
}

export interface PythonVersionDownload {
  python_version: string;
  total_downloads: number;
}

export interface CountryDownload {
  country_code: string;
  total_downloads: number;
}

export interface VersionAdoption {
  download_date: string;
  version: string;
  downloads: number;
}

export interface DailyDownload {
  download_date: string;
  daily_downloads: number;
}

export interface DashboardData {
  lastWeekDownloads: WeeklyDownload | null;
  previousWeekDownloads: WeeklyDownload | null;
  lastMonthDownloads: MonthlyDownload | null;
  previousMonthDownloads: MonthlyDownload | null;
  totalDownloads: number;
  refreshDate: string;
  weeklyTimeSeries: WeeklyDownload[];
  monthlyRecent: MonthlyDownload[];
  duckdbVersions: VersionDownload[];
  pythonVersions: PythonVersionDownload[];
  topCountries: CountryDownload[];
  versionAdoption: VersionAdoption[];
  dailyDownloads: DailyDownload[];
  periodDays: number;
}
