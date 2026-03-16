"use client";

import { useEffect, useState, useCallback } from "react";
import type { DashboardData } from "@/lib/types";
import { KpiCards } from "./kpi-cards";
import { DownloadsLineChart } from "./downloads-line-chart";
import { MonthlyTable } from "./monthly-table";
import { VersionBarChart } from "./version-bar-chart";
import { PythonBarChart } from "./python-bar-chart";
import { CountryBarChart } from "./country-bar-chart";
import { AdoptionAreaChart } from "./adoption-area-chart";
import { DailyDownloadsChart } from "./daily-downloads-chart";

function Skeleton({ className }: { className?: string }) {
  return (
    <div
      className={`animate-pulse rounded-xl bg-muted ${className ?? ""}`}
    />
  );
}

function LoadingSkeleton() {
  return (
    <div className="space-y-8">
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
        {Array.from({ length: 4 }).map((_, i) => (
          <Skeleton key={i} className="h-[120px]" />
        ))}
      </div>
      <Skeleton className="h-[400px]" />
      <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
        <Skeleton className="h-[400px]" />
        <Skeleton className="h-[400px]" />
      </div>
      <Skeleton className="h-[400px]" />
      <Skeleton className="h-[400px]" />
    </div>
  );
}

function ErrorState({ message, onRetry }: { message: string; onRetry: () => void }) {
  return (
    <div className="flex flex-col items-center justify-center gap-4 py-20">
      <div className="text-4xl">🦆</div>
      <h2 className="text-xl font-semibold">Something went wrong</h2>
      <p className="text-sm text-muted-foreground">{message}</p>
      <button
        onClick={onRetry}
        className="rounded-lg bg-primary px-4 py-2 text-sm font-medium text-primary-foreground hover:bg-primary/90"
      >
        Try again
      </button>
    </div>
  );
}

export function Dashboard() {
  const [data, setData] = useState<DashboardData | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  const fetchData = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const res = await fetch(`/api/stats?days=0`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json();
      setData(json);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load data");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  if (loading && !data) return <LoadingSkeleton />;
  if (error && !data) return <ErrorState message={error} onRetry={fetchData} />;
  if (!data) return null;

  return (
    <div className="space-y-6">
      <KpiCards
        lastWeek={data.lastWeekDownloads}
        previousWeek={data.previousWeekDownloads}
        lastMonth={data.lastMonthDownloads}
        previousMonth={data.previousMonthDownloads}
        totalDownloads={data.totalDownloads}
        refreshDate={data.refreshDate}
        weeklyTimeSeries={data.weeklyTimeSeries}
      />

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
        <div className="lg:col-span-2">
          <DownloadsLineChart data={data.weeklyTimeSeries} />
        </div>
        <MonthlyTable data={data.monthlyRecent} />
      </div>

      <h2 className="text-base font-semibold tracking-tight">Breakdown</h2>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <DailyDownloadsChart data={data.dailyDownloads} periodDays={data.periodDays} />
        <CountryBarChart data={data.topCountries} periodDays={data.periodDays} />
      </div>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <VersionBarChart data={data.duckdbVersions} periodDays={data.periodDays} />
        <PythonBarChart data={data.pythonVersions} periodDays={data.periodDays} />
      </div>

      <AdoptionAreaChart data={data.versionAdoption} />
    </div>
  );
}
