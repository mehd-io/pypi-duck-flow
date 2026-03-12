"use client";

import { Card, CardContent } from "@/components/ui/card";
import { formatCompact, formatPercent, formatDate } from "@/lib/format";
import { TrendingUp, TrendingDown } from "lucide-react";
import type { WeeklyDownload, MonthlyDownload } from "@/lib/types";
import {
  Area,
  AreaChart,
  ResponsiveContainer,
} from "recharts";

interface SparklineProps {
  data: { value: number }[];
  color?: string;
}

function Sparkline({ data, color = "currentColor" }: SparklineProps) {
  if (data.length < 2) return null;
  return (
    <div className="mt-2 h-8 w-full opacity-60">
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart data={data}>
          <defs>
            <linearGradient id={`spark-${color.replace(/[^a-z0-9]/gi, "")}`} x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor={color} stopOpacity={0.15} />
              <stop offset="100%" stopColor={color} stopOpacity={0} />
            </linearGradient>
          </defs>
          <Area
            type="monotone"
            dataKey="value"
            stroke={color}
            fill={`url(#spark-${color.replace(/[^a-z0-9]/gi, "")})`}
            strokeWidth={1.5}
            dot={false}
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}

interface KpiCardProps {
  title: string;
  value: string;
  change?: number;
  sparkData?: { value: number }[];
  sparkColor?: string;
}

function KpiCard({ title, value, change, sparkData, sparkColor }: KpiCardProps) {
  return (
    <Card>
      <CardContent className="pt-3 pb-2">
        <p className="text-[11px] font-medium tracking-wide text-muted-foreground">{title}</p>
        <div className="mt-0.5 flex items-baseline gap-2">
          <p className="text-[28px] font-medium leading-tight tracking-tight">{value}</p>
          {change !== undefined && (
            <span
              className={`inline-flex items-center gap-0.5 text-[11px] font-medium ${
                change >= 0 ? "text-emerald-500" : "text-red-500"
              }`}
            >
              {change >= 0 ? (
                <TrendingUp className="h-3 w-3" />
              ) : (
                <TrendingDown className="h-3 w-3" />
              )}
              {formatPercent(change)}
            </span>
          )}
        </div>
        {sparkData && <Sparkline data={sparkData} color={sparkColor} />}
      </CardContent>
    </Card>
  );
}

interface KpiCardsProps {
  lastWeek: WeeklyDownload | null;
  previousWeek: WeeklyDownload | null;
  lastMonth: MonthlyDownload | null;
  previousMonth: MonthlyDownload | null;
  totalDownloads: number;
  refreshDate: string;
  weeklyTimeSeries?: WeeklyDownload[];
}

export function KpiCards({
  lastWeek,
  previousWeek,
  lastMonth,
  previousMonth,
  totalDownloads,
  refreshDate,
  weeklyTimeSeries,
}: KpiCardsProps) {
  const weekChange =
    lastWeek && previousWeek
      ? ((lastWeek.weekly_downloads - previousWeek.weekly_downloads) /
          previousWeek.weekly_downloads) *
        100
      : undefined;

  const monthChange =
    lastMonth && previousMonth
      ? ((lastMonth.monthly_downloads - previousMonth.monthly_downloads) /
          previousMonth.monthly_downloads) *
        100
      : undefined;

  const recentSpark = weeklyTimeSeries
    ? weeklyTimeSeries.slice(-12).map((w) => ({ value: w.weekly_downloads }))
    : undefined;

  return (
    <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
      <KpiCard
          title="Last Week"
          value={lastWeek ? formatCompact(lastWeek.weekly_downloads) : "—"}
          change={weekChange}
          sparkData={recentSpark}
          sparkColor="var(--chart-1)"
        />
        <KpiCard
          title="Last Month"
          value={lastMonth ? formatCompact(lastMonth.monthly_downloads) : "—"}
          change={monthChange}
          sparkData={recentSpark}
          sparkColor="var(--chart-2)"
        />
        <KpiCard
          title="All-Time Downloads"
          value={formatCompact(totalDownloads)}
          sparkData={recentSpark}
          sparkColor="var(--chart-3)"
        />
        <KpiCard
          title="Data Freshness"
          value={refreshDate ? formatDate(refreshDate) : "—"}
        />
    </div>
  );
}
