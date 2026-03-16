"use client";

import { useState, useCallback } from "react";
import {
  Area,
  AreaChart,
  CartesianGrid,
  XAxis,
  YAxis,
  ReferenceArea,
} from "recharts";
import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
  type ChartConfig,
} from "@/components/ui/chart";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { formatNumber, formatShortDate } from "@/lib/format";
import type { WeeklyDownload } from "@/lib/types";

const chartConfig = {
  weekly_downloads: {
    label: "Weekly Downloads",
    color: "var(--chart-1)",
  },
} satisfies ChartConfig;

interface DownloadsLineChartProps {
  data: WeeklyDownload[];
}

export function DownloadsLineChart({ data }: DownloadsLineChartProps) {
  const [chartData, setChartData] = useState(data);
  const [refAreaLeft, setRefAreaLeft] = useState<string | null>(null);
  const [refAreaRight, setRefAreaRight] = useState<string | null>(null);
  const [isZoomed, setIsZoomed] = useState(false);

  const handleMouseDown = useCallback((e: { activeLabel?: string }) => {
    if (e?.activeLabel) {
      setRefAreaLeft(e.activeLabel);
      setRefAreaRight(null);
    }
  }, []);

  const handleMouseMove = useCallback(
    (e: { activeLabel?: string }) => {
      if (refAreaLeft && e?.activeLabel) {
        setRefAreaRight(e.activeLabel);
      }
    },
    [refAreaLeft],
  );

  const handleMouseUp = useCallback(() => {
    if (!refAreaLeft || !refAreaRight) {
      setRefAreaLeft(null);
      setRefAreaRight(null);
      return;
    }

    const idxLeft = data.findIndex((d) => d.week_start_date === refAreaLeft);
    const idxRight = data.findIndex((d) => d.week_start_date === refAreaRight);

    if (idxLeft === idxRight) {
      setRefAreaLeft(null);
      setRefAreaRight(null);
      return;
    }

    const [from, to] =
      idxLeft < idxRight ? [idxLeft, idxRight] : [idxRight, idxLeft];

    setChartData(data.slice(from, to + 1));
    setIsZoomed(true);
    setRefAreaLeft(null);
    setRefAreaRight(null);
  }, [refAreaLeft, refAreaRight, data]);

  const handleZoomOut = useCallback(() => {
    setChartData(data);
    setIsZoomed(false);
  }, [data]);

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between">
        <div>
          <CardTitle>Weekly Downloads</CardTitle>
          <CardDescription>
            DuckDB Python package downloads aggregated by week
          </CardDescription>
        </div>
        {isZoomed ? (
          <button
            onClick={handleZoomOut}
            className="rounded-md border border-input bg-background px-3 py-1.5 text-xs font-medium text-muted-foreground transition-colors hover:bg-accent hover:text-accent-foreground"
          >
            Reset zoom
          </button>
        ) : (
          <span className="inline-flex items-center gap-1.5 rounded-md bg-muted px-2.5 py-1 text-xs text-muted-foreground">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <circle cx="11" cy="11" r="8" />
              <line x1="21" y1="21" x2="16.65" y2="16.65" />
              <line x1="11" y1="8" x2="11" y2="14" />
              <line x1="8" y1="11" x2="14" y2="11" />
            </svg>
            Drag to zoom
          </span>
        )}
      </CardHeader>
      <CardContent>
        <ChartContainer config={chartConfig} className="aspect-auto h-[250px] w-full cursor-crosshair">
          <AreaChart
            data={chartData}
            accessibilityLayer
            onMouseDown={handleMouseDown}
            onMouseMove={handleMouseMove}
            onMouseUp={handleMouseUp}
          >
            <defs>
              <linearGradient id="fillWeekly" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="var(--color-weekly_downloads)" stopOpacity={0.3} />
                <stop offset="95%" stopColor="var(--color-weekly_downloads)" stopOpacity={0.02} />
              </linearGradient>
            </defs>
            <CartesianGrid vertical={false} strokeDasharray="3 3" />
            <XAxis
              dataKey="week_start_date"
              tickLine={false}
              axisLine={false}
              tickMargin={8}
              tickFormatter={(v) => formatShortDate(v)}
              interval="preserveStartEnd"
              minTickGap={50}
            />
            <YAxis
              tickLine={false}
              axisLine={false}
              tickMargin={8}
              tickFormatter={(v) => formatNumber(v)}
              width={60}
            />
            <ChartTooltip
              content={
                <ChartTooltipContent
                  labelFormatter={(v) => formatShortDate(v as string)}
                  formatter={(value) => [formatNumber(value as number), "Downloads"]}
                />
              }
            />
            <Area
              type="monotone"
              dataKey="weekly_downloads"
              stroke="var(--color-weekly_downloads)"
              fill="url(#fillWeekly)"
              strokeWidth={2}
            />
            {refAreaLeft && refAreaRight && (
              <ReferenceArea
                x1={refAreaLeft}
                x2={refAreaRight}
                fill="var(--color-weekly_downloads)"
                fillOpacity={0.15}
                strokeOpacity={0.3}
                stroke="var(--color-weekly_downloads)"
              />
            )}
          </AreaChart>
        </ChartContainer>
      </CardContent>
    </Card>
  );
}
