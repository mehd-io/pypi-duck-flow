"use client";

import {
  Area,
  AreaChart,
  CartesianGrid,
  XAxis,
  YAxis,
  Brush,
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
  return (
    <Card>
      <CardHeader>
        <CardTitle>Weekly Downloads</CardTitle>
        <CardDescription>
          DuckDB Python package downloads aggregated by week
        </CardDescription>
      </CardHeader>
      <CardContent>
        <ChartContainer config={chartConfig} className="aspect-auto h-[250px] w-full">
          <AreaChart data={data} accessibilityLayer>
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
            <Brush
              dataKey="week_start_date"
              height={30}
              stroke="var(--color-weekly_downloads)"
              tickFormatter={(v) => formatShortDate(v)}
            />
          </AreaChart>
        </ChartContainer>
      </CardContent>
    </Card>
  );
}
