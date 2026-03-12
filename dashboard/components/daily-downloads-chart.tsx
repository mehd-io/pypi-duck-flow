"use client";

import {
  Bar,
  BarChart,
  CartesianGrid,
  XAxis,
  YAxis,
} from "recharts";
import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
  type ChartConfig,
} from "@/components/ui/chart";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { formatNumber, formatShortDate, formatPeriod } from "@/lib/format";
import type { DailyDownload } from "@/lib/types";

const chartConfig = {
  daily_downloads: {
    label: "Downloads",
    color: "var(--chart-2)",
  },
} satisfies ChartConfig;

interface DailyDownloadsChartProps {
  data: DailyDownload[];
  periodDays: number;
}

export function DailyDownloadsChart({ data, periodDays }: DailyDownloadsChartProps) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Daily Downloads</CardTitle>
        <CardDescription>
          Day-by-day download volume ({formatPeriod(periodDays)})
        </CardDescription>
      </CardHeader>
      <CardContent>
        <ChartContainer config={chartConfig} className="aspect-auto h-[220px] w-full">
          <BarChart data={data} accessibilityLayer>
            <CartesianGrid vertical={false} strokeDasharray="3 3" />
            <XAxis
              dataKey="download_date"
              tickLine={false}
              axisLine={false}
              tickMargin={8}
              tickFormatter={(v) => formatShortDate(v)}
              interval="preserveStartEnd"
              minTickGap={40}
            />
            <YAxis
              tickLine={false}
              axisLine={false}
              tickMargin={8}
              tickFormatter={(v) => formatNumber(v)}
              width={55}
            />
            <ChartTooltip
              content={
                <ChartTooltipContent
                  labelFormatter={(v) => formatShortDate(v as string)}
                  formatter={(value) => [formatNumber(value as number), "Downloads"]}
                />
              }
            />
            <Bar
              dataKey="daily_downloads"
              fill="var(--color-daily_downloads)"
              radius={[2, 2, 0, 0]}
            />
          </BarChart>
        </ChartContainer>
      </CardContent>
    </Card>
  );
}
