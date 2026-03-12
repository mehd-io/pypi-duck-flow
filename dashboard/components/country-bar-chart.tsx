"use client";

import { Bar, BarChart, CartesianGrid, XAxis, YAxis, LabelList } from "recharts";
import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
  type ChartConfig,
} from "@/components/ui/chart";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { formatNumber, formatPeriod } from "@/lib/format";
import type { CountryDownload } from "@/lib/types";

const chartConfig = {
  total_downloads: {
    label: "Downloads",
    color: "var(--chart-3)",
  },
} satisfies ChartConfig;

interface CountryBarChartProps {
  data: CountryDownload[];
  periodDays: number;
}

export function CountryBarChart({ data, periodDays }: CountryBarChartProps) {
  const sorted = [...data].sort((a, b) => b.total_downloads - a.total_downloads);

  return (
    <Card>
      <CardHeader>
        <CardTitle>Top 10 Countries</CardTitle>
        <CardDescription>Countries with most downloads ({formatPeriod(periodDays)})</CardDescription>
      </CardHeader>
      <CardContent>
        <ChartContainer config={chartConfig} className="aspect-auto h-[280px] w-full">
          <BarChart data={sorted} layout="vertical" accessibilityLayer>
            <CartesianGrid horizontal={false} strokeDasharray="3 3" />
            <XAxis
              type="number"
              tickLine={false}
              axisLine={false}
              tickFormatter={(v) => formatNumber(v)}
            />
            <YAxis
              type="category"
              dataKey="country_code"
              tickLine={false}
              axisLine={false}
              tickMargin={8}
              width={40}
            />
            <ChartTooltip
              content={
                <ChartTooltipContent
                  formatter={(value) => [formatNumber(value as number), "Downloads"]}
                />
              }
            />
            <Bar
              dataKey="total_downloads"
              fill="var(--color-total_downloads)"
              radius={[0, 4, 4, 0]}
            >
              <LabelList
                dataKey="total_downloads"
                position="right"
                formatter={(v: number) => formatNumber(v)}
                className="fill-muted-foreground text-xs"
              />
            </Bar>
          </BarChart>
        </ChartContainer>
      </CardContent>
    </Card>
  );
}
