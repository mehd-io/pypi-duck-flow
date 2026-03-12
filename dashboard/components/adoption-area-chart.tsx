"use client";

import { useMemo, useState } from "react";
import {
  Area,
  AreaChart,
  CartesianGrid,
  XAxis,
  YAxis,
} from "recharts";
import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
  ChartLegend,
  ChartLegendContent,
  type ChartConfig,
} from "@/components/ui/chart";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  CardDescription,
} from "@/components/ui/card";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { formatNumber, formatShortDate } from "@/lib/format";
import type { VersionAdoption } from "@/lib/types";

const VERSION_COLORS = [
  "var(--chart-1)",
  "var(--chart-2)",
  "var(--chart-3)",
  "var(--chart-4)",
  "var(--chart-5)",
];

interface AdoptionAreaChartProps {
  data: VersionAdoption[];
}

export function AdoptionAreaChart({ data }: AdoptionAreaChartProps) {
  const [mode, setMode] = useState<"absolute" | "percent">("absolute");

  const versions = useMemo(() => {
    const set = new Set<string>();
    data.forEach((d) => set.add(d.version));
    return Array.from(set).sort();
  }, [data]);

  const chartConfig = useMemo(() => {
    const config: ChartConfig = {};
    versions.forEach((v, i) => {
      config[v] = {
        label: v,
        color: VERSION_COLORS[i % VERSION_COLORS.length],
      };
    });
    return config;
  }, [versions]);

  const pivotedData = useMemo(() => {
    const byDate = new Map<string, Record<string, number>>();
    data.forEach((d) => {
      if (!byDate.has(d.download_date)) {
        byDate.set(d.download_date, { download_date: 0 } as unknown as Record<string, number>);
      }
      const entry = byDate.get(d.download_date)!;
      (entry as Record<string, unknown>)["download_date"] = d.download_date;
      entry[d.version] = (entry[d.version] || 0) + d.downloads;
    });

    const result = Array.from(byDate.values());

    if (mode === "percent") {
      return result.map((row) => {
        const total = versions.reduce((sum, v) => sum + (row[v] || 0), 0);
        const normalized: Record<string, unknown> = { download_date: (row as unknown as Record<string, unknown>).download_date };
        versions.forEach((v) => {
          normalized[v] = total > 0 ? ((row[v] || 0) / total) * 100 : 0;
        });
        return normalized as Record<string, number>;
      });
    }

    return result;
  }, [data, versions, mode]);

  return (
    <Card>
      <CardHeader>
        <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <CardTitle>Version Adoption</CardTitle>
            <CardDescription>
              Download trends for recent DuckDB versions (non-dev releases)
            </CardDescription>
          </div>
          <Tabs value={mode} onValueChange={(v) => setMode(v as "absolute" | "percent")}>
            <TabsList>
              <TabsTrigger value="absolute">Absolute</TabsTrigger>
              <TabsTrigger value="percent">% Share</TabsTrigger>
            </TabsList>
          </Tabs>
        </div>
      </CardHeader>
      <CardContent>
        <ChartContainer config={chartConfig} className="aspect-auto h-[280px] w-full">
          <AreaChart
            data={pivotedData}
            accessibilityLayer
            stackOffset={mode === "percent" ? "expand" : undefined}
          >
            <defs>
              {versions.map((v, i) => (
                <linearGradient key={v} id={`fill-${v.replace(/\./g, "-")}`} x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor={VERSION_COLORS[i % VERSION_COLORS.length]} stopOpacity={0.4} />
                  <stop offset="95%" stopColor={VERSION_COLORS[i % VERSION_COLORS.length]} stopOpacity={0.05} />
                </linearGradient>
              ))}
            </defs>
            <CartesianGrid vertical={false} strokeDasharray="3 3" />
            <XAxis
              dataKey="download_date"
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
              tickFormatter={(v) =>
                mode === "percent" ? `${(v * 100).toFixed(0)}%` : formatNumber(v)
              }
              width={60}
            />
            <ChartTooltip
              content={
                <ChartTooltipContent
                  labelFormatter={(v) => formatShortDate(v as string)}
                  formatter={(value) => [
                    mode === "percent"
                      ? `${(value as number).toFixed(1)}%`
                      : formatNumber(value as number),
                    "",
                  ]}
                />
              }
            />
            <ChartLegend content={<ChartLegendContent />} />
            {versions.map((v, i) => (
              <Area
                key={v}
                type="monotone"
                dataKey={v}
                stackId="1"
                stroke={VERSION_COLORS[i % VERSION_COLORS.length]}
                fill={`url(#fill-${v.replace(/\./g, "-")})`}
                strokeWidth={1.5}
              />
            ))}
          </AreaChart>
        </ChartContainer>
      </CardContent>
    </Card>
  );
}
