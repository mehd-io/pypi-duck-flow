import { NextResponse, type NextRequest } from "next/server";
import { getAllDashboardData } from "@/lib/queries";
import type { MetricMode } from "@/lib/types";

export const dynamic = "force-dynamic";
export const revalidate = 0;

const VALID_PERIODS = [0, 7, 30, 90];
const VALID_METRICS: MetricMode[] = ["artifact", "legacy"];

export async function GET(request: NextRequest) {
  try {
    const daysParam = request.nextUrl.searchParams.get("days");
    const days = daysParam ? parseInt(daysParam, 10) : 0;
    const safeDays = VALID_PERIODS.includes(days) ? days : 0;
    const metricParam = request.nextUrl.searchParams.get("metric") as MetricMode | null;
    const metric = metricParam && VALID_METRICS.includes(metricParam) ? metricParam : "artifact";

    const data = await getAllDashboardData(safeDays, metric);
    return NextResponse.json(data, {
      headers: { "Cache-Control": "public, s-maxage=3600, stale-while-revalidate=7200" },
    });
  } catch (error) {
    console.error("Failed to fetch dashboard data:", error);
    return NextResponse.json(
      { error: "Failed to fetch dashboard data" },
      { status: 500 }
    );
  }
}
