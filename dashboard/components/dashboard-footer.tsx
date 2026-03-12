import { Separator } from "@/components/ui/separator";

export function DashboardFooter() {
  return (
    <footer className="mt-12 pb-8">
      <Separator className="mb-8" />
      <div className="mx-auto max-w-7xl px-4 sm:px-6">
        <div className="flex flex-col gap-6 sm:flex-row sm:items-start sm:justify-between">
          <div className="max-w-md">
            <h3 className="text-sm font-semibold">Build Your Own Insights</h3>
            <p className="mt-1 text-sm text-muted-foreground">
              This dashboard is powered by{" "}
              <a href="https://nextjs.org" className="underline hover:text-foreground" target="_blank" rel="noopener noreferrer">Next.js</a>,{" "}
              <a href="https://duckdb.org" className="underline hover:text-foreground" target="_blank" rel="noopener noreferrer">DuckDB</a>, and{" "}
              <a href="https://motherduck.com" className="underline hover:text-foreground" target="_blank" rel="noopener noreferrer">MotherDuck</a>.
              Find the code on{" "}
              <a href="https://github.com/mehd-io/pypi-duck-flow" className="underline hover:text-foreground" target="_blank" rel="noopener noreferrer">GitHub</a>.
            </p>
          </div>
          <div>
            <h3 className="text-sm font-semibold">Access Raw Data</h3>
            <p className="mt-1 text-sm text-muted-foreground">
              Query directly from any DuckDB client:
            </p>
            <code className="mt-2 block rounded bg-muted px-3 py-2 text-xs">
              ATTACH &apos;md:_share/duckdb_stats/1eb684bf-faff-4860-8e7d-92af4ff9a410&apos;;
            </code>
          </div>
        </div>
        <p className="mt-6 text-center text-xs text-muted-foreground">
          Made with ❤️ by{" "}
          <a
            href="https://www.linkedin.com/in/mehd-io/"
            className="underline hover:text-foreground"
            target="_blank"
            rel="noopener noreferrer"
          >
            mehdio
          </a>
        </p>
      </div>
    </footer>
  );
}
