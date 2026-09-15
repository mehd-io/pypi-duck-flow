"use client";

import { useRef, useState } from "react";
import { ExternalLink, Info } from "lucide-react";
import type { MetricMode } from "@/lib/types";

interface MetricSelectorProps {
  value: MetricMode;
  loading: boolean;
  onChange: (metric: MetricMode) => void;
}

export function MetricSelector({ value, loading, onChange }: MetricSelectorProps) {
  const [infoOpen, setInfoOpen] = useState(false);
  const closeTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const comparable = value === "artifact";

  const showInfo = () => {
    if (closeTimer.current) clearTimeout(closeTimer.current);
    setInfoOpen(true);
  };

  const hideInfo = () => {
    closeTimer.current = setTimeout(() => setInfoOpen(false), 100);
  };

  return (
    <div className="flex justify-end">
      <div className="flex items-center gap-2">
        <span
          className="text-sm font-medium"
          onMouseEnter={showInfo}
          onMouseLeave={hideInfo}
        >
          Comparable download history
        </span>
        <button
          type="button"
          role="switch"
          aria-checked={comparable}
          aria-label="Comparable download history"
          disabled={loading}
          onClick={() => onChange(comparable ? "legacy" : "artifact")}
          className={`relative inline-flex h-5 w-9 shrink-0 items-center rounded-full transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50 ${
            comparable ? "bg-primary" : "bg-input"
          }`}
        >
          <span
            aria-hidden="true"
            className={`block size-4 rounded-full bg-background shadow-sm transition-transform ${
              comparable ? "translate-x-4.5" : "translate-x-0.5"
            }`}
          />
        </button>
        <div className="relative">
          <span
            role="button"
            tabIndex={0}
            aria-label="About the download metrics"
            aria-expanded={infoOpen}
            aria-controls="download-metric-info"
            onMouseEnter={showInfo}
            onMouseLeave={hideInfo}
            onFocus={showInfo}
            onBlur={hideInfo}
            className="inline-flex size-8 items-center justify-center rounded-lg text-muted-foreground transition-colors hover:bg-muted hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
          >
            <Info className="size-4" />
          </span>
          {infoOpen && (
            <div
              id="download-metric-info"
              role="dialog"
              aria-label="Download metric explanation"
              onMouseEnter={showInfo}
              onMouseLeave={hideInfo}
              onFocus={showInfo}
              onBlur={hideInfo}
              className="absolute right-0 top-10 z-20 w-[min(22rem,calc(100vw-2rem))] rounded-xl border bg-popover p-4 text-sm text-popover-foreground shadow-lg"
            >
              <p className="font-medium">Why are there two metrics?</p>
              <p className="mt-2 text-muted-foreground">
                On August 24, 2026, PyPI stopped including metadata sidecars and
                other package objects in its download logs. When enabled, this
                applies PyPI&apos;s current <code>.whl</code>, <code>.tar.gz</code>, and{" "}
                <code>.zip</code> definition to every historical date, keeping the
                timeline comparable. Turn it off to see the broader legacy request
                count and its permanent methodology break.
              </p>
              <a
                href="https://blog.pypi.org/posts/2026-08-31-download-counts/"
                target="_blank"
                rel="noopener noreferrer"
                className="mt-3 inline-flex items-center gap-1 font-medium underline underline-offset-4"
              >
                Read the PyPI blog post
                <ExternalLink className="size-3.5" />
              </a>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
