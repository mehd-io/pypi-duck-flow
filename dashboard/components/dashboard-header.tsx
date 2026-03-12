"use client";

import { useState } from "react";
import Image from "next/image";
import { ThemeToggle } from "./theme-toggle";
import { Github, Check, Copy } from "lucide-react";

const ATTACH_COMMAND = "ATTACH 'md:_share/duckdb_stats/1eb684bf-faff-4860-8e7d-92af4ff9a410' AS duckdb_stats;";

export function DashboardHeader() {
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    await navigator.clipboard.writeText(ATTACH_COMMAND);
    setCopied(true);
    setTimeout(() => setCopied(false), 2500);
  };

  return (
    <header className="sticky top-0 z-50 border-b bg-background/80 backdrop-blur-sm">
      <div className="mx-auto flex h-14 max-w-7xl items-center justify-between px-4 sm:px-6">
        <div className="flex items-center gap-3">
          <Image src="/duckdb-mark.svg" alt="DuckDB" width={32} height={32} />
          <h1 className="text-lg tracking-tight">
            <span className="font-bold">DuckDB</span>
            <span className="font-normal text-muted-foreground"> Stats</span>
          </h1>
          <span className="hidden text-border sm:inline">|</span>
          <div className="hidden items-center gap-1.5 sm:flex">
            <Image src="/python-logo.svg" alt="Python" width={14} height={14} />
            <p className="text-xs font-medium uppercase tracking-widest text-muted-foreground">
              PyPI Downloads
            </p>
          </div>
        </div>
        <div className="flex items-center gap-1">
          <div className="relative">
            <button
              onClick={handleCopy}
              className="inline-flex h-8 items-center gap-1.5 rounded-lg px-2.5 text-xs font-medium text-muted-foreground hover:bg-muted hover:text-foreground transition-colors"
              title="Copy MotherDuck ATTACH command"
            >
              <Image src="/motherduck-logo.svg" alt="MotherDuck" width={16} height={16} />
              <span className="hidden sm:inline">
                {copied ? "Copied!" : "MotherDuck share link"}
              </span>
              {copied ? (
                <Check className="h-3.5 w-3.5 text-emerald-500" />
              ) : (
                <Copy className="h-3.5 w-3.5" />
              )}
            </button>
            {copied && (
              <div className="absolute right-0 top-full mt-2 w-64 rounded-lg border bg-popover p-3 text-xs text-popover-foreground shadow-lg">
                <p className="font-medium">Share command copied to clipboard</p>
                <p className="mt-1 text-muted-foreground">
                  Paste in any DuckDB client to access the raw data.
                </p>
                <code className="mt-2 block rounded bg-muted px-2 py-1 text-[10px] break-all">
                  {ATTACH_COMMAND}
                </code>
              </div>
            )}
          </div>
          <a
            href="https://github.com/mehd-io/pypi-duck-flow"
            target="_blank"
            rel="noopener noreferrer"
            aria-label="GitHub repository"
            className="inline-flex size-8 items-center justify-center rounded-lg hover:bg-muted"
          >
            <Github className="h-5 w-5" />
          </a>
          <ThemeToggle />
        </div>
      </div>
    </header>
  );
}
