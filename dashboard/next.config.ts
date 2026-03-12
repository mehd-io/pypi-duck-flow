import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  serverExternalPackages: ["@duckdb/node-api", "@duckdb/node-bindings"],
  outputFileTracingIncludes: {
    "/api/*": ["./node_modules/@duckdb/**/*"],
  },
};

export default nextConfig;
