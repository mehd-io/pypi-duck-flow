import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  serverExternalPackages: ["@duckdb/node-api", "@duckdb/node-bindings"],
  outputFileTracingIncludes: {
    "/api/*": ["./node_modules/@duckdb/**/*"],
  },
  webpack: (config, { isServer }) => {
    if (isServer) {
      config.externals = config.externals || [];
      config.externals.push({
        "@duckdb/node-api": "commonjs @duckdb/node-api",
        "@duckdb/node-bindings": "commonjs @duckdb/node-bindings",
      });
    }
    return config;
  },
};

export default nextConfig;
