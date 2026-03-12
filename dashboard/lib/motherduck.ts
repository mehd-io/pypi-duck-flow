import { DuckDBInstance, DuckDBConnection } from "@duckdb/node-api";

let connectionPromise: Promise<DuckDBConnection> | null = null;

async function createConnection(): Promise<DuckDBConnection> {
  const token = process.env.MOTHERDUCK_TOKEN;
  if (!token) {
    throw new Error("MOTHERDUCK_TOKEN environment variable is not set");
  }

  const instance = await DuckDBInstance.create("md:", {
    motherduck_token: token,
  });
  const connection = await instance.connect();
  await connection.run("SET home_directory = '/tmp'");
  await connection.run(
    "ATTACH IF NOT EXISTS 'md:duckdb_stats' AS duckdb_stats"
  );
  return connection;
}

export async function getConnection(): Promise<DuckDBConnection> {
  if (!connectionPromise) {
    connectionPromise = createConnection().catch((err) => {
      connectionPromise = null;
      throw err;
    });
  }
  return connectionPromise;
}

export async function query<T>(sql: string): Promise<T[]> {
  const conn = await getConnection();
  const reader = await conn.runAndReadAll(sql);
  return reader.getRowObjectsJson() as T[];
}
