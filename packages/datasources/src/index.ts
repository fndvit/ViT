import fs from "fs";
import path from "path";
import { parse } from "csv-parse/sync";
import { Pool } from "pg";

export type Row = Record<string, any>;

let globalPool: Pool | null = null;

function getPool(connectionString?: string) {
  if (!connectionString) {
    if (!globalPool) {
      // no connection string provided; returning null indicates fallback
      return null;
    }
    return globalPool;
  }
  if (!globalPool) {
    globalPool = new Pool({ connectionString });
    return globalPool;
  }
  // If a different connectionString is needed in the future, create new pools per connection.
  return globalPool;
}

export async function fetchFromPostgres(
  connectionString: string | undefined,
  query: string
) {
  if (!connectionString) {
    // signal fallback by returning empty array to be handled by caller
    return [];
  }
  const pool = getPool(connectionString);
  if (!pool) return [];
  const res = await pool.query(query);
  return res.rows;
}

export async function fetchFromCSV(p: string) {
  const file = fs.readFileSync(path.resolve(process.cwd(), p), "utf8");
  const records = parse(file, { columns: true, skip_empty_lines: true });
  return records;
}

export async function fetchFromJSON(p: string) {
  const file = fs.readFileSync(path.resolve(process.cwd(), p), "utf8");
  const data = JSON.parse(file);
  if (Array.isArray(data)) return data;
  if (data && typeof data === "object" && Array.isArray((data as any).rows))
    return (data as any).rows;
  return [];
}
