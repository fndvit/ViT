import { z } from "zod";

/**
 * Core types and normalization / transform registry
 */

export type Row = Record<string, string | number | boolean | null>;
export interface Dataset {
  name: string;
  rows: Row[];
}

export const DatasetSchema = z.object({
  name: z.string(),
  rows: z.array(
    z.record(z.union([z.string(), z.number(), z.boolean(), z.null()]))
  ),
});

export type DashboardConfig = {
  id: string;
  title: string;
  charts?: Array<{ type: string; dataSource: string; x?: string; y?: string }>;
  dataSources: Record<
    string,
    {
      type: string;
      connectionString?: string;
      query?: string;
      path?: string;
    }
  >;
};

export function normalizeDataset(raw: any[], name = "dataset"): Dataset {
  // Very simple normalization: lowercase keys, null -> null, keep types
  const rows = (raw || []).map((r: any) => {
    const out: Row = {};
    Object.entries(r || {}).forEach(([k, v]) => {
      const key = String(k).toLowerCase();
      out[key] =
        v === undefined || (typeof v === "object" && v !== null)
          ? null
          : (v as string | number | boolean | null);
    });
    return out;
  });
  const ds: Dataset = { name, rows };
  DatasetSchema.parse(ds); // runtime validation (throws if invalid)
  return ds;
}

/**
 * Transform registry for per-dashboard transforms
 */
type TransformFn = (ds: Dataset) => Dataset;
const transforms: Map<string, TransformFn> = new Map();

export function registerTransform(dashboardId: string, fn: TransformFn) {
  transforms.set(dashboardId, fn);
}

export function applyTransform(dashboardId: string, ds: Dataset): Dataset {
  const t = transforms.get(dashboardId);
  if (!t) return ds;
  return t(ds);
}
