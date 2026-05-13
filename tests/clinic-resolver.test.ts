import { describe, it, expect, vi, beforeEach } from "vitest";
import { resolveClinic } from "../src/background/clinic-resolver";

const storage: Record<string, unknown> = {};
const fetchMock = vi.fn();

beforeEach(() => {
  for (const k of Object.keys(storage)) delete storage[k];
  fetchMock.mockReset();
  // @ts-expect-error patch chrome global
  globalThis.chrome = {
    storage: {
      local: {
        get: async (key: string) => ({ [key]: storage[key] }),
        set: async (kv: Record<string, unknown>) => { Object.assign(storage, kv) },
      },
    },
  };
  // @ts-expect-error patch fetch
  globalThis.fetch = fetchMock;
});

describe("resolveClinic", () => {
  it("returns null when realmId is null", async () => {
    expect(await resolveClinic(null, "http://localhost:3001/api", "key")).toBeNull();
  });

  it("fetches the backend on cache miss and caches the result", async () => {
    fetchMock.mockResolvedValueOnce({
      ok: true,
      json: async () => ({ clinicId: "cl_abc", clinicSlug: "acme" }),
    });
    const res1 = await resolveClinic("12345", "http://localhost:3001/api", "key");
    expect(res1).toEqual({ clinicId: "cl_abc", clinicSlug: "acme" });
    expect(fetchMock).toHaveBeenCalledOnce();

    // Second call hits cache
    const res2 = await resolveClinic("12345", "http://localhost:3001/api", "key");
    expect(res2).toEqual({ clinicId: "cl_abc", clinicSlug: "acme" });
    expect(fetchMock).toHaveBeenCalledOnce();
  });

  it("returns null on 404", async () => {
    fetchMock.mockResolvedValueOnce({ ok: false, status: 404, json: async () => ({}) });
    const res = await resolveClinic("99999", "http://localhost:3001/api", "key");
    expect(res).toBeNull();
  });

  it("returns null on network error", async () => {
    fetchMock.mockRejectedValueOnce(new Error("boom"));
    const res = await resolveClinic("12345", "http://localhost:3001/api", "key");
    expect(res).toBeNull();
  });
});
