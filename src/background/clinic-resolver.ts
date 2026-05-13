export interface ResolvedClinic {
  clinicId: string
  clinicSlug: string
}

const CACHE_KEY = 'realmIdClinicMap'

async function readCache(): Promise<Record<string, ResolvedClinic>> {
  const { [CACHE_KEY]: cache } = await chrome.storage.local.get(CACHE_KEY)
  return (cache as Record<string, ResolvedClinic>) ?? {}
}

async function writeCache(map: Record<string, ResolvedClinic>) {
  await chrome.storage.local.set({ [CACHE_KEY]: map })
}

export async function resolveClinic(
  realmId: string | null,
  apiBaseUrl: string,
  extensionApiKey: string
): Promise<ResolvedClinic | null> {
  if (!realmId) return null
  const cache = await readCache()
  if (cache[realmId]) return cache[realmId]

  try {
    const res = await fetch(`${apiBaseUrl}/clinics/by-realm/${encodeURIComponent(realmId)}`, {
      headers: { 'X-Extension-Api-Key': extensionApiKey },
    })
    if (!res.ok) return null
    const json = await res.json()
    if (!json?.clinicId || !json?.clinicSlug) return null
    const resolved: ResolvedClinic = { clinicId: json.clinicId, clinicSlug: json.clinicSlug }
    cache[realmId] = resolved
    await writeCache(cache)
    return resolved
  } catch {
    return null
  }
}
