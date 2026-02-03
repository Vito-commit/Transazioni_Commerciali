export async function apiGet<T>(path: string): Promise<T> {
    const res = await fetch(`/api${path}`);
    if (!res.ok) {
        const text = await res.text().catch(() => '');
        throw new Error(`HTTP ${res.status} - ${text || res.statusText}`);
    }
    return res.json() as Promise<T>;
}

export type Timings = {
    clientMs: number;
    serverMs?: number;
};

export type TimedResult<T> = {
    data: T;
    timings: Timings;
};

export async function apiGetTimed<T>(path: string): Promise<TimedResult<T>> {
    const t0 = performance.now();

    const res = await fetch(`/api${path}`);

    const t1 = performance.now();

    if (!res.ok) {
        const text = await res.text().catch(() => '');
        throw new Error(`HTTP ${res.status} - ${text || res.statusText}`);
    }

    const serverHeader = res.headers.get("X-Process-Time-ms");
    const serverMs = serverHeader ? Number(serverHeader) : undefined;

    const data = (await res.json()) as T;

    return {
        data,
        timings: {
            clientMs: t1 - t0,
            serverMs,
        },
    };
}
