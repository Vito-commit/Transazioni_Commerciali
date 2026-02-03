import {
    ResponsiveContainer,
    BarChart,
    Bar,
    XAxis,
    YAxis,
    Tooltip,
    LineChart,
    Line,
    ReferenceDot,
} from "recharts";

const LABELS: Record<string, string> = {
    order_hour_of_day: "Ora del giorno",
    order_dow: "Giorno della settimana",
    num_orders: "Numero ordini",
    num_items: "Numero prodotti",
    items_sold: "Prodotti venduti",
    times_bought: "Numero acquisti",
    avg_items: "Media prodotti",
    avg_items_per_order: "Prodotti medi per ordine",
    avg_orders_per_user: "Ordini medi per cliente",
    reorder_rate: "Tasso di riacquisto",
    pct: "Percentuale",
    bucket: "Dimensione carrello",
    department: "Reparto",
    product_name: "Prodotto",
    eval_set: "Dataset",
    user_id: "Cliente",
};

function labelOf(key: string) {
    return LABELS[key] ?? key.replace(/_/g, " ");
}


function isNumber(v: unknown): v is number {
    return typeof v === "number" && !Number.isNaN(v);
}

function fmtNumber(v: unknown) {
    if (v === null || v === undefined) return "";
    if (!isNumber(v)) return String(v);
    return Number.isInteger(v) ? v.toLocaleString("it-IT") : v.toFixed(2);
}

function pickXKey(keys: string[]) {
    const preferred = [
        "order_hour_of_day",
        "order_dow",
        "department",
        "department_id",
        "product_name",
        "product_id",
        "eval_set",
        "bucket",
        "user_id",
    ];
    for (const k of preferred) if (keys.includes(k)) return k;
    return keys[0];
}

function pickYKey(keys: string[]) {
    const preferred = [
        "num_orders",
        "times_bought",
        "items_sold",
        "num_items",
        "num_items_sold",
        "avg_items",
        "avg_items_per_order",
        "avg_orders_per_user",
        "reorder_rate",
        "pct",
    ];
    for (const k of preferred) if (keys.includes(k)) return k;

    return keys[1] ?? keys[0];
}

function shouldUseLine(xKey: string) {
    return xKey === "order_hour_of_day" || xKey === "order_dow";
}

function CustomTooltip(props: any) {
    const { active, payload, label } = props;
    if (!active || !payload || payload.length === 0) return null;

    const row = payload[0]?.payload ?? {};
    const title =
        row.product_name ??
        row.department ??
        (label !== undefined ? String(label) : "Dettaglio");

    const metaPairs: Array<[string, unknown]> = [];
    if (row.product_id !== undefined) metaPairs.push(["product_id", row.product_id]);
    if (row.department_id !== undefined) metaPairs.push(["department_id", row.department_id]);
    if (row.eval_set !== undefined) metaPairs.push(["eval_set", row.eval_set]);

    const mainPairs: Array<[string, unknown]> = payload.map((p: any) => [p.name, p.value]);

    if (row.reorder_rate !== undefined && !mainPairs.some(([k]) => k === "reorder_rate")) {
        mainPairs.push(["reorder_rate", row.reorder_rate]);
    }
    if (row.pct !== undefined && !mainPairs.some(([k]) => k === "pct")) {
        mainPairs.push(["pct", row.pct]);
    }

    return (
        <div
            style={{
                border: "1px solid rgba(255,255,255,0.14)",
                background: "rgba(10,14,28,0.92)",
                borderRadius: 14,
                padding: "10px 12px",
                maxWidth: 320,
                boxShadow: "0 20px 50px rgba(0,0,0,.45)",
            }}
        >
            <div style={{ fontWeight: 700, marginBottom: 6 }}>{title}</div>

            {metaPairs.length > 0 && (
                <div style={{ opacity: 0.75, fontSize: 12, marginBottom: 8 }}>
                    {metaPairs.map(([k, v]) => (
                        <span key={k} style={{ marginRight: 10 }}>
              {k}: {String(v)}
            </span>
                    ))}
                </div>
            )}

            <div style={{ display: "grid", gap: 4, fontSize: 13 }}>
                {mainPairs.map(([k, v]) => (
                    <div key={k} style={{display: "flex", justifyContent: "space-between", gap: 12}}>
                        <span style={{opacity: 0.8}}>{labelOf(String(k))}</span>
                        <span style={{fontWeight: 650}}>{fmtNumber(v)}</span>
                    </div>
                ))}
            </div>
        </div>
    );
}

export function SimpleChart({ rows }: { rows: any[] }) {
    // ✅ se non ho abbastanza righe, niente box nero
    if (!rows || rows.length < 2) return null;

    const keys = Object.keys(rows[0] ?? {});
    if (keys.length < 2) return null;

    const xKey = pickXKey(keys);
    const yKey = pickYKey(keys);

    // evita warning TS in certi setup
    void yKey;

    // ✅ se xKey e yKey sono uguali (fallback sballato), non disegnare
    if (xKey === yKey) return null;

    // ✅ servono almeno 2 valori numerici validi per fare un grafico sensato
    const numericVals = rows
        .map((r) => r?.[yKey])
        .filter(isNumber);

    if (numericVals.length < 2) return null;

    // highlight max
    let maxRow: any = null;
    let maxVal = -Infinity;
    for (const r of rows) {
        const v = r?.[yKey];
        if (isNumber(v) && v > maxVal) {
            maxVal = v;
            maxRow = r;
        }
    }

    const line = shouldUseLine(xKey);

    const axes = (
        <>
            <XAxis dataKey={xKey} tick={{ fontSize: 12 }} name={labelOf(xKey)} />
            <YAxis tick={{ fontSize: 12 }} name={labelOf(yKey)} />
            <Tooltip content={(p) => <CustomTooltip {...p} />} />
            {maxRow && (
                <ReferenceDot
                    x={maxRow[xKey]}
                    y={maxRow[yKey]}
                    r={6}
                    fill="rgba(120,200,255,0.95)"
                    stroke="rgba(255,255,255,0.85)"
                    strokeWidth={2}
                />
            )}
        </>
    );

    return (
        <div style={{ width: "100%", height: 320 }}>
            <ResponsiveContainer>
                {line ? (
                    <LineChart data={rows}>
                        {axes}
                        <Line type="monotone" dataKey={yKey} name={labelOf(yKey)} dot={false} activeDot={{ r: 6 }} />
                    </LineChart>
                ) : (
                    <BarChart data={rows}>
                        {axes}
                        <Bar dataKey={yKey} name={labelOf(yKey)} radius={[8, 8, 0, 0]} />
                    </BarChart>
                )}
            </ResponsiveContainer>
        </div>
    );
}
