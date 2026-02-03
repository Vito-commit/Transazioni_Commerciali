export function KpiCards({ title, items }: { title: string; items: { label: string; value: string }[] }) {
    return (
        <div className="card" style={{ marginBottom: 12 }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline", gap: 12 }}>
                <h2 style={{ margin: 0 }}>{title}</h2>
                <span className="badge">{items.length} metriche</span>
            </div>

            <div className="grid3" style={{ marginTop: 12 }}>
                {items.map((it) => (
                    <div key={it.label} className="card" style={{ boxShadow: "none" }}>
                        <div className="kpiLabel">{it.label}</div>
                        <div className="kpiValue">{it.value}</div>
                    </div>
                ))}
            </div>
        </div>
    );
}
