export function ResultTable({ rows }: { rows: any[] }) {
    if (!rows || rows.length === 0) return <div>Nessun risultato.</div>;

    const cols = Object.keys(rows[0]);

    return (
        <div style={{ overflowX: 'auto', border: '1px solid #ddd', borderRadius: 8 }}>
            <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                <thead>
                <tr>
                    {cols.map((c) => (
                        <th key={c} style={{ textAlign: 'left', padding: 8, borderBottom: '1px solid #ddd' }}>
                            {c}
                        </th>
                    ))}
                </tr>
                </thead>
                <tbody>
                {rows.map((r, i) => (
                    <tr key={i}>
                        {cols.map((c) => (
                            <td key={c} style={{ padding: 8, borderBottom: '1px solid #f0f0f0' }}>
                                {String(r[c])}
                            </td>
                        ))}
                    </tr>
                ))}
                </tbody>
            </table>
        </div>
    );
}
