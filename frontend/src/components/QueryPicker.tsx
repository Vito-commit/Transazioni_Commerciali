import { QUERY_DEFS, getQueryDef } from "../pages/queryRegistry";

export type QuerySelection = {
    key: (typeof QUERY_DEFS)[number]["key"];
    n: number;
    minCount: number;
};

export function QueryPicker(props: {
    value: QuerySelection;
    onChange: (v: QuerySelection) => void;
}) {
    const v = props.value;
    const def = getQueryDef(v.key);

    return (
        <div style={{ display: "flex", gap: 12, alignItems: "end", flexWrap: "wrap" }}>
            <div>
                <label>Query</label><br />
                <select
                    value={v.key}
                    onChange={(e) => props.onChange({ ...v, key: e.target.value as any })}
                >
                    {QUERY_DEFS.map((q) => (
                        <option key={q.key} value={q.key}>
                            {q.label}
                        </option>
                    ))}
                </select>
            </div>

            {def.needsN && (
                <div>
                    <label>Top N</label><br />
                    <input
                        type="number"
                        min={1}
                        max={100}
                        value={v.n}
                        onChange={(e) => props.onChange({ ...v, n: Number(e.target.value) })}
                    />
                </div>
            )}

            {def.needsMinCount && (
                <div>
                    <label>Min count</label><br />
                    <input
                        type="number"
                        min={1}
                        max={1000000}
                        value={v.minCount}
                        onChange={(e) => props.onChange({ ...v, minCount: Number(e.target.value) })}
                    />
                </div>
            )}
        </div>
    );
}
