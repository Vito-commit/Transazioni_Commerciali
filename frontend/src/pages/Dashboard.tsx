import { useEffect, useMemo, useState } from "react";
import { AnimatedBackground } from "../components/AnimatedBackground";
import { Sidebar } from "../components/Sidebar";
import { ResultTable } from "../components/ResultTable";
import { SimpleChart } from "../components/SimpleChart";
import { Loading } from "../components/Loading";
import { ErrorBox } from "../components/ErrorBox";
import { PerfPill } from "../components/PerfPill";
import { getQueryDef } from "./queryRegistry";

type QuerySelection = {
    key: any;
    n: number;
    minCount: number;
};

export function Dashboard() {
    const [sel, setSel] = useState<QuerySelection>({ key: "KPI", n: 10, minCount: 200 });
    const [rows, setRows] = useState<any[]>([]);
    const [loading, setLoading] = useState(false);
    const [err, setErr] = useState("");
    const [reloadTick, setReloadTick] = useState(0);

    const [perfMs, setPerfMs] = useState<number | null>(null); // ✅ SOLO NUMERO MS

    const def = useMemo(() => getQueryDef(sel.key), [sel.key]);

    useEffect(() => {
        let cancelled = false;

        async function run() {
            setLoading(true);
            setErr("");
            setPerfMs(null);

            try {
                const out = await def.fetch(sel as any); // { data, timings }

                if (!cancelled) {
                    setRows(out.data);

                    // ✅ numero unico: preferisci server, fallback client
                    const ms = out.timings.serverMs ?? out.timings.clientMs;
                    setPerfMs(ms);
                }
            } catch (e: any) {
                if (!cancelled) setErr(e?.message || "Errore sconosciuto");
            } finally {
                if (!cancelled) setLoading(false);
            }
        }

        run();
        return () => {
            cancelled = true;
        };
    }, [def, sel, reloadTick]);

    const needsN = !!def.needsN;
    const needsMin = !!def.needsMinCount;

    return (
        <>
            <AnimatedBackground />

            <div className="appLayout">
                <Sidebar activeKey={sel.key} onSelect={(key) => setSel((s) => ({ ...s, key }))} />

                <main className="mainPanel">
                    <div className="card">
                        <div className="panelTitle">
                            <div>
                                <h2 style={{ margin: 0 }}>{def.label}</h2>
                                <p style={{ margin: "6px 0 0", color: "var(--muted)", fontSize: 13 }}>
                                    {def.description ?? "Seleziona un’analisi dalla sidebar."}
                                </p>
                            </div>

                            {/* se vuoi lasciare righe ok, altrimenti puoi anche rimuoverlo */}
                            <span className="badge">righe: {rows.length}</span>
                        </div>

                        <hr className="hr" />

                        {err && <ErrorBox error={err} />}
                        {loading ? (
                            <Loading />
                        ) : (
                            <div key={sel.key} className="swap">
                                <SimpleChart rows={rows} />
                                <div style={{ height: 12 }} />
                                <ResultTable rows={rows} />
                            </div>
                        )}
                    </div>
                </main>

                <aside className="rightPanel">
                    <div className="card">
                        <div className="panelTitle">
                            <h3 style={{margin: 0}}>Controls</h3>
                            <button onClick={() => setReloadTick((x) => x + 1)}>↻ Refresh</button>
                        </div>


                        <hr className="hr"/>

                        {needsN && (
                            <div style={{marginBottom: 12}}>
                                <label>Top N</label>
                                <input
                                    type="number"
                                    min={1}
                                    max={100}
                                    value={sel.n}
                                    onChange={(e) => setSel((s) => ({...s, n: Number(e.target.value)}))}
                                />
                            </div>
                        )}

                        {needsMin && (
                            <div style={{marginBottom: 12}}>
                                <label>Min count</label>
                                <input
                                    type="number"
                                    min={1}
                                    max={1000000}
                                    value={sel.minCount}
                                    onChange={(e) => setSel((s) => ({...s, minCount: Number(e.target.value)}))}
                                />
                            </div>
                        )}

                        <div className="badge" style={{marginTop: 8}}>
                            Tip: usa Top N e Min count per rendere la demo più “interattiva”.
                        </div>

                        {/* opzionale: se vuoi vedere debug sotto (puoi anche toglierlo) */}
                        {/* <div className="badge" style={{ marginTop: 8 }}>
              {timings ? `server: ${timings.serverMs?.toFixed(0) ?? "?"} · client: ${timings.clientMs.toFixed(0)}` : "—"}
            </div> */}
                    </div>

                    <div className="card" style={{marginTop: 12}}>
                        <div className="panelTitle">
                            <h3 style={{margin: 0}}>Performance</h3>
                        </div>

                        <hr className="hr"/>

                        <div style={{display: "flex", justifyContent: "flex-end"}}>
                            <PerfPill ms={perfMs}/>
                        </div>
                    </div>


                </aside>
            </div>
        </>
    );
}
