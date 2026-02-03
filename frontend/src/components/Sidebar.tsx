import { QUERY_DEFS } from "../pages/queryRegistry";

export function Sidebar(props: {
    activeKey: string;
    onSelect: (key: string) => void;
}) {
    const groups = ["KPI", "Prodotti", "Reparti", "Tempo", "Clienti", "Dataset"] as const;

    return (
        <aside className="sidebar card">
            <div className="sidebarTitle">Insights</div>
            <div className="sidebarSub">Seleziona un’analisi</div>

            {groups.map((g) => {
                const items = QUERY_DEFS.filter((q) => q.group === g);
                if (items.length === 0) return null;

                return (
                    <div key={g} className="sidebarGroup">
                        <div className="sidebarGroupTitle">{g}</div>
                        <div className="sidebarList">
                            {items.map((q) => {
                                const active = q.key === props.activeKey;
                                return (
                                    <button
                                        key={q.key}
                                        className={`sidebarItem ${active ? "active" : ""}`}
                                        onClick={() => props.onSelect(q.key)}
                                        title={q.description ?? q.label}
                                    >
                                        <span className="dot" />
                                        <span className="label">{q.label}</span>
                                    </button>
                                );
                            })}
                        </div>
                    </div>
                );
            })}
        </aside>
    );
}
