// Frontend/src/pages/queryRegistry.ts
import { endpoints } from "../api/endpoints";
import type { QuerySelection as QS } from "../components/QueryPicker";
import type { TimedResult } from "../api/client";


/**
 * Categorie per la sidebar
 */
export type QueryGroup = "KPI" | "Prodotti" | "Reparti" | "Tempo" | "Clienti" | "Dataset";

/**
 * Chiavi query disponibili
 */
export type QueryKey =
    | "KPI"
    | "TOP_PRODUCTS"
    | "REORDER_RATE_GLOBAL"
    | "TOP_REORDERED_PRODUCTS"
    | "SALES_BY_DEPT"
    | "REORDER_BY_DEPT"
    | "ORDERS_BY_HOUR"
    | "ORDERS_BY_DOW"
    | "AVG_ITEMS_BY_DOW"
    | "BASKET_BUCKETS"
    | "AVG_ORDERS_PER_USER"
    | "TOP_USERS_BY_ORDERS"
    | "ORDERS_COUNT_BY_EVAL_SET"
    | "ITEMS_SOLD_BY_EVAL_SET"
    | "AVG_ITEMS_BY_EVAL_SET"
    | "REORDER_RATE_BY_EVAL_SET"
    | "KMEANS_USER_CLUSTERS"
    | "KMEANS_USER_CLUSTER_PROFILE";

/**
 * Selezione query (con parametri).
 */
export type QuerySelection = Omit<QS, "key"> & { key: QueryKey };

export type QueryDef = {
    key: QueryKey;
    label: string;
    description?: string;
    group: QueryGroup;
    needsN?: boolean;
    needsMinCount?: boolean;
    fetch: (s: QuerySelection) => Promise<TimedResult<any[]>>;

};

export const QUERY_DEFS: QueryDef[] = [
    // ---------------- KPI ----------------
    {
        key: "KPI",
        label: "Panoramica KPI",
        description: "Numero ordini, numero items e media items/ordine.",
        group: "KPI",
        fetch: () => endpoints.kpi(),
    },
    {
        key: "REORDER_RATE_GLOBAL",
        label: "Fidelizzazione globale",
        description: "Reorder rate medio complessivo (prior + train).",
        group: "KPI",
        fetch: () => endpoints.reorderRateGlobal(),
    },
    {
        key: "AVG_ORDERS_PER_USER",
        label: "Frequenza clienti",
        description: "Numero medio di ordini per utente.",
        group: "KPI",
        fetch: () => endpoints.avgOrdersPerUser(),
    },

    // ---------------- Prodotti ----------------
    {
        key: "TOP_PRODUCTS",
        label: "Prodotti più venduti",
        description: "Top prodotti più acquistati (prior + train).",
        group: "Prodotti",
        needsN: true,
        fetch: (s) => endpoints.topProducts(s.n),
    },
    {
        key: "TOP_REORDERED_PRODUCTS",
        label: "Prodotti più riacquistati",
        description: "Prodotti con reorder rate più alto (con soglia min_count).",
        group: "Prodotti",
        needsN: true,
        needsMinCount: true,
        fetch: (s) => endpoints.topReorderedProducts(s.n, s.minCount),
    },

    // ---------------- Reparti ----------------
    {
        key: "SALES_BY_DEPT",
        label: "Vendite per reparto",
        description: "Items venduti e percentuale sul totale per reparto.",
        group: "Reparti",
        fetch: () => endpoints.salesByDepartment(),
    },
    {
        key: "REORDER_BY_DEPT",
        label: "Fidelizzazione per reparto",
        description: "Reorder rate medio e volume items per reparto.",
        group: "Reparti",
        fetch: () => endpoints.reorderRateByDepartment(),
    },

    // ---------------- Tempo ----------------
    {
        key: "ORDERS_BY_HOUR",
        label: "Picchi orari",
        description: "Numero di ordini per fascia oraria (0–23).",
        group: "Tempo",
        fetch: () => endpoints.ordersByHour(),
    },
    {
        key: "ORDERS_BY_DOW",
        label: "Trend settimanale",
        description: "Numero di ordini per giorno della settimana (0–6).",
        group: "Tempo",
        fetch: () => endpoints.ordersByDow(),
    },
    {
        key: "AVG_ITEMS_BY_DOW",
        label: "Intensità d’acquisto (settimanale)",
        description: "Media items/ordine per giorno della settimana.",
        group: "Tempo",
        fetch: () => endpoints.avgItemsByDow(),
    },

    // ---------------- Clienti ----------------
    {
        key: "TOP_USERS_BY_ORDERS",
        label: "Top clienti",
        description: "Utenti con il maggior numero di ordini.",
        group: "Clienti",
        needsN: true,
        fetch: (s) => endpoints.topUsersByOrders(s.n),
    },
    {
        key: "BASKET_BUCKETS",
        label: "Dimensione carrello",
        description: "Distribuzione della dimensione carrello (bucket).",
        group: "Clienti",
        fetch: () => endpoints.basketSizeBuckets(),
    },

    // ---------------- Dataset / Split ----------------
    {
        key: "ORDERS_COUNT_BY_EVAL_SET",
        label: "Ordini per split",
        description: "Numero ordini per eval_set in orders.csv.",
        group: "Dataset",
        fetch: () => endpoints.ordersCountByEvalSet(),
    },
    {
        key: "ITEMS_SOLD_BY_EVAL_SET",
        label: "Items venduti per split",
        description: "Numero items venduti in prior vs train.",
        group: "Dataset",
        fetch: () => endpoints.itemsSoldByEvalSet(),
    },
    {
        key: "AVG_ITEMS_BY_EVAL_SET",
        label: "Media items/ordine per split",
        description: "Media items/ordine in prior vs train.",
        group: "Dataset",
        fetch: () => endpoints.avgItemsPerOrderByEvalSet(),
    },
    {
        key: "REORDER_RATE_BY_EVAL_SET",
        label: "Fidelizzazione per split",
        description: "Reorder rate medio in prior vs train.",
        group: "Dataset",
        fetch: () => endpoints.reorderRateByEvalSet(),
    },
    {
        key: "KMEANS_USER_CLUSTER_PROFILE",
        label: "K-Means: profilo cluster",
        description: "1 riga per cluster con medie (ordini, basket, reorder, ecc.)",
        group: "Clienti",
        fetch: () => endpoints.kmeansUserClusterProfile(5, 42),
    },
    {
        key: "KMEANS_USER_CLUSTERS",
        label: "K-Means: utenti → cluster",
        description: "Assegna a ogni user_id un cluster (utile per tabella e filtri).",
        group: "Clienti",
        fetch: () => endpoints.kmeansUserClusters(5, 42, 1000), // limit 1000
    },

];

export function getQueryDef(key: QueryKey) {
    return QUERY_DEFS.find((q) => q.key === key)!;
}
