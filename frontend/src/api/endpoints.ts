// Frontend/src/api/endpoints.ts
import { apiGetTimed } from "./client";


/**
 * Il backend restituisce sempre una LISTA di righe (df_to_json).
 * Per alcune query la lista contiene 1 sola riga.
 */
export type OneRow<T> = [T] | T[];

// --------------------
// Types: KPI / overview
// --------------------
export type KPIOverviewRow = {
    num_orders: number;
    num_items: number;
    avg_items_per_order: number; // double
};

// --------------------
// Types: Products
// --------------------
export type TopProductRow = {
    product_id: number;
    product_name: string;
    times_bought: number;
};

export type ReorderRateGlobalRow = {
    reorder_rate: number; // 0..1
};

export type TopReorderedProductRow = {
    product_id: number;
    product_name: string;
    times_bought: number;
    reorder_rate: number; // 0..1
};

// --------------------
// Types: Departments
// --------------------
export type SalesByDepartmentRow = {
    department_id: number;
    department: string;
    items_sold: number;
    pct: number; // percentuale (0..100)
};

export type ReorderRateByDepartmentRow = {
    department_id: number;
    department: string;
    reorder_rate: number; // 0..1
    items: number;
};

// --------------------
// Types: Time (orders)
// --------------------
export type OrdersByDowRow = {
    order_dow: number; // 0..6
    num_orders: number;
};

export type OrdersByHourRow = {
    order_hour_of_day: number; // 0..23
    num_orders: number;
};

export type AvgItemsByDowRow = {
    order_dow: number;
    avg_items: number;
};

// --------------------
// Types: Basket / Users
// --------------------
export type BasketSizeBucketRow = {
    bucket: "1-5" | "6-10" | "11-20" | "21+";
    num_orders: number;
};

export type AvgOrdersPerUserRow = {
    avg_orders_per_user: number;
};

export type TopUsersByOrdersRow = {
    user_id: number;
    num_orders: number;
};

// --------------------
// Types: eval_set
// --------------------
export type OrdersCountByEvalSetRow = {
    eval_set: string; // di solito "prior"/"train" in orders.csv
    num_orders: number;
};

export type ItemsSoldByEvalSetRow = {
    eval_set: "prior" | "train";
    num_items: number;
};

export type AvgItemsPerOrderByEvalSetRow = {
    eval_set: "prior" | "train";
    avg_items: number;
};

export type ReorderRateByEvalSetRow = {
    eval_set: "prior" | "train";
    reorder_rate: number;
};

// --------------------
// Types: ML (K-Means)
// --------------------
export type KMeansUserClusterRow = {
    user_id: number;
    cluster: number;
};

export type KMeansUserClusterProfileRow = {
    cluster: number;
    users: number;
    avg_num_orders: number;
    avg_basket_size: number;
    avg_reorder_rate: number;
    avg_dept_diversity: number;
    avg_days_between: number;
    avg_hour: number;
};


// --------------------
// Endpoints
// --------------------
export const endpoints = {
    // utility
    health: () => apiGetTimed<{ status: string }>("/health"),

    // KPI
    kpi: () => apiGetTimed<OneRow<KPIOverviewRow>>("/kpi"),

    // products
    topProducts: (n = 10) =>
        apiGetTimed<TopProductRow[]>(`/top-products?n=${n}`),

    reorderRateGlobal: () =>
        apiGetTimed<OneRow<ReorderRateGlobalRow>>("/reorder-rate-global"),

    topReorderedProducts: (n = 10, minCount = 200) =>
        apiGetTimed<TopReorderedProductRow[]>(
            `/top-reordered-products?n=${n}&min_count=${minCount}`
        ),

    // departments
    salesByDepartment: () =>
        apiGetTimed<SalesByDepartmentRow[]>("/sales-by-department"),

    reorderRateByDepartment: () =>
        apiGetTimed<ReorderRateByDepartmentRow[]>("/reorder-rate-by-department"),

    // time
    ordersByDow: () =>
        apiGetTimed<OrdersByDowRow[]>("/orders-by-dow"),

    ordersByHour: () =>
        apiGetTimed<OrdersByHourRow[]>("/orders-by-hour"),

    avgItemsByDow: () =>
        apiGetTimed<AvgItemsByDowRow[]>("/avg-items-by-dow"),

    // basket / users
    basketSizeBuckets: () =>
        apiGetTimed<BasketSizeBucketRow[]>("/basket-size-buckets"),

    avgOrdersPerUser: () =>
        apiGetTimed<OneRow<AvgOrdersPerUserRow>>("/avg-orders-per-user"),

    topUsersByOrders: (n = 10) =>
        apiGetTimed<TopUsersByOrdersRow[]>(`/top-users-by-orders?n=${n}`),

    // eval_set
    ordersCountByEvalSet: () =>
        apiGetTimed<OrdersCountByEvalSetRow[]>("/orders-count-by-eval-set"),

    itemsSoldByEvalSet: () =>
        apiGetTimed<ItemsSoldByEvalSetRow[]>("/items-sold-by-eval-set"),

    avgItemsPerOrderByEvalSet: () =>
        apiGetTimed<AvgItemsPerOrderByEvalSetRow[]>("/avg-items-per-order-by-eval-set"),

    reorderRateByEvalSet: () =>
        apiGetTimed<ReorderRateByEvalSetRow[]>("/reorder-rate-by-eval-set"),

    // --------------------
    // ML (K-Means)
    // --------------------
    kmeansUserClusters: (k = 5, seed = 42, limit = 1000, cluster?: number) => {
        const params = new URLSearchParams({
            k: String(k),
            seed: String(seed),
            limit: String(limit),
        });
        if (cluster !== undefined) params.set("cluster", String(cluster));
        return apiGetTimed<KMeansUserClusterRow[]>(`/kmeans-user-clusters?${params.toString()}`);
    },

    kmeansUserClusterProfile: (k = 5, seed = 42) => {
        const params = new URLSearchParams({
            k: String(k),
            seed: String(seed),
        });
        return apiGetTimed<KMeansUserClusterProfileRow[]>(`/kmeans-user-cluster-profile?${params.toString()}`);
    },

};
