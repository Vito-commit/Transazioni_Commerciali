import os
import pytest
from pathlib import Path

from app.data import create_spark, load_data  # :contentReference[oaicite:1]{index=1}
from app import queries as q                  # :contentReference[oaicite:2]{index=2}


# -----------------------
# Fixtures (una volta sola)
# -----------------------

@pytest.fixture(scope="session")
def spark():
    s = create_spark("InstacartAggregationsTests")  # usa la tua create_spark :contentReference[oaicite:3]{index=3}
    s.sparkContext.setLogLevel("WARN")
    yield s
    s.stop()


@pytest.fixture(scope="session")
def base_path():
    """
    Risolve la cartella /data come fa main.py e api.py.
    Se vuoi, puoi anche settare env DATA_DIR.
    """
    env_dir = os.getenv("DATA_DIR")
    if env_dir:
        return env_dir

    # progetto/
    #   app/
    #   tests/
    #   data/
    return str(Path(__file__).resolve().parent.parent / "data")


@pytest.fixture(scope="session")
def data_bundle(spark, base_path):
    data = load_data(spark, base_path=base_path, create_temp_views=False)  # :contentReference[oaicite:4]{index=4}
    # cache dimension tables come fai in main/api
    data.products.cache()
    data.departments.cache()
    data.aisles.cache()
    # materializza cache (evita lentezze nei test)
    _ = data.products.count()
    _ = data.departments.count()
    _ = data.aisles.count()
    return data


# -----------------------
# Test QUERIES (consigliati)
# -----------------------

def test_kpi_overview(data_bundle):
    df = q.kpi_overview_from_splits(data_bundle.orders, data_bundle.op_prior, data_bundle.op_train)
    rows = df.collect()
    assert len(rows) == 1
    r = rows[0].asDict()
    assert r["num_orders"] > 0
    assert r["num_items"] > 0
    assert r["avg_items_per_order"] > 0


def test_top_products_shape(data_bundle):
    df = q.top_products_from_splits(data_bundle.op_prior, data_bundle.op_train, data_bundle.products, n=10)
    rows = df.collect()
    assert 1 <= len(rows) <= 10
    # controlli minimi su colonne attese
    first = rows[0].asDict()
    assert "product_id" in first
    assert "product_name" in first
    assert "times_bought" in first


def test_orders_by_dow_has_7_buckets_or_less(data_bundle):
    df = q.orders_by_dow(data_bundle.orders)
    rows = df.collect()
    assert 1 <= len(rows) <= 7
    # order_dow dovrebbe essere tra 0 e 6
    for r in rows:
        assert 0 <= int(r["order_dow"]) <= 6


def test_reorder_rate_global_range(data_bundle):
    df = q.reorder_rate_global_from_splits(data_bundle.op_prior, data_bundle.op_train)
    rate = df.collect()[0]["reorder_rate"]
    assert 0.0 <= float(rate) <= 1.0


# -----------------------
# Test API (opzionali ma “fanno scena”)
# -----------------------

def test_api_health_works(data_bundle, monkeypatch):
    """
    Importa app.api (che crea spark+data globali) :contentReference[oaicite:6]{index=6}
    e poi patcha 'data' con il nostro DataBundle già caricato.
    """
    from fastapi.testclient import TestClient
    import app.api as api_mod  # :contentReference[oaicite:7]{index=7}

    # patcha data globale usata dagli endpoint
    monkeypatch.setattr(api_mod, "data", data_bundle, raising=True)

    client = TestClient(api_mod.app)
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json().get("status") == "ok"


def test_api_top_products(data_bundle, monkeypatch):
    from fastapi.testclient import TestClient
    import app.api as api_mod  # :contentReference[oaicite:8]{index=8}

    monkeypatch.setattr(api_mod, "data", data_bundle, raising=True)

    client = TestClient(api_mod.app)
    r = client.get("/top-products?n=5")
    assert r.status_code == 200
    body = r.json()
    assert isinstance(body, list)
    assert 1 <= len(body) <= 5
    assert "product_id" in body[0]
    assert "product_name" in body[0]
    assert "times_bought" in body[0]
