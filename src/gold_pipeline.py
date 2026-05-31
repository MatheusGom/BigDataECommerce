"""
gold_pipeline.py
================
Camada Gold — Pipeline de Features para Sistema de Recomendação
Dataset: Olist Brazilian E-Commerce (Silver Layer)

Gera em data/gold/:
  1. gold_user_product_interactions.csv   — Matriz usuário × produto (collaborative filtering)
  2. gold_product_features.csv            — Features de produto (content-based)
  3. gold_customer_profile.csv            — Perfil de cliente (segmentação / cold start)
  4. gold_category_affinity.csv           — Afinidade cliente × categoria (cross-category)
  5. gold_product_cooccurrence.csv        — Produtos comprados juntos (regras de associação)
  6. gold_top_products_by_segment.csv     — Top-N por segmento (cold start / fallback)
  7. gold_schema.json                     — Contrato de dados para backend/API
  8. gold_feature_config.json             — Configuração de features para o time de ML
  9. README_gold.md                       — Documentação da camada Gold
"""

import os
import json
import logging
from datetime import datetime, timezone
from itertools import combinations

import pandas as pd
import numpy as np

# ─── Configuração ────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_DIR   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")
GOLD_DIR   = os.path.join(BASE_DIR, "data", "gold")
TOP_N      = 10   # itens por segmento no ranking de cold start

# ─── Utilitários ─────────────────────────────────────────────────────────────

def read_csv(filename: str) -> pd.DataFrame:
    path = os.path.join(SILVER_DIR, filename)
    try:
        return pd.read_csv(path, encoding="utf-8")
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="latin1")


def save_csv(df: pd.DataFrame, filename: str) -> None:
    os.makedirs(GOLD_DIR, exist_ok=True)
    path = os.path.join(GOLD_DIR, filename)
    df.to_csv(path, index=False, encoding="utf-8")
    log.info(f"  ✓ {filename:50s} → {len(df):>7,} linhas")


def minmax_normalize(series: pd.Series) -> pd.Series:
    mn, mx = series.min(), series.max()
    if mx == mn:
        return pd.Series(0.0, index=series.index)
    return (series - mn) / (mx - mn)


# ─── Carga dos dados Silver ───────────────────────────────────────────────────

def load_silver() -> dict:
    log.info("Carregando dados da camada Silver...")
    data = {
        "customers":    read_csv("olist_customers_dataset.csv"),
        "orders":       read_csv("olist_orders_dataset.csv"),
        "order_items":  read_csv("olist_order_items_dataset.csv"),
        "products":     read_csv("olist_products_dataset.csv"),
        "sellers":      read_csv("olist_sellers_dataset.csv"),
        "reviews":      read_csv("olist_order_reviews_dataset.csv"),
        "payments":     read_csv("olist_order_payments_dataset.csv"),
        "translation":  read_csv("product_category_name_translation.csv"),
    }
    for name, df in data.items():
        log.info(f"  ✓ {name:20s} → {len(df):>7,} linhas | colunas: {list(df.columns)}")
    return data


# ─── 1. Interações Usuário × Produto ─────────────────────────────────────────

def build_user_product_interactions(data: dict) -> pd.DataFrame:
    log.info("Construindo gold_user_product_interactions...")

    items    = data["order_items"][["order_id", "product_id", "price", "freight_value", "total_item_value"]].copy()
    orders   = data["orders"][["order_id", "customer_id"]].copy()
    customers = data["customers"][["customer_id", "customer_unique_id"]].copy()
    reviews  = data["reviews"][["order_id", "review_score", "sentiment"]].copy()
    products = data["products"][["product_id", "product_category_name"]].copy()
    trans    = data["translation"].rename(columns={
        "product_category_name": "product_category_name",
        "product_category_name_english": "product_category_name_english",
    })

    # Mapear customer_id → customer_unique_id
    items = items.merge(orders, on="order_id", how="left")
    items = items.merge(customers, on="customer_id", how="left")

    # Agregar compras por (customer_unique_id, product_id)
    agg = items.groupby(["customer_unique_id", "product_id"], dropna=True).agg(
        purchase_count=("order_id", "count"),
        total_spent=("total_item_value", "sum"),
    ).reset_index()

    # Associar review (uma review por pedido — pegar a média por produto × cliente)
    reviews_items = items[["order_id", "product_id", "customer_unique_id"]].merge(
        reviews[["order_id", "review_score", "sentiment"]], on="order_id", how="left"
    )
    review_agg = reviews_items.groupby(["customer_unique_id", "product_id"], dropna=True).agg(
        review_score=("review_score", "mean"),
        sentiment=("sentiment", lambda x: x.mode()[0] if not x.dropna().empty else None),
    ).reset_index()

    df = agg.merge(review_agg, on=["customer_unique_id", "product_id"], how="left")

    # Categoria do produto (em inglês)
    products = products.merge(trans, on="product_category_name", how="left")
    df = df.merge(
        products[["product_id", "product_category_name_english"]],
        on="product_id", how="left"
    )

    # Implicit score: compras (peso 1.0) + review normalizado (peso 0.5)
    review_norm = df["review_score"].fillna(3.0) / 5.0
    df["implicit_score"] = (df["purchase_count"] * 1.0) + (review_norm * 0.5)

    df = df[[
        "customer_unique_id", "product_id", "product_category_name_english",
        "purchase_count", "total_spent", "review_score", "sentiment", "implicit_score",
    ]].sort_values("implicit_score", ascending=False).reset_index(drop=True)

    save_csv(df, "gold_user_product_interactions.csv")
    return df


# ─── 2. Features de Produto ───────────────────────────────────────────────────

def build_product_features(data: dict) -> pd.DataFrame:
    log.info("Construindo gold_product_features...")

    items    = data["order_items"][["order_id", "product_id", "seller_id", "price", "freight_value"]].copy()
    reviews  = data["reviews"][["order_id", "review_score", "sentiment"]].copy()
    products = data["products"][["product_id", "product_category_name"]].copy()
    sellers  = data["sellers"][["seller_id", "seller_state"]].copy()
    trans    = data["translation"]

    # Categoria traduzida
    products = products.merge(trans, on="product_category_name", how="left")

    # Métricas de vendas por produto
    sales_agg = items.groupby("product_id", dropna=True).agg(
        avg_price=("price", "mean"),
        avg_freight_value=("freight_value", "mean"),
        total_orders=("order_id", "count"),
    ).reset_index()

    # Vendedor principal por produto (mais frequente)
    main_seller = (
        items.groupby(["product_id", "seller_id"])
        .size().reset_index(name="cnt")
        .sort_values("cnt", ascending=False)
        .drop_duplicates("product_id")[["product_id", "seller_id"]]
    )
    main_seller = main_seller.merge(sellers, on="seller_id", how="left")

    # Métricas de review por produto (via order_id)
    rev_items = items[["order_id", "product_id"]].merge(reviews, on="order_id", how="left")
    rev_agg = rev_items.groupby("product_id", dropna=True).agg(
        avg_review_score=("review_score", "mean"),
        total_reviews=("review_score", "count"),
        positive_sentiment_ratio=("sentiment", lambda x: (x == "positivo").sum() / len(x) if len(x) > 0 else 0),
    ).reset_index()

    # Montar tabela final
    df = products[["product_id", "product_category_name", "product_category_name_english"]].copy()
    df = df.merge(sales_agg, on="product_id", how="left")
    df = df.merge(rev_agg, on="product_id", how="left")
    df = df.merge(main_seller[["product_id", "seller_state"]], on="product_id", how="left")

    df = df.round(4)

    save_csv(df, "gold_product_features.csv")
    return df


# ─── 3. Perfil de Cliente ─────────────────────────────────────────────────────

def build_customer_profile(data: dict) -> pd.DataFrame:
    log.info("Construindo gold_customer_profile...")

    customers = data["customers"][["customer_id", "customer_unique_id", "customer_city", "customer_state"]].copy()
    orders    = data["orders"][["order_id", "customer_id", "order_status", "delivery_days", "is_late"]].copy()
    items     = data["order_items"][["order_id", "product_id", "total_item_value"]].copy()
    payments  = data["payments"][["order_id", "payment_type", "payment_installments"]].copy()
    reviews   = data["reviews"][["order_id", "review_score"]].copy()
    products  = data["products"][["product_id", "product_category_name"]].copy()
    trans     = data["translation"]

    # Categoria traduzida
    products = products.merge(trans, on="product_category_name", how="left")
    items = items.merge(products[["product_id", "product_category_name_english"]], on="product_id", how="left")

    # Mapear pedidos → cliente único
    orders = orders.merge(customers[["customer_id", "customer_unique_id"]], on="customer_id", how="left")

    # Métricas de pedidos
    order_agg = orders.groupby("customer_unique_id", dropna=True).agg(
        total_orders=("order_id", "count"),
        avg_delivery_days=("delivery_days", "mean"),
        is_late_buyer_pct=("is_late", lambda x: pd.to_numeric(x.map({"True": 1, "False": 0, True: 1, False: 0}), errors="coerce").mean()),
    ).reset_index()

    # Gastos totais por cliente
    items_cust = items.merge(orders[["order_id", "customer_unique_id"]], on="order_id", how="left")
    spend_agg = items_cust.groupby("customer_unique_id", dropna=True).agg(
        total_spent=("total_item_value", "sum"),
    ).reset_index()

    # Categoria preferida
    cat_pref = (
        items_cust.groupby(["customer_unique_id", "product_category_name_english"])
        .size().reset_index(name="cat_count")
        .sort_values("cat_count", ascending=False)
        .drop_duplicates("customer_unique_id")[["customer_unique_id", "product_category_name_english"]]
        .rename(columns={"product_category_name_english": "preferred_category"})
    )

    # Pagamento preferido e média de parcelas
    pay_cust = payments.merge(orders[["order_id", "customer_unique_id"]], on="order_id", how="left")
    pay_type_pref = (
        pay_cust.groupby(["customer_unique_id", "payment_type"])
        .size().reset_index(name="pay_count")
        .sort_values("pay_count", ascending=False)
        .drop_duplicates("customer_unique_id")[["customer_unique_id", "payment_type"]]
        .rename(columns={"payment_type": "preferred_payment_type"})
    )
    pay_install = pay_cust.groupby("customer_unique_id", dropna=True).agg(
        avg_installments=("payment_installments", "mean"),
    ).reset_index()

    # Média de avaliações dadas
    rev_cust = reviews.merge(orders[["order_id", "customer_unique_id"]], on="order_id", how="left")
    rev_agg = rev_cust.groupby("customer_unique_id", dropna=True).agg(
        avg_review_score=("review_score", "mean"),
    ).reset_index()

    # Montar perfil
    profile = customers[["customer_unique_id", "customer_city", "customer_state"]].drop_duplicates("customer_unique_id")
    profile = profile.merge(order_agg,    on="customer_unique_id", how="left")
    profile = profile.merge(spend_agg,    on="customer_unique_id", how="left")
    profile = profile.merge(rev_agg,      on="customer_unique_id", how="left")
    profile = profile.merge(cat_pref,     on="customer_unique_id", how="left")
    profile = profile.merge(pay_type_pref, on="customer_unique_id", how="left")
    profile = profile.merge(pay_install,  on="customer_unique_id", how="left")

    profile = profile.round(4)

    save_csv(profile, "gold_customer_profile.csv")
    return profile


# ─── 4. Afinidade Cliente × Categoria ────────────────────────────────────────

def build_category_affinity(data: dict) -> pd.DataFrame:
    log.info("Construindo gold_category_affinity...")

    items    = data["order_items"][["order_id", "product_id", "total_item_value"]].copy()
    orders   = data["orders"][["order_id", "customer_id"]].copy()
    customers = data["customers"][["customer_id", "customer_unique_id"]].copy()
    products = data["products"][["product_id", "product_category_name"]].copy()
    reviews  = data["reviews"][["order_id", "review_score"]].copy()
    trans    = data["translation"]

    products = products.merge(trans, on="product_category_name", how="left")
    items = items.merge(products[["product_id", "product_category_name_english"]], on="product_id", how="left")
    items = items.merge(orders, on="order_id", how="left")
    items = items.merge(customers, on="customer_id", how="left")

    # Associar reviews
    items = items.merge(reviews, on="order_id", how="left")

    aff = items.groupby(["customer_unique_id", "product_category_name_english"], dropna=True).agg(
        purchase_count=("order_id", "count"),
        total_spent=("total_item_value", "sum"),
        avg_review_score=("review_score", "mean"),
    ).reset_index()

    # Affinity score normalizado por cliente via transform (compatível com todas versões pandas)
    aff["_review_filled"] = aff["avg_review_score"].fillna(3.0)

    def _minmax_transform(s: pd.Series) -> pd.Series:
        mn, mx = s.min(), s.max()
        if mx == mn:
            return pd.Series(0.0, index=s.index)
        return (s - mn) / (mx - mn)

    aff["purchase_norm"] = aff.groupby("customer_unique_id")["purchase_count"].transform(_minmax_transform)
    aff["spent_norm"]    = aff.groupby("customer_unique_id")["total_spent"].transform(_minmax_transform)
    aff["review_norm"]   = aff.groupby("customer_unique_id")["_review_filled"].transform(_minmax_transform)

    aff["affinity_score"] = (
        aff["purchase_norm"] * 0.5 +
        aff["spent_norm"]    * 0.3 +
        aff["review_norm"]   * 0.2
    ).round(4)

    aff = aff.drop(columns=["_review_filled", "purchase_norm", "spent_norm", "review_norm"])
    aff = aff.sort_values(["customer_unique_id", "affinity_score"], ascending=[True, False]).reset_index(drop=True)

    save_csv(aff, "gold_category_affinity.csv")
    return aff


# ─── 5. Co-ocorrência de Produtos ────────────────────────────────────────────

def build_product_cooccurrence(data: dict) -> pd.DataFrame:
    log.info("Construindo gold_product_cooccurrence...")

    items    = data["order_items"][["order_id", "product_id"]].copy()
    products = data["products"][["product_id", "product_category_name"]].copy()
    trans    = data["translation"]
    products = products.merge(trans, on="product_category_name", how="left")

    # Pedidos com mais de 1 produto
    order_sizes = items.groupby("order_id")["product_id"].count()
    multi_orders = order_sizes[order_sizes > 1].index
    items_multi = items[items["order_id"].isin(multi_orders)]

    # Gerar pares por pedido
    pairs = []
    for order_id, group in items_multi.groupby("order_id"):
        prods = group["product_id"].unique().tolist()
        for a, b in combinations(sorted(prods), 2):
            pairs.append((a, b))

    if not pairs:
        log.warning("Nenhum par de co-ocorrência encontrado.")
        return pd.DataFrame()

    pair_df = pd.DataFrame(pairs, columns=["product_id_a", "product_id_b"])
    cooc = pair_df.groupby(["product_id_a", "product_id_b"]).size().reset_index(name="cooccurrence_count")

    # Frequência individual de cada produto (em pedidos multi-item)
    prod_freq = items_multi.groupby("product_id")["order_id"].nunique().reset_index(name="order_count")
    prod_freq = prod_freq.set_index("product_id")["order_count"].to_dict()

    # Jaccard similarity: |A ∩ B| / |A ∪ B|
    cooc["orders_a"] = cooc["product_id_a"].map(prod_freq).fillna(1)
    cooc["orders_b"] = cooc["product_id_b"].map(prod_freq).fillna(1)
    cooc["cooccurrence_score"] = (
        cooc["cooccurrence_count"] /
        (cooc["orders_a"] + cooc["orders_b"] - cooc["cooccurrence_count"])
    ).round(6)

    # Adicionar categorias
    cat_map = products.set_index("product_id")["product_category_name_english"].to_dict()
    cooc["category_a"] = cooc["product_id_a"].map(cat_map)
    cooc["category_b"] = cooc["product_id_b"].map(cat_map)

    cooc = cooc[["product_id_a", "product_id_b", "category_a", "category_b",
                  "cooccurrence_count", "cooccurrence_score"]]
    cooc = cooc.sort_values("cooccurrence_count", ascending=False).reset_index(drop=True)

    save_csv(cooc, "gold_product_cooccurrence.csv")
    return cooc


# ─── 6. Top-N por Segmento (Cold Start) ──────────────────────────────────────

def build_top_products_by_segment(data: dict, product_features: pd.DataFrame) -> pd.DataFrame:
    log.info("Construindo gold_top_products_by_segment...")

    items     = data["order_items"][["order_id", "product_id", "price"]].copy()
    orders    = data["orders"][["order_id", "customer_id"]].copy()
    customers = data["customers"][["customer_id", "customer_state"]].copy()
    products  = data["products"][["product_id", "product_category_name"]].copy()
    trans     = data["translation"]

    products = products.merge(trans, on="product_category_name", how="left")
    items    = items.merge(orders, on="order_id", how="left")
    items    = items.merge(customers, on="customer_id", how="left")
    items    = items.merge(products[["product_id", "product_category_name_english"]], on="product_id", how="left")

    # Features base de produto para o ranking
    pf = product_features[["product_id", "product_category_name_english",
                             "avg_review_score", "avg_price", "total_orders",
                             "positive_sentiment_ratio"]].copy()

    records = []

    def top_n(subset_items, segment_type, segment_value):
        prod_counts = subset_items.groupby("product_id")["order_id"].count().reset_index(name="seg_orders")
        merged = prod_counts.merge(pf, on="product_id", how="left")
        merged = merged.sort_values(
            ["seg_orders", "avg_review_score"], ascending=[False, False]
        ).head(TOP_N).reset_index(drop=True)
        merged["segment_type"]  = segment_type
        merged["segment_value"] = segment_value
        merged["rank"]          = merged.index + 1
        return merged

    # Global
    records.append(top_n(items, "global", "all"))

    # Por estado
    for state, grp in items.groupby("customer_state"):
        records.append(top_n(grp, "state", state))

    # Por categoria
    for cat, grp in items.groupby("product_category_name_english"):
        records.append(top_n(grp, "category", str(cat)))

    df = pd.concat(records, ignore_index=True)
    df = df[["segment_type", "segment_value", "rank", "product_id",
              "product_category_name_english", "avg_review_score",
              "avg_price", "total_orders", "positive_sentiment_ratio"]]
    df = df.round(4)

    save_csv(df, "gold_top_products_by_segment.csv")
    return df


# ─── 7. Schema JSON ──────────────────────────────────────────────────────────

def build_schema_json(tables: dict) -> None:
    log.info("Construindo gold_schema.json...")

    schema = {
        "version": "1.0",
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "description": "Contrato de dados da camada Gold — Sistema de Recomendação Olist",
        "tables": {},
    }

    type_map = {
        "object":  "string",
        "float64": "float",
        "float32": "float",
        "int64":   "integer",
        "int32":   "integer",
        "bool":    "boolean",
    }

    for name, df in tables.items():
        cols = {}
        for col in df.columns:
            dtype = str(df[col].dtype)
            mapped = type_map.get(dtype, "string")
            col_info = {"type": mapped, "nullable": bool(df[col].isna().any())}
            if mapped in ("float", "integer") and df[col].notna().any():
                col_info["min"] = round(float(df[col].min()), 4)
                col_info["max"] = round(float(df[col].max()), 4)
            cols[col] = col_info

        schema["tables"][name] = {
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": cols,
        }

    path = os.path.join(GOLD_DIR, "gold_schema.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=2, ensure_ascii=False)
    log.info(f"  ✓ gold_schema.json")


# ─── 8. Feature Config JSON ───────────────────────────────────────────────────

def build_feature_config() -> None:
    log.info("Construindo gold_feature_config.json...")

    config = {
        "version": "1.0",
        "description": "Configuração de features para o sistema de recomendação Olist",
        "user_product_interactions": {
            "file": "gold_user_product_interactions.csv",
            "user_id_col": "customer_unique_id",
            "item_id_col": "product_id",
            "implicit_rating_col": "implicit_score",
            "explicit_rating_col": "review_score",
            "rating_scale": {"min": 1, "max": 5},
            "implicit_score_formula": "purchase_count * 1.0 + (review_score / 5.0) * 0.5",
            "numeric_features": ["total_spent", "purchase_count", "implicit_score"],
            "categorical_features": ["product_category_name_english", "sentiment"],
            "notes": "review_score pode ser nulo quando o cliente não avaliou o pedido.",
        },
        "customer_profile": {
            "file": "gold_customer_profile.csv",
            "user_id_col": "customer_unique_id",
            "numeric_features": [
                "total_orders", "total_spent", "avg_review_score",
                "avg_installments", "avg_delivery_days", "is_late_buyer_pct",
            ],
            "categorical_features": [
                "customer_state", "customer_city",
                "preferred_category", "preferred_payment_type",
            ],
            "cold_start_fallback": "gold_top_products_by_segment.csv (segment_type=state)",
        },
        "product_features": {
            "file": "gold_product_features.csv",
            "item_id_col": "product_id",
            "numeric_features": [
                "avg_review_score", "total_reviews", "avg_price",
                "total_orders", "avg_freight_value", "positive_sentiment_ratio",
            ],
            "categorical_features": ["product_category_name_english", "seller_state"],
        },
        "category_affinity": {
            "file": "gold_category_affinity.csv",
            "user_id_col": "customer_unique_id",
            "item_id_col": "product_category_name_english",
            "affinity_col": "affinity_score",
            "affinity_formula": "purchase_norm*0.5 + spent_norm*0.3 + review_norm*0.2",
            "affinity_range": [0.0, 1.0],
            "notes": "Normalizado por cliente via min-max. Usar para recomendação cross-category.",
        },
        "product_cooccurrence": {
            "file": "gold_product_cooccurrence.csv",
            "item_id_col_a": "product_id_a",
            "item_id_col_b": "product_id_b",
            "similarity_col": "cooccurrence_score",
            "similarity_formula": "Jaccard: |A∩B| / |A∪B|",
            "use_case": "Regra de associação 'quem comprou A também comprou B' sem modelo treinado.",
        },
        "cold_start": {
            "file": "gold_top_products_by_segment.csv",
            "segment_types": ["global", "state", "category"],
            "use_case": "Retornar recomendações para usuários sem histórico. Filtrar por segment_type e segment_value.",
            "example_query": "segment_type=='state' & segment_value=='SP' — top 10 produtos em SP",
        },
    }

    path = os.path.join(GOLD_DIR, "gold_feature_config.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)
    log.info(f"  ✓ gold_feature_config.json")


# ─── 9. README Gold ───────────────────────────────────────────────────────────

def build_readme() -> None:
    log.info("Construindo README_gold.md...")

    content = """\
# Camada Gold — Sistema de Recomendação Olist

Gerada a partir da camada Silver pelo script `src/gold_pipeline.py`.

## Como re-executar

```bash
python src/gold_pipeline.py
```

---

## Tabelas Disponíveis

| Arquivo | Linhas aprox. | Caso de Uso |
|---|---|---|
| `gold_user_product_interactions.csv` | ~115k | Collaborative filtering (user-item matrix) |
| `gold_product_features.csv` | ~32k | Content-based filtering |
| `gold_customer_profile.csv` | ~100k | Perfil / segmentação de clientes |
| `gold_category_affinity.csv` | ~500k | Recomendação cross-category |
| `gold_product_cooccurrence.csv` | variável | Regras de associação / "também comprou" |
| `gold_top_products_by_segment.csv` | ~3k | Cold start / fallback sem histórico |

---

## Estratégias de Recomendação Sugeridas

### 1. Collaborative Filtering (usuário com histórico)
- **Input**: `gold_user_product_interactions.csv`
- **Coluna target**: `implicit_score` (treino sem avaliação explícita) ou `review_score` (com avaliação)
- **Algoritmos sugeridos**: ALS (Alternating Least Squares), BPR, LightFM

### 2. Content-Based Filtering
- **Input**: `gold_product_features.csv`
- Vetorizar features numéricas + one-hot categorias → similaridade cosseno
- **Uso**: "Produtos similares a X"

### 3. Cold Start (novo usuário)
- **Input**: `gold_top_products_by_segment.csv`
- Filtrar por `segment_type='state'` e `segment_value=<estado do cliente>`
- Fallback: `segment_type='global'`

### 4. "Também Comprou" (sem modelo)
- **Input**: `gold_product_cooccurrence.csv`
- Ordenar por `cooccurrence_score` desc para o produto visualizado
- Pronto para uso imediato no frontend

### 5. Cross-Category
- **Input**: `gold_category_affinity.csv`
- Buscar categorias com `affinity_score` alto para o cliente → recomendar top produtos dessas categorias

---

## Contrato de Dados

- `gold_schema.json` — tipos, ranges e nullability de todas as colunas
- `gold_feature_config.json` — quais colunas são features, IDs e targets para cada tabela

---

## Limitações Conhecidas

| Limitação | Detalhe |
|---|---|
| Sem gênero | O dataset Olist não expõe nome nem gênero do cliente |
| Reviews opcionais | ~50% dos pedidos não têm avaliação — `review_score` pode ser nulo |
| Janela temporal | Dados de 2016–2018; comportamento sazonal pode estar desatualizado |
| Sem dados de clique | Apenas compras finalizadas; não há dados de browse/clique |

---

## Dependências

```
pandas >= 1.5
numpy >= 1.23
```
"""

    path = os.path.join(GOLD_DIR, "README_gold.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    log.info(f"  ✓ README_gold.md")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    start = datetime.now()
    log.info("=" * 60)
    log.info("  PIPELINE GOLD — Sistema de Recomendação Olist")
    log.info("=" * 60)

    data = load_silver()

    log.info("\n[1/6] Interações Usuário × Produto")
    interactions = build_user_product_interactions(data)

    log.info("\n[2/6] Features de Produto")
    product_features = build_product_features(data)

    log.info("\n[3/6] Perfil de Cliente")
    build_customer_profile(data)

    log.info("\n[4/6] Afinidade Cliente × Categoria")
    build_category_affinity(data)

    log.info("\n[5/6] Co-ocorrência de Produtos")
    build_product_cooccurrence(data)

    log.info("\n[6/6] Top-N por Segmento (Cold Start)")
    build_top_products_by_segment(data, product_features)

    log.info("\n[Meta] Schema, Feature Config e README")
    tables = {
        "gold_user_product_interactions": interactions,
        "gold_product_features": product_features,
    }
    build_schema_json(tables)
    build_feature_config()
    build_readme()

    elapsed = (datetime.now() - start).seconds
    log.info("\n" + "=" * 60)
    log.info(f"  Pipeline concluído em {elapsed}s → data/gold/")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
