import os
import logging

import joblib
import numpy as np
import pandas as pd
import xgboost as xgb
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Any, Dict, Optional, List

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

app = FastAPI(title="Motor de Recomendação - BigDataECommerce")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MODELS_DIR = os.path.join(BASE_DIR, "models")


def _load_assets() -> dict:
    log.info("Carregando assets de recomendação de: %s", MODELS_DIR)
    assets = joblib.load(os.path.join(MODELS_DIR, "recomendacao_assets.pkl"))
    if not isinstance(assets, dict):
        raise TypeError(f"assets deve ser um dicionário, recebido: {type(assets)}")
    log.info("Assets carregados. Chaves disponíveis: %s", list(assets.keys()))
    return assets


try:
    modelo_als = joblib.load(os.path.join(MODELS_DIR, "modelo_als.pkl"))

    modelo_xgb = xgb.XGBClassifier()
    modelo_xgb.load_model(os.path.join(MODELS_DIR, "modelo_xgb.json"))

    assets = _load_assets()

    df_interacoes = None
    df_produtos: Optional[pd.DataFrame] = assets.get("product_feat")
    df_top_segmentos: Optional[pd.DataFrame] = assets.get("top_products")
    user_mapper: Optional[dict] = assets.get("cliente_idx")
    product_mapper: Optional[dict] = assets.get("idx_produto_cf")
    XGB_FEATURES: Optional[List[str]] = assets.get("FEATURES")

    DATA_DIR = os.path.join(BASE_DIR, "data")
    df_customer_profile: Optional[pd.DataFrame] = pd.read_csv(
        os.path.join(DATA_DIR, "gold", "gold_customer_profile.csv")
    )
    df_payments: Optional[pd.DataFrame] = pd.read_csv(
        os.path.join(DATA_DIR, "olist_order_payments_dataset.csv")
    )

    log.info("Modelos e dados de dashboard carregados com sucesso.")
except Exception as exc:
    log.exception("Falha crítica ao carregar modelos: %s", exc)
    raise


class RecommendationRequest(BaseModel):
    customer_unique_id: str
    product_id: Optional[str] = None
    categoria: Optional[str] = None
    preco: Optional[float] = None
    estado: Optional[str] = None
    nota_produto: Optional[float] = None


class ProductDetail(BaseModel):
    product_id: str
    product_category_name: Optional[str] = None
    product_category_name_english: Optional[str] = None
    avg_price: Optional[float] = None
    avg_freight_value: Optional[float] = None
    avg_review_score: Optional[float] = None
    total_orders: Optional[int] = None
    total_reviews: Optional[int] = None
    positive_sentiment_ratio: Optional[float] = None
    seller_state: Optional[str] = None
    total_sellers: Optional[int] = None


def _top_products_from(
    df: pd.DataFrame, col_id: str, col_score: Optional[str], n: int = 10
) -> List[str]:
    if col_score:
        return df.sort_values(by=col_score, ascending=False)[col_id].head(n).tolist()
    return df[col_id].head(n).tolist()


def executar_cold_start(estado: Optional[str], categoria: Optional[str]) -> List[str]:
    if df_top_segmentos is not None and not df_top_segmentos.empty:
        log.info("Cold start via top_products.")

        col_id = (
            "product_id"
            if "product_id" in df_top_segmentos.columns
            else df_top_segmentos.columns[0]
        )
        col_score = next(
            (
                c
                for c in ["total_orders", "total_vendas", "vendas", "count"]
                if c in df_top_segmentos.columns
            ),
            None,
        )
        col_estado = next(
            (
                c
                for c in ["customer_state", "segment_value"]
                if c in df_top_segmentos.columns
            ),
            None,
        )
        col_categoria = next(
            (
                c
                for c in [
                    "product_category_name_english",
                    "product_category_name",
                    "categoria",
                ]
                if c in df_top_segmentos.columns
            ),
            None,
        )

        if estado and col_estado:
            df_filtrado = df_top_segmentos[df_top_segmentos[col_estado] == estado]
            if not df_filtrado.empty:
                return _top_products_from(df_filtrado, col_id, col_score)

        if categoria and col_categoria:
            df_filtrado = df_top_segmentos[df_top_segmentos[col_categoria] == categoria]
            if not df_filtrado.empty:
                return _top_products_from(df_filtrado, col_id, col_score)

        return _top_products_from(df_top_segmentos, col_id, col_score)

    if df_produtos is not None and not df_produtos.empty:
        log.info("Cold start via product_feat (fallback).")

        col_id = (
            "product_id"
            if "product_id" in df_produtos.columns
            else df_produtos.columns[0]
        )
        col_score = next(
            (
                c
                for c in ["avg_review_score", "review_score", "total_orders", "price"]
                if c in df_produtos.columns
            ),
            None,
        )
        col_categoria = next(
            (
                c
                for c in ["product_category_name", "categoria"]
                if c in df_produtos.columns
            ),
            None,
        )

        if categoria and col_categoria:
            df_filtrado = df_produtos[df_produtos[col_categoria] == categoria]
            if not df_filtrado.empty:
                return _top_products_from(df_filtrado, col_id, col_score)

        return _top_products_from(df_produtos, col_id, col_score)

    log.warning(
        "Cold start sem dados validos. Retornando produtos de fallback estatico."
    )
    return ["prod_fallback_1", "prod_fallback_2", "prod_fallback_3"]


def _encode_features(X: pd.DataFrame) -> pd.DataFrame:
    if "le_state" in assets and "estado_cliente" in X.columns:
        classes = assets["le_state"].classes_
        X["estado_cliente"] = X["estado_cliente"].apply(
            lambda v: assets["le_state"].transform([v if v in classes else "SP"])[0]
        )

    if "le_cat" in assets and "categoria_contexto" in X.columns:
        le = assets["le_cat"]
        X["categoria_contexto"] = X["categoria_contexto"].apply(
            lambda v: le.transform([v if v in le.classes_ else le.classes_[0]])[0]
        )

    return X


# Recommendation endpoints
@app.post("/recomendar")
def recomendar(data: RecommendationRequest):
    try:
        customer_id = data.customer_unique_id

        if user_mapper is None or customer_id not in user_mapper:
            log.info("Cliente %s sem histórico. Aplicando cold start.", customer_id)
            return {
                "products": executar_cold_start(data.estado, data.categoria),
                "strategy": "cold_start",
            }

        log.info(
            "Cliente %s identificado. Aplicando recomendação híbrida.", customer_id
        )

        user_index = user_mapper[customer_id]
        candidatos_indexes, _ = modelo_als.recommend(
            userid=user_index,
            user_items=df_interacoes,
            N=50,
            filter_already_liked_items=True,
        )

        candidatos_ids = [
            product_mapper[idx]
            for idx in candidatos_indexes
            if idx < len(product_mapper)
        ]

        if not candidatos_ids:
            return {
                "products": executar_cold_start(data.estado, data.categoria),
                "strategy": "fallback_hybrid",
            }

        linhas = []
        for p_id in candidatos_ids:
            feats = df_produtos[df_produtos["product_id"] == p_id].to_dict(
                orient="records"
            )
            feats = feats[0] if feats else {}
            linhas.append(
                {
                    "product_id": p_id,
                    "categoria_contexto": data.categoria
                    or feats.get("product_category_name"),
                    "preco_contexto": data.preco or feats.get("price"),
                    "estado_cliente": data.estado,
                    "nota_media_produto": feats.get("review_score", 4.0),
                }
            )

        df_input = pd.DataFrame(linhas)

        if df_input.empty:
            return {
                "products": executar_cold_start(data.estado, data.categoria),
                "strategy": "fallback_empty",
            }

        X = _encode_features(df_input.copy())

        if XGB_FEATURES:
            X = X[[col for col in XGB_FEATURES if col in X.columns]]

        df_input["score"] = modelo_xgb.predict_proba(X)[:, 1]
        top_ids = (
            df_input.sort_values("score", ascending=False)["product_id"]
            .head(10)
            .tolist()
        )

        return {"products": top_ids, "strategy": "xgboost_hybrid"}

    except Exception as exc:
        log.exception("Erro ao calcular recomendação: %s", exc)
        raise HTTPException(
            status_code=500, detail="Erro interno no motor de recomendação."
        )


@app.get("/produto/{product_id}", response_model=ProductDetail)
def get_produto(product_id: str):
    if df_produtos is None or df_produtos.empty:
        raise HTTPException(
            status_code=503, detail="Tabela de produtos não disponível."
        )

    resultado = df_produtos[df_produtos["product_id"] == product_id]

    if resultado.empty:
        raise HTTPException(
            status_code=404, detail=f"Produto '{product_id}' não encontrado."
        )

    row: Dict[str, Any] = (
        resultado.iloc[0].where(pd.notna(resultado.iloc[0]), other=None).to_dict()
    )
    return ProductDetail(
        product_id=product_id, **{k: v for k, v in row.items() if k != "product_id"}
    )


# Dashboard endpoints
@app.get("/dashboard/preco-avaliacao")
def dashboard_preco_avaliacao():
    if df_produtos is None or df_produtos.empty:
        raise HTTPException(
            status_code=503, detail="Dados de produtos não disponíveis."
        )

    df = df_produtos[["avg_price", "avg_review_score"]].dropna()

    bins = [0, 50, 100, 200, 500, 1000, float("inf")]
    labels = ["0-50", "50-100", "100-200", "200-500", "500-1000", "1000+"]
    df = df.copy()
    df["faixa_preco"] = pd.cut(df["avg_price"], bins=bins, labels=labels, right=False)

    agrupado = (
        df.groupby("faixa_preco", observed=True)
        .agg(
            total_produtos=("avg_price", "count"),
            avaliacao_media=("avg_review_score", "mean"),
        )
        .reset_index()
    )

    return {
        "faixas": agrupado["faixa_preco"].astype(str).tolist(),
        "total_produtos": agrupado["total_produtos"].tolist(),
        "avaliacao_media": agrupado["avaliacao_media"].round(2).tolist(),
    }


@app.get("/dashboard/top-categorias")
def dashboard_top_categorias():
    if df_produtos is None or df_produtos.empty:
        raise HTTPException(
            status_code=503, detail="Dados de produtos não disponíveis."
        )

    col_cat = (
        "product_category_name_english"
        if "product_category_name_english" in df_produtos.columns
        else "product_category_name"
    )

    agrupado = (
        df_produtos.groupby(col_cat)
        .agg(
            total_pedidos=("total_orders", "sum"),
            avaliacao_media=("avg_review_score", "mean"),
        )
        .reset_index()
        .sort_values("total_pedidos", ascending=False)
        .dropna(subset=[col_cat])
    )

    top10 = agrupado.head(10)

    return {
        "categorias": top10[col_cat].tolist(),
        "total_pedidos": top10["total_pedidos"].astype(int).tolist(),
        "avaliacao_media": top10["avaliacao_media"].round(2).tolist(),
    }


@app.get("/dashboard/distribuicao-geografica")
def dashboard_distribuicao_geografica():
    if df_customer_profile is None or df_customer_profile.empty:
        raise HTTPException(
            status_code=503, detail="Dados de perfil de clientes não disponíveis."
        )

    agrupado = (
        df_customer_profile.groupby("customer_state")
        .agg(
            total_clientes=("customer_unique_id", "count"),
            total_pedidos=("total_orders", "sum"),
            gasto_medio=("total_spent", "mean"),
        )
        .reset_index()
        .sort_values("total_pedidos", ascending=False)
    )

    return {
        "estados": agrupado["customer_state"].tolist(),
        "total_clientes": agrupado["total_clientes"].tolist(),
        "total_pedidos": agrupado["total_pedidos"].astype(int).tolist(),
        "gasto_medio": agrupado["gasto_medio"].round(2).tolist(),
    }


@app.get("/dashboard/metodos-pagamento")
def dashboard_metodos_pagamento():
    if df_payments is None or df_payments.empty:
        raise HTTPException(
            status_code=503, detail="Dados de pagamentos não disponíveis."
        )

    agrupado = (
        df_payments[df_payments["payment_type"] != "not_defined"]
        .groupby("payment_type")
        .agg(
            total_transacoes=("payment_type", "count"),
            valor_total=("payment_value", "sum"),
        )
        .reset_index()
        .sort_values("total_transacoes", ascending=False)
    )

    label_map = {
        "credit_card": "Cartão de Crédito",
        "boleto": "Boleto",
        "voucher": "Voucher",
        "debit_card": "Cartão de Débito",
    }
    agrupado["label"] = (
        agrupado["payment_type"].map(label_map).fillna(agrupado["payment_type"])
    )

    return {
        "metodos": agrupado["label"].tolist(),
        "total_transacoes": agrupado["total_transacoes"].tolist(),
        "valor_total": agrupado["valor_total"].round(2).tolist(),
    }
