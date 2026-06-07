from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Optional, List
import joblib
import pandas as pd
import numpy as np
import os

app = FastAPI(title="Motor de Recomendação - BigDataECommerce")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))  # pasta 'src'
BASE_DIR = os.path.dirname(SCRIPT_DIR)  # pasta raiz do projeto
MODELS_DIR = os.path.join(BASE_DIR, "models")  # pasta 'models'

print(f"📂 Diretório da API: {SCRIPT_DIR}")
print(f"📂 Procurando modelos em: {MODELS_DIR}")

try:
    print("Carregando modelos e assets de recomendação...")

    modelo_als = joblib.load(os.path.join(MODELS_DIR, "modelo_als.pkl"))

    assets = joblib.load(os.path.join(MODELS_DIR, "recomendacao_assets.pkl"))

    # 🎯 COLE ESTE BLOCO AQUI PARA DETECTAR AS CHAVES REAIS:
    print("\n📦 --- CHAVES ENCONTRADAS NO SEU ACERVO DE ASSETS ---")
    if isinstance(assets, dict):
        print("Chaves disponíveis:", list(assets.keys()))
        for chave, valor in assets.items():
            print(f"  - Tipo do objeto na chave '{chave}': {type(valor)}")
    else:
        print(
            f"⚠️ O arquivo assets não é um dicionário. É um objeto do tipo: {type(assets)}"
        )
    print("----------------------------------------------------\n")

    import xgboost as xgb

    modelo_xgb = xgb.XGBClassifier()
    modelo_xgb.load_model(os.path.join(MODELS_DIR, "modelo_xgb.json"))

    df_interacoes = None  # Se a sua matriz CSR for separada. Se for a tabela 'cp', mude para: assets.get("cp")
    df_produtos = assets.get(
        "product_feat"
    )  # DataFrame de características dos produtos
    df_top_segmentos = assets.get("top_products")  # DataFrame para o Cold Start

    user_mapper = assets.get(
        "cliente_idx"
    )  # Mapeia o customer_unique_id -> ID numérico
    product_mapper = assets.get(
        "idx_produto_cf"
    )  # Mapeia índice numérico -> ID String do Produto

    # Lista com a ordem exata das colunas que o seu XGBoost foi treinado
    XGB_FEATURES = assets.get("FEATURES")

    print("✅ Todos os modelos e chaves reais foram carregados com sucesso!")

    print("\n🔍 --- INSPEÇÃO DE COLUNAS DAS TABELAS GOLD ---")
    if df_produtos is not None:
        print("Colunas df_produtos:", df_produtos.columns.tolist())
    if df_top_segmentos is not None:
        print("Colunas df_top_segmentos:", df_top_segmentos.columns.tolist())
    print("----------------------------------------------\n")

    print("Todos os modelos foram carregados com sucesso!")
except Exception as e:
    print(f"Erro crítico ao carregar os modelos: {str(e)}")
    raise e


class RecommendationRequest(BaseModel):
    customer_unique_id: str
    product_id: Optional[str] = None
    categoria: Optional[str] = None
    preco: Optional[float] = None
    estado: Optional[str] = None
    nota_produto: Optional[float] = None


def executar_cold_start(estado: Optional[str], categoria: Optional[str]) -> List[str]:
    # --- PLANO DE BACKUP MÁXIMO: Se as duas tabelas principais falharem ---
    def fallback_estatico():
        print(
            "🚨 [Cold Start] Falha crítica: Nenhuma tabela Gold possuía dados válidos."
        )
        return ["prod_fallback_1", "prod_fallback_2", "prod_fallback_3"]

    # --- TENTATIVA 1: USANDO A TABELA DE TOP_PRODUCTS (Caso ela tenha linhas) ---
    if df_top_segmentos is not None and not df_top_segmentos.empty:
        print(
            f"🔎 [Cold Start] Buscando em top_products. Colunas: {df_top_segmentos.columns.tolist()}"
        )
        col_id = (
            "product_id"
            if "product_id" in df_top_segmentos.columns
            else df_top_segmentos.columns[0]
        )

        col_vendas = None
        for col in ["total_vendas", "vendas", "count", "quantity", "vendas_qtd"]:
            if col in df_top_segmentos.columns:
                col_vendas = col
                break

        col_estado = (
            "customer_state"
            if "customer_state" in df_top_segmentos.columns
            else ("estado" if "estado" in df_top_segmentos.columns else None)
        )
        col_categoria = (
            "product_category_name"
            if "product_category_name" in df_top_segmentos.columns
            else ("categoria" if "categoria" in df_top_segmentos.columns else None)
        )

        if estado and col_estado and estado in df_top_segmentos[col_estado].values:
            df_filtrado = df_top_segmentos[df_top_segmentos[col_estado] == estado]
            if not df_filtrado.empty:
                return (
                    df_filtrado.sort_values(by=col_vendas, ascending=False)[col_id]
                    .head(10)
                    .tolist()
                    if col_vendas
                    else df_filtrado[col_id].head(10).tolist()
                )

        if (
            categoria
            and col_categoria
            and categoria in df_top_segmentos[col_categoria].values
        ):
            df_filtrado = df_top_segmentos[df_top_segmentos[col_categoria] == categoria]
            if not df_filtrado.empty:
                return (
                    df_filtrado.sort_values(by=col_vendas, ascending=False)[col_id]
                    .head(10)
                    .tolist()
                    if col_vendas
                    else df_filtrado[col_id].head(10).tolist()
                )

        return (
            df_top_segmentos.sort_values(by=col_vendas, ascending=False)[col_id]
            .head(10)
            .tolist()
            if col_vendas
            else df_top_segmentos[col_id].head(10).tolist()
        )

    # --- TENTATIVA 2: USANDO A TABELA PRODUCT_FEAT (Seu backup com dados reais) ---
    print(
        "🔄 [Cold Start] 'top_products' estava vazia. Recorrendo à tabela 'product_feat'..."
    )
    if df_produtos is not None and not df_produtos.empty:
        print(
            f"🔎 [Cold Start] Colunas disponíveis em product_feat: {df_produtos.columns.tolist()}"
        )

        # Identifica a coluna de ID do produto na product_feat
        col_id_prod = (
            "product_id"
            if "product_id" in df_produtos.columns
            else df_produtos.columns[0]
        )

        # Identifica se há alguma coluna de score/nota/vendas para ordenar os melhores
        col_ordenacao = None
        for col in ["review_score", "vendas", "total_vendas", "price"]:
            if col in df_produtos.columns:
                col_ordenacao = col
                break

        # Identifica a coluna de categoria na product_feat
        col_cat_prod = (
            "product_category_name"
            if "product_category_name" in df_produtos.columns
            else ("categoria" if "categoria" in df_produtos.columns else None)
        )

        # Se o usuário escolheu uma categoria no front, filtra os produtos reais dessa categoria
        if categoria and col_cat_prod and col_cat_prod in df_produtos.columns:
            df_filtrado = df_produtos[df_produtos[col_cat_prod] == categoria]
            if not df_filtrado.empty:
                print(
                    f"✅ Cold Start gerado com sucesso extraindo produtos da categoria: {categoria}"
                )
                if col_ordenacao:
                    return (
                        df_filtrado.sort_values(by=col_ordenacao, ascending=False)[
                            col_id_prod
                        ]
                        .head(10)
                        .tolist()
                    )
                return df_filtrado[col_id_prod].head(10).tolist()

        # Se não houver filtro ou a categoria não bater, pega os 10 melhores (ou os 10 primeiros) gerais da base
        print(
            "🎯 [Cold Start] Retornando os 10 principais produtos reais cadastrados na base de dados."
        )
        if col_ordenacao:
            return (
                df_produtos.sort_values(by=col_ordenacao, ascending=False)[col_id_prod]
                .head(10)
                .tolist()
            )
        return df_produtos[col_id_prod].head(10).tolist()

    return fallback_estatico()


@app.post("/recomendar")
def recomendar(data: RecommendationRequest):
    try:
        customer_id = data.customer_unique_id

        possui_historico = user_mapper is not None and customer_id in user_mapper

        if not possui_historico:
            print(
                f"Cliente {customer_id} é novo ou sem histórico. Aplicando Módulo 4: Cold Start."
            )
            produtos_recomendados = executar_cold_start(data.estado, data.categoria)
            return {"products": produtos_recomendados, "strategy": "cold_start"}

        print(
            f"Cliente {customer_id} identificado. Aplicando Módulo 6: XGBoost Híbrido."
        )

        user_index = user_mapper[customer_id]

        ids_candidatos_indexes, _ = modelo_als.recommend(
            userid=user_index,
            user_items=df_interacoes,
            N=50,
            filter_already_liked_items=True,
        )

        candidatos_ids = [
            product_mapper[idx]
            for idx in ids_candidatos_indexes
            if idx < len(product_mapper)
        ]

        if not candidatos_ids:
            return {
                "products": executar_cold_start(data.estado, data.categoria),
                "strategy": "fallback_hybrid",
            }

        recursos_previsao = []

        for p_id in candidatos_ids:
            features_produto = df_produtos[df_produtos["product_id"] == p_id].to_dict(
                orient="records"
            )
            features_produto = features_produto[0] if features_produto else {}

            linha = {
                "product_id": p_id,
                "categoria_contexto": data.categoria
                or features_produto.get("product_category_name"),
                "preco_contexto": data.preco or features_produto.get("price"),
                "estado_cliente": data.estado,
                "nota_media_produto": features_produto.get("review_score", 4.0),
            }
            recursos_previsao.append(linha)

        df_xgb_input = pd.DataFrame(recursos_previsao)

        # Garante que temos dados para prever
        if df_xgb_input.empty:
            return {
                "products": executar_cold_start(data.estado, data.categoria),
                "strategy": "fallback_empty",
            }

        # Criamos o X_features para alimentar a IA
        X_features = df_xgb_input.copy()

        # 🎯 PARTE VITAL: Transforma o texto em número usando seus LabelEncoders reais!
        try:
            if "le_state" in assets and "estado_cliente" in X_features.columns:
                # Caso venha um estado novo que o encoder não conheça, usamos SP como fallback seguro
                X_features["estado_cliente"] = X_features["estado_cliente"].apply(
                    lambda x: (
                        assets["le_state"].transform([x])[0]
                        if x in assets["le_state"].classes_
                        else assets["le_state"].transform(["SP"])[0]
                    )
                )

            if "le_cat" in assets and "categoria_contexto" in X_features.columns:
                X_features["categoria_contexto"] = X_features[
                    "categoria_contexto"
                ].apply(
                    lambda x: (
                        assets["le_cat"].transform([x])[0]
                        if x in assets["le_cat"].classes_
                        else assets["le_cat"].transform([assets["le_cat"].classes_[0]])[
                            0
                        ]
                    )
                )
        except Exception as enc_err:
            print(
                f"⚠️ Aviso: Falha ao aplicar LabelEncoders, tentando predict direto: {enc_err}"
            )

        # Reordena as colunas para ficar exatamente no formato que o XGBoost espera
        # Se você salvou a lista em 'FEATURES', usamos ela:
        if XGB_FEATURES:
            # Mantém apenas as colunas que pertencem ao modelo de Machine Learning
            X_features = X_features[
                [col for col in XGB_FEATURES if col in X_features.columns]
            ]

        # O XGBoost faz a mágica acontecer
        probabilidades = modelo_xgb.predict_proba(X_features)[:, 1]

        df_xgb_input["score_ia"] = probabilidades
        df_ordenado = df_xgb_input.sort_values(by="score_ia", ascending=False)

        return {
            "products": df_ordenado["product_id"].head(10).tolist(),
            "strategy": "xgboost_hybrid",
        }

    except Exception as e:
        print(f"Erro durante o cálculo de recomendação: {str(e)}")
        raise HTTPException(
            status_code=500, detail="Erro interno no motor de inteligência artificial."
        )
