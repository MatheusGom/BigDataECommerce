import pandas as pd
import os


def _load_layer(base_path: str, layer_name: str) -> dict:
    """Carrega todos os CSVs de um diretório de camada (Silver ou Gold)."""
    datasets = {}

    if not os.path.exists(base_path):
        raise FileNotFoundError(
            f"A pasta da camada {layer_name} não foi encontrada em: {os.path.abspath(base_path)}"
        )

    print(f"Buscando arquivos em: {os.path.abspath(base_path)}")

    for arquivo in os.listdir(base_path):
        if arquivo.endswith(".csv"):
            nome_tabela = arquivo.replace(".csv", "")
            caminho_completo = os.path.join(base_path, arquivo)

            try:
                datasets[nome_tabela] = pd.read_csv(caminho_completo, encoding="utf-8")
            except UnicodeDecodeError:
                datasets[nome_tabela] = pd.read_csv(caminho_completo, encoding="latin1")

            print(f" -> {nome_tabela} carregado. ({len(datasets[nome_tabela])} linhas)")

    return datasets


def load_silver_data(base_path=None) -> dict:
    """
    Carrega todos os CSVs da camada Silver.

    Returns:
        dict: chave = nome do arquivo (sem .csv), valor = DataFrame.
    """
    if base_path is None:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        base_path = os.path.join(current_dir, "..", "data", "silver")

    return _load_layer(base_path, "Silver")


def load_gold_data(base_path=None) -> dict:
    """
    Carrega todos os CSVs da camada Gold (features para o sistema de recomendação).

    Tabelas disponíveis:
      - gold_user_product_interactions : Matriz usuário × produto (collaborative filtering)
      - gold_product_features          : Features de produto (content-based)
      - gold_customer_profile          : Perfil de cliente (segmentação / cold start)
      - gold_category_affinity         : Afinidade cliente × categoria (cross-category)
      - gold_product_cooccurrence      : Produtos comprados juntos (regras de associação)
      - gold_top_products_by_segment   : Top-N por segmento (cold start / fallback)

    Consulte data/gold/gold_schema.json para tipos e ranges de cada coluna,
    e data/gold/gold_feature_config.json para a configuração de features do modelo.

    Returns:
        dict: chave = nome do arquivo (sem .csv), valor = DataFrame.
    """
    if base_path is None:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        base_path = os.path.join(current_dir, "..", "data", "gold")

    return _load_layer(base_path, "Gold")


if __name__ == "__main__":
    print("-" * 30)
    print("Iniciando teste do Data Loader...")
    print("-" * 30)

    for layer, loader in [("Silver", load_silver_data), ("Gold", load_gold_data)]:
        print(f"\n{'=' * 30}")
        print(f"Camada {layer}")
        print("=" * 30)
        try:
            dados = loader()
            if dados:
                print(f"\nCarregamento concluído com sucesso")
                print(f"Total de tabelas: {len(dados)}")
                print(f"Tabelas: {list(dados.keys())}")
                exemplo = list(dados.keys())[0]
                print(f"\nColunas de '{exemplo}': {list(dados[exemplo].columns)}")
            else:
                print("Nenhum CSV encontrado.")
        except FileNotFoundError as e:
            print(f"Aviso: {e}")
