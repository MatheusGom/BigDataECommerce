import pandas as pd
import os


def load_silver_data(base_path=None):
    if base_path is None:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        base_path = os.path.join(current_dir, "..", "data", "silver")

    datasets = {}

    if not os.path.exists(base_path):
        raise FileNotFoundError(
            f"A pasta não foi encontrada em: {os.path.abspath(base_path)}"
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


if __name__ == "__main__":
    print("-" * 30)
    print("Iniciando teste do Data Loader...")
    print("-" * 30)

    try:
        dados = load_silver_data()

        if len(dados) > 0:
            print("\nCarregamento concluído com sucesso\n")
            print(f"Total de tabelas carregadas: {len(dados)}")
            print(f"Tabelas encontradas: {list(dados.keys())}")

            exemplo = list(dados.keys())[0]
            print(f"\nExemplo de colunas em '{exemplo}':")
            print(list(dados[exemplo].columns))
        else:
            print("\nNenhum csv encontrado")

    except Exception as e:
        print(f"\nerro ao carregar os dados: \n{e}")
