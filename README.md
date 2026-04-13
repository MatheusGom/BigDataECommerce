# BigDataECommerce

## Descrição do Projeto
Este projeto implementa um pipeline de dados completo fundamentado na arquitetura Medallion para analisar o ecossistema de e-commerce brasileiro. O objetivo principal é transformar dados relacionais brutos em um banco de dados orientado a grafos (Neo4j), permitindo análises complexas de logística, comportamento do consumidor e conexões entre entidades que seriam custosas em ambientes SQL tradicionais.

## Fonte dos Dados
Os dados provêm do **Brazilian E-Commerce Public Dataset by Olist**, disponível no Kaggle. O conjunto contém aproximadamente 100 mil pedidos realizados entre 2016 e 2018, distribuídos originalmente em 9 arquivos CSV que cobrem clientes, geolocalização, itens, pagamentos, avaliações, produtos e vendedores.

## Ferramentas Aplicadas

### Processamento e Análise (Camadas Bronze e Silver)
* **Python e Pandas:** Limpeza, tipagem de dados e tratamento de valores nulos (NaN).
* **Jupyter Notebook:** Ambiente para exploração estatística e geração de visualizações.
* **Matplotlib/Seaborn:** Identificação de gargalos logísticos e padrões de venda.

### Armazenamento e Modelagem (Camada Gold)
* **Neo4j 5.18:** Banco de dados de grafos para modelagem de relacionamentos.
* **Docker / Docker Compose:** Provisionamento da infraestrutura e plugins (APOC).
* **Cypher:** Linguagem de consulta para análises de rede e inteligência geográfica.

### Gestão de Ambiente
* **Venv / Pip:** Isolamento de dependências Python.
* **Ipykernel:** Registro do kernel para execução do pipeline em notebooks.

---

## Grupo

| Nome | GitHub | E-mail |
| :--- | :--- | :--- |
| **Estela de Lacerda Oliveira** | [EstelaLacerda](https://github.com/EstelaLacerda) | elo@cesar.school |
| **Matheus Gomes de Andrade** | [MatheusGom](https://github.com/MatheusGom) | mga@cesar.school |
| **Maria Luiza Calife de Figueiredo** | [LuizaCalife](https://github.com/LuizaCalife) | mlcdf@cesar.school |
| **Paulo Montenegro Campos** | [paulo-campos-57](https://github.com/paulo-campos-57) | pmc3@cesar.school |
| **Maria Júlia Pessôa Cunha** | [mariajuliapessoa](https://github.com/mariajuliapessoa) | mjpc@cesar.school |
| **Raphael Vasconcelos Morant** | [RvmGit](https://github.com/RvmGit) | rvm@cesar.school |
| **Yara Rodrigues** | [Yara-R](https://github.com/Yara-R) | yri@cesar.school |
