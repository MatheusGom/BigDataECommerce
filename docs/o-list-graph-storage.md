# Olist E-Commerce — Armazenamento, Tratamento e Insights (Neo4j)
Documentação técnica do processo de transformação dos dados da camada Bronze (dados brutos em CSV) para a camada Silver (dados limpos, tratados e padronizados),
utilizando Python e Jupyter Notebook para garantir a integridade da ingestão no Neo4j.

## 1. Visão Geral
O dataset Olist na camada Bronze consiste em 9 arquivos CSV brutos extraídos diretamente da fonte (Kaggle). Estes dados contêm inconsistências típicas de sistemas transacionais, 
como valores nulos (NaN), registros duplicados e tipos de dados incompatíveis para modelagem em grafos.

O processo de transformação para a camada Silver aplica limpeza, padronização e deduplicação, resultando em dados consistentes.
A transformação ocorre através do Jupyter Notebook silver_cleaning.ipynb, que lê a pasta data/ e exporta os resultados para data/silver/, preservando a imutabilidade dos dados originais.

## 2. Arquitetura Medallion
O pipeline adota o padrão de arquitetura de medalhão para organizar o fluxo de dados:

| Camada | Estado do Dado | Responsabilidade | Destino |
| :--- | :---: | :---: | ---: |
| Bronze | Bruto (Raw) | Armazenamento fiel à fonte original. | Pasta /data |
| Silver | Tratado (Cleaned) | Limpeza, tipos de dados e deduplicação | Pasta /data/silver |
| Gold | Analítico (Graph) | Ingestão e modelagem de relacionamentos. | Neo4j Graph DB |

## 3. Transformações da Camada Silver
Cada dataset passa por um processo de refinamento específico para garantir que o script_ingestão.py opere sem erros de serialização ou violação de constraints.

### 3.1 Limpeza e Padronização Principal
- Geolocalização: Redução drástica de 1.071.330 para 19.015 registros (98,2% de redução) ao manter apenas uma coordenada única por prefixo de CEP.
- Avaliações (Reviews): Remoção de duplicatas de review_id, reduzindo o volume de 99.224 para 98.371, garantindo a unicidade exigida pelo Neo4j.
- Produtos: Atribuição do valor 'sem_categoria' para os ~6.500 produtos (20% do total) que não possuíam categoria definida, evitando a perda de nós durante a ingestão.

### 3.2 Criação de Chaves Sintéticas
Para tabelas que não possuem IDs únicos nativos, foram geradas chaves compostas:
- Payment ID: order_id + _ + payment_sequential
- OrderItem ID: order_id + _ + order_item_id

### 3.3 Tipagem de Dados

| Tipo orignal | Transformação Silver | Motivo |
| :--- | :---: | ---: |
| Strings com NaN | Convertido para '' (vazia) ou None | Evita erros de serialização no driver do Neo4j. |
| Timestamps (Object) | Convertido para datetime64 | Permite filtragens temporais eficientes no grafo. |
| Numéricos (Float) | Padronização de decimais e conversão de IDs para Int | Consistência na busca e indexação. |


## 4. Estrutura de Arquivos

projeto/
├── data/                       # Camada BRONZE (Imutável)
│   ├── olist_customers_dataset.csv
│   └── ... (8 arquivos)
└── data/silver/                # Camada SILVER (Gerada via Notebook)
    ├── olist_customers_dataset.csv
    └── ... (arquivos limpos prontos para o Neo4j)

## 5. Decisões Técnicas e Impacto
1. Deduplicação Estratégica: Ao reduzir o dataset de geolocalização em 98%, o tempo de processamento do grafo foi reduzido de horas para minutos, sem perder a capacidade de mapear clientes e vendedores por região.
2. Segurança de Ingestão: O tratamento de NaN em strings garante que as propriedades dos nós no Neo4j sejam criadas corretamente, uma vez que o banco de grafos diferencia uma propriedade ausente de uma string nula.
3. Idempotência: A separação das pastas permite que o processo de limpeza seja reexecutado quantas vezes for necessário sem corromper a fonte da verdade (Bronze).

## 6. Insights e Análises do Grafo
Com os dados integrados no Neo4j, é possível realizar análises que seriam custosas em ambientes relacionais, como a identificação de padrões de compra e correlações geográficas.

### 6.1 Desempenho por Categoria
A análise das categorias permite identificar quais segmentos trazem maior volume vs. maior margem.
- Volume de Vendas: As categorias bed_bath_table (11.104 itens) e health_beauty (9.670 itens) lideram em volume de produtos vendidos.
- Receita Total: Em termos financeiros, health_beauty (R$ 1,25M) e watches_gifts (R$ 1,20M) são as categorias mais rentáveis do ecossistema.
- Ticket Médio: A categoria computers possui o maior ticket médio unitário (R$ 1.098,34), contrastando com categorias de alto volume e baixo valor unitário.

### 6.2 Market Basket Analysis (Associações)
Utilizando a conectividade do grafo, identificamos quais categorias os clientes tendem a comprar juntas em um mesmo pedido ou perfil de consumo.

| Categoria A | Categoria B | Frequência de Associação |
| :--- | :---: | ---: |
| moveis_decoracao | cama_mesa_banho |128 |
| ferramentas_jardim | moveis_decoracao | 60 |
| casa_conforto | cama_mesa_banho | 58 |

Dados baseados em co-ocorrência de categorias via nós de pedidos comuns

### 6.3 Inteligência Geográfica e Logística
A análise por estado revela disparidades no comportamento de compra e nos custos logísticos.

- Liderança de Pedidos: São Paulo (SP) concentra o maior volume (465 pedidos no conjunto analisado), seguido pelo Rio de Janeiro (RJ) e Minas Gerais (MG).
- Maiores Tickets Médios por Estado:
   1. Alagoas (AL): R$ 181,00
   2. Acre (AC): R$ 173,73
   3. Paraíba (PB): R$ 170,59
- Relação Preço vs. Frete: Estados da região Norte e Nordeste, como PB (R$ 42,02) e AC (R$ 40,07), apresentam os maiores fretes médios, o que impacta diretamente na decisão de compra do cliente.

### 6.4 Análise Financeira e de Pagamentos
O grafo conecta pedidos aos seus respectivos métodos de pagamento, permitindo visualizar a preferência financeira do consumidor.

| Método de Pagamento | Receita Total Acumulada|
| :--- | ---: | 
| Cartão de Crédito | R$ 12.100.624,80 |
| Boleto Bancário | R$ 2.767.977,98 | 
| Voucher | R$ 343.013,19 | 
| Cartão de Débito  | R$ 208.066,88 | 

