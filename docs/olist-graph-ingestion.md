# Olist E-Commerce — Pipeline de Ingestão para Grafo Neo4j

> Documentação técnica do processo de transformação do dataset relacional Olist em um grafo de propriedades no Neo4j, incluindo modelagem, decisões de design e instruções de execução.

---

## Índice

1. [Visão Geral](#1-visão-geral)
2. [Estrutura do Projeto](#2-estrutura-do-projeto)
3. [Pré-requisitos](#3-pré-requisitos)
4. [Infraestrutura com Docker](#4-infraestrutura-com-docker)
5. [Modelo de Dados Original (Relacional)](#5-modelo-de-dados-original-relacional)
6. [Modelo de Dados Transformado (Grafo)](#6-modelo-de-dados-transformado-grafo)
7. [Pipeline de Ingestão — Detalhamento](#7-pipeline-de-ingestão--detalhamento)
   - [7.1 Configuração e Conexão](#71-configuração-e-conexão)
   - [7.2 Constraints e Índices](#72-constraints-e-índices)
   - [7.3 Fase 1 — Carga de Nós](#73-fase-1--carga-de-nós)
   - [7.4 Fase 2 — Carga de Relacionamentos](#74-fase-2--carga-de-relacionamentos)
8. [Decisões Técnicas e Tratamento de Dados](#8-decisões-técnicas-e-tratamento-de-dados)
9. [Execução](#9-execução)
10. [Verificação do Grafo](#10-verificação-do-grafo)
11. [Exemplos de Consultas Cypher](#11-exemplos-de-consultas-cypher)

---

## 1. Visão Geral

O [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) é um conjunto de dados público com aproximadamente **100 mil pedidos** realizados entre 2016 e 2018 em múltiplos marketplaces do Brasil.

Originalmente distribuído em **9 arquivos CSV separados** com relacionamentos implícitos por chaves estrangeiras, o dataset foi transformado em um **único grafo de propriedades** no Neo4j, onde:

- Cada entidade (cliente, pedido, produto, etc.) vira um **nó** com seus atributos
- Cada relacionamento entre entidades (cliente fez pedido, pedido contém item, etc.) vira uma **aresta** tipada e direcional

Essa representação elimina a necessidade de JOINs e permite navegar as conexões de forma direta e eficiente.

---

## 2. Estrutura do Projeto

```
projeto/
├── docker-compose.yml         # Infraestrutura Neo4j
├── script_ingestão.py         # Pipeline principal de ingestão
├── olist-graph-ingestion.md   # Este documento
└── data/
    ├── olist_customers_dataset.csv
    ├── olist_orders_dataset.csv
    ├── olist_order_items_dataset.csv
    ├── olist_products_dataset.csv
    ├── olist_sellers_dataset.csv
    ├── olist_order_payments_dataset.csv
    ├── olist_order_reviews_dataset.csv
    ├── olist_geolocation_dataset.csv
    └── product_category_name_translation.csv
```

---

## 3. Pré-requisitos

| Dependência | Versão mínima | Instalação |
|---|---|---|
| Python | 3.8+ | [python.org](https://python.org) |
| Docker Desktop | qualquer | [docker.com](https://docker.com) |
| pandas | — | `pip install pandas` |
| neo4j (driver) | — | `pip install neo4j` |
| tqdm | — | `pip install tqdm` |

Instalação de todas as dependências Python de uma vez:

```bash
pip install pandas neo4j tqdm
```

---

## 4. Infraestrutura com Docker

O Neo4j é provisionado via Docker Compose. O arquivo `docker-compose.yml` sobe um container com:

- **Neo4j 5.18** com plugin APOC habilitado
- Porta `7474` para o Neo4j Browser (interface web)
- Porta `7687` para o protocolo Bolt (usado pelo driver Python)
- Volume `./data` montado em `/var/lib/neo4j/import` para acesso aos CSVs
- Persistência de dados via volumes Docker nomeados

```yaml
version: '3.8'

services:
  neo4j:
    image: neo4j:5.18.0
    container_name: neo4j-olist
    ports:
      - "7474:7474"
      - "7687:7687"
    environment:
      - NEO4J_AUTH=neo4j/olist1234
      - NEO4J_PLUGINS=["apoc"]
      - NEO4J_dbms_security_procedures_unrestricted=apoc.*
      - NEO4J_dbms_memory_heap_initial__size=512m
      - NEO4J_dbms_memory_heap_max__size=2G
      - NEO4J_dbms_memory_pagecache_size=1G
    volumes:
      - ./data:/var/lib/neo4j/import
      - neo4j_data:/data
      - neo4j_logs:/logs
    restart: unless-stopped

volumes:
  neo4j_data:
  neo4j_logs:
```

**Iniciar o ambiente:**

```bash
# Garantir que o Docker Desktop está rodando
open -a Docker          # macOS
# ou abra manualmente o Docker Desktop no Windows/Linux

# Subir o Neo4j
docker compose up -d

# Acompanhar os logs até estar pronto (~30s)
docker compose logs -f
```

---

## 5. Modelo de Dados Original (Relacional)

O dataset original segue o esquema relacional abaixo, onde as tabelas se conectam por chaves estrangeiras:

![Schema original do dataset Olist](schema.png)

Cada seta no diagrama representa uma chave estrangeira compartilhada entre dois arquivos CSV. As conexões centrais passam por `olist_orders_dataset`, que funciona como hub — ligado a clientes, itens, pagamentos e avaliações. `olist_order_items_dataset` por sua vez conecta pedidos a produtos e vendedores. A geolocalização é referenciada tanto por clientes quanto por vendedores via `zip_code_prefix`.

Para realizar qualquer análise que cruzasse entidades, era necessário executar múltiplos JOINs em SQL. No grafo, essa navegação acontece diretamente pelas arestas.

---

## 6. Modelo de Dados Transformado (Grafo)

### Nós (Labels) e seus atributos principais

| Label | Chave Única | Atributos Principais |
|---|---|---|
| `Customer` | `customer_id` | `unique_id`, `city`, `state`, `zip_code` |
| `Order` | `order_id` | `status`, `purchase_timestamp`, `delivered_customer_date` |
| `OrderItem` | `item_id` | `price`, `freight_value`, `shipping_limit` |
| `Product` | `product_id` | `category`, `weight_g`, `photos_qty` |
| `Seller` | `seller_id` | `city`, `state`, `zip_code` |
| `Payment` | `payment_id` | `type`, `installments`, `value` |
| `Review` | `review_id` | `score`, `comment`, `creation_date` |
| `Category` | `name` | `name_en` |
| `Geolocation` | `zip_code` | `lat`, `lng`, `city`, `state` |

### Relacionamentos (Edges)

| Relacionamento | De → Para | Chave de Junção Original |
|---|---|---|
| `PLACED` | `Customer → Order` | `customer_id` |
| `CONTAINS` | `Order → OrderItem` | `order_id` |
| `REFERENCES` | `OrderItem → Product` | `product_id` |
| `FULFILLED_BY` | `OrderItem → Seller` | `seller_id` |
| `PAID_WITH` | `Order → Payment` | `order_id` |
| `HAS_REVIEW` | `Order → Review` | `order_id` |
| `BELONGS_TO` | `Product → Category` | `product_category_name` |
| `LOCATED_IN` | `Customer → Geolocation` | `customer_zip_code_prefix` |
| `LOCATED_IN` | `Seller → Geolocation` | `seller_zip_code_prefix` |

### Representação visual

```
                    ┌──────────┐
                    │ Category │
                    └────▲─────┘
                  BELONGS_TO
                         │
(Geolocation) ◄──── (Customer) ────PLACED────► (Order) ────PAID_WITH────► (Payment)
      │          LOCATED_IN                       │
LOCATED_IN                                   HAS_REVIEW / CONTAINS
      │                                           │              │
  (Seller) ◄──FULFILLED_BY── (OrderItem) ◄────CONTAINS      (Review)
                                  │
                             REFERENCES
                                  │
                             (Product) ────BELONGS_TO────► (Category)
```

---

## 7. Pipeline de Ingestão — Detalhamento

O script `script_ingestão.py` executa a ingestão em **4 etapas sequenciais**: configuração, constraints, carga de nós e carga de relacionamentos.

### 7.1 Configuração e Conexão

A conexão com o Neo4j é feita via variáveis de ambiente, com fallback para valores padrão:

```python
NEO4J_URI      = os.getenv("NEO4J_URI",      "bolt://localhost:7687")
NEO4J_USER     = os.getenv("NEO4J_USER",     "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "olist1234")
DATA_DIR       = os.getenv("DATA_DIR",       "./data")
BATCH_SIZE     = 500
```

Para sobrescrever sem alterar o código:

```bash
NEO4J_PASSWORD=minhasenha DATA_DIR=/outro/caminho python script_ingestão.py
```

Os dados são enviados ao Neo4j em **lotes de 500 registros** por transação (parâmetro `BATCH_SIZE`), o que evita sobrecarga de memória e permite progresso incremental visível via barra `tqdm`.

### 7.2 Constraints e Índices

Antes de qualquer inserção, o script cria constraints de unicidade para todos os labels. Isso serve a dois propósitos:

1. **Corretude:** impede nós duplicados quando o mesmo ID aparece em múltiplos CSVs
2. **Performance:** o Neo4j cria automaticamente um índice B-tree para cada constraint, acelerando os `MERGE` subsequentes

```cypher
CREATE CONSTRAINT IF NOT EXISTS FOR (n:Customer)    REQUIRE n.customer_id IS UNIQUE
CREATE CONSTRAINT IF NOT EXISTS FOR (n:Order)       REQUIRE n.order_id    IS UNIQUE
CREATE CONSTRAINT IF NOT EXISTS FOR (n:Product)     REQUIRE n.product_id  IS UNIQUE
CREATE CONSTRAINT IF NOT EXISTS FOR (n:Seller)      REQUIRE n.seller_id   IS UNIQUE
CREATE CONSTRAINT IF NOT EXISTS FOR (n:Category)    REQUIRE n.name        IS UNIQUE
CREATE CONSTRAINT IF NOT EXISTS FOR (n:Review)      REQUIRE n.review_id   IS UNIQUE
CREATE CONSTRAINT IF NOT EXISTS FOR (n:Payment)     REQUIRE n.payment_id  IS UNIQUE
CREATE CONSTRAINT IF NOT EXISTS FOR (n:Geolocation) REQUIRE n.zip_code    IS UNIQUE
```

### 7.3 Fase 1 — Carga de Nós

Todos os nós são criados antes de qualquer relacionamento. Isso é intencional: os `MATCH` usados na Fase 2 só funcionam se os nós já existirem.

Cada função de carga segue o mesmo padrão:

```python
df = clean(pd.read_csv(csv("arquivo.csv")))   # lê e limpa NaN
rows = df.to_dict("records")                   # converte para lista de dicts
run_batched(session, query, rows, "Label")     # envia em lotes
```

A função `clean()` substitui valores `NaN` do pandas por `None`, que é o tipo nulo aceito pelo driver Neo4j. Sem isso, o driver lançaria erros de serialização.

Cada nó é inserido com `MERGE` (não `CREATE`), garantindo **idempotência** — o script pode ser reexecutado sem duplicar dados.

**Ordem de carga dos nós:**

```
1. Customer       — olist_customers_dataset.csv
2. Geolocation    — olist_geolocation_dataset.csv   (deduplicado por CEP)
3. Category       — product_category_name_translation.csv
4. Product        — olist_products_dataset.csv
5. Seller         — olist_sellers_dataset.csv
6. Order          — olist_orders_dataset.csv
7. Payment        — olist_order_payments_dataset.csv
8. Review         — olist_order_reviews_dataset.csv (deduplicado por review_id)
```

### 7.4 Fase 2 — Carga de Relacionamentos

Com todos os nós presentes, os relacionamentos são criados usando `MATCH` nos dois extremos e `MERGE` na aresta:

```cypher
UNWIND $rows AS r
MATCH (c:Customer {customer_id: r.customer_id})
MATCH (o:Order    {order_id:    r.order_id})
MERGE (c)-[:PLACED]->(o)
```

O uso de `MERGE` na aresta também garante idempotência: reexecutar o script não cria arestas duplicadas.

**Ordem de carga dos relacionamentos:**

```
1. (Customer)-[:PLACED]->(Order)
2. (Order)-[:CONTAINS]->(OrderItem)-[:REFERENCES]->(Product)
                                   -[:FULFILLED_BY]->(Seller)
3. (Order)-[:PAID_WITH]->(Payment)
4. (Order)-[:HAS_REVIEW]->(Review)
5. (Product)-[:BELONGS_TO]->(Category)
6. (Customer)-[:LOCATED_IN]->(Geolocation)
   (Seller)-[:LOCATED_IN]->(Geolocation)
```

---

## 8. Decisões Técnicas e Tratamento de Dados

### Deduplicação de Geolocation

O arquivo `olist_geolocation_dataset.csv` contém aproximadamente **1 milhão de linhas**, mas muitas são coordenadas repetidas para o mesmo CEP. O script mantém apenas **uma entrada por `zip_code_prefix`** via `drop_duplicates`, reduzindo o volume para ~19 mil nós únicos e acelerando significativamente a ingestão.

### Deduplicação de Reviews

O dataset original de reviews contém `review_id`s duplicados — um problema conhecido no dataset público. O script aplica `drop_duplicates(subset=["review_id"])` antes da inserção para garantir a unicidade exigida pela constraint.

### Chave sintética de Payment

O dataset de pagamentos não possui um identificador único por registro. A chave `payment_id` é gerada sinteticamente como:

```python
df["payment_id"] = df["order_id"] + "_" + df["payment_sequential"].astype(str)
# Exemplo: "abc123_1", "abc123_2"
```

### Chave sintética de OrderItem

Mesma lógica aplicada para `OrderItem`:

```python
df["item_id"] = df["order_id"] + "_" + df["order_item_id"].astype(str)
# Exemplo: "abc123_1", "abc123_2"
```

### Carga em lotes (batching)

Enviar todos os registros em uma única transação pode causar timeout ou estouro de heap no Neo4j. O padrão adotado é de **500 registros por transação**, controlado pelo parâmetro `BATCH_SIZE`. Para máquinas com mais memória disponível, esse valor pode ser aumentado para até 2000 sem problemas.

---

## 9. Execução

```bash
# 1. Inicie o Docker Desktop e suba o Neo4j
docker compose up -d

# 2. Aguarde o container ficar healthy (~30 segundos)
docker compose ps

# 3. Execute o pipeline de ingestão
python script_ingestão.py
```

**Saída esperada no terminal:**

```
10:00:01 [INFO] Conectando em bolt://localhost:7687 como 'neo4j'...
10:00:01 [INFO] Criando constraints e índices...
10:00:01 [INFO] Constraints criadas.
10:00:01 [INFO] -- FASE 1: Nos -------------------------------------------
10:00:02 [INFO] Carregando Customer...
Customers: 100%|████████████████| 200/200 [00:18<00:00]
10:00:20 [INFO] 99441 customers.
...
10:12:45 [INFO] Concluido em 764.2s
10:12:45 [INFO] -- RESUMO DO GRAFO --------------------------------------
10:12:45 [INFO]    Geolocation     →  19,015 nós
10:12:45 [INFO]    Customer        →  99,441 nós
10:12:45 [INFO]    Order           →  99,441 nós
10:12:45 [INFO]    OrderItem       → 112,650 nós
10:12:45 [INFO]    Product         →  32,951 nós
10:12:45 [INFO]    Review          →  98,371 nós
10:12:45 [INFO]    Payment         → 103,886 nós
10:12:45 [INFO]    Seller          →   3,095 nós
10:12:45 [INFO]    Category        →      71 nós
10:12:45 [INFO]    PLACED          →  99,441 rels
10:12:45 [INFO]    CONTAINS        → 112,650 rels
10:12:45 [INFO]    REFERENCES      → 112,650 rels
10:12:45 [INFO]    FULFILLED_BY    → 112,650 rels
10:12:45 [INFO]    PAID_WITH       → 103,886 rels
10:12:45 [INFO]    HAS_REVIEW      →  98,371 rels
10:12:45 [INFO]    BELONGS_TO      →  32,341 rels
10:12:45 [INFO]    LOCATED_IN      → 102,536 rels
```

---

## 10. Verificação do Grafo

Acesse o Neo4j Browser em `http://localhost:7474` com `neo4j` / `olist1234`.

**Contagem geral de nós e relacionamentos:**

```cypher
MATCH (n) RETURN labels(n)[0] AS label, count(n) AS total ORDER BY total DESC;

MATCH ()-[r]->() RETURN type(r) AS rel, count(r) AS total ORDER BY total DESC;
```

**Visualizar uma subgraph de exemplo:**

```cypher
MATCH path = (c:Customer)-[:PLACED]->(o:Order)-[:CONTAINS]->(oi:OrderItem)-[:REFERENCES]->(p:Product)
RETURN path LIMIT 25
```

---

## 11. Exemplos de Consultas Cypher

**Produtos comprados por um cliente com nota da avaliação:**

```cypher
MATCH (c:Customer {unique_id: "abc123"})-[:PLACED]->(o:Order),
      (o)-[:CONTAINS]->(oi:OrderItem)-[:REFERENCES]->(p:Product),
      (o)-[:HAS_REVIEW]->(r:Review)
RETURN p.product_id, p.category, oi.price, r.score
ORDER BY r.score DESC
```

**Top 10 vendedores por receita:**

```cypher
MATCH (oi:OrderItem)-[:FULFILLED_BY]->(s:Seller)
RETURN s.seller_id, s.state, sum(oi.price) AS receita_total
ORDER BY receita_total DESC
LIMIT 10
```

**Categorias com melhor avaliação média:**

```cypher
MATCH (p:Product)-[:BELONGS_TO]->(cat:Category),
      (oi:OrderItem)-[:REFERENCES]->(p),
      (o:Order)-[:CONTAINS]->(oi),
      (o)-[:HAS_REVIEW]->(r:Review)
RETURN cat.name_en AS categoria, round(avg(r.score), 2) AS nota_media, count(r) AS total_avaliacoes
ORDER BY nota_media DESC
LIMIT 15
```

**Clientes que compraram produtos de múltiplos estados:**

```cypher
MATCH (c:Customer)-[:PLACED]->(o:Order)-[:CONTAINS]->(oi:OrderItem)-[:FULFILLED_BY]->(s:Seller)
WITH c, collect(DISTINCT s.state) AS estados
WHERE size(estados) > 2
RETURN c.customer_id, c.state AS estado_cliente, estados
LIMIT 20
```

**Pedidos com frete mais caro que o produto:**

```cypher
MATCH (oi:OrderItem)
WHERE oi.freight_value > oi.price
RETURN count(oi) AS pedidos_frete_caro,
       round(avg(oi.freight_value - oi.price), 2) AS diferenca_media
```

---

*Dataset: [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — licença CC BY-NC-SA 4.0*