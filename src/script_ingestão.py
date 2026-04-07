import os
import time
import logging
import pandas as pd
from tqdm import tqdm
from neo4j import GraphDatabase

# ─────────────────────────────────────────────
# Configuração
# ─────────────────────────────────────────────
NEO4J_URI      = os.getenv("NEO4J_URI",      "bolt://localhost:7687")
NEO4J_USER     = os.getenv("NEO4J_USER",     "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "olist1234")
DATA_DIR       = os.getenv("DATA_DIR",       "./data")
BATCH_SIZE     = 500   # registros por transação

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────
def csv(filename: str) -> str:
    return os.path.join(DATA_DIR, filename)


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Substitui NaN por None para o driver Neo4j aceitar."""
    return df.where(pd.notna(df), other=None)


def batches(lst: list, size: int):
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


def run_batched(session, query: str, rows: list, label: str):
    total = len(rows)
    for batch in tqdm(batches(rows, BATCH_SIZE), desc=label, total=-(-total // BATCH_SIZE)):
        session.run(query, rows=batch)


# ─────────────────────────────────────────────
# Constraints & Índices
# ─────────────────────────────────────────────
CONSTRAINTS = [
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Customer)     REQUIRE n.customer_id IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Order)        REQUIRE n.order_id    IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Product)      REQUIRE n.product_id  IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Seller)       REQUIRE n.seller_id   IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Category)     REQUIRE n.name        IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Review)       REQUIRE n.review_id   IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Payment)      REQUIRE n.payment_id  IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Geolocation)  REQUIRE n.zip_code    IS UNIQUE",
]


def create_constraints(session):
    log.info("Criando constraints e índices...")
    for c in CONSTRAINTS:
        session.run(c)
    log.info("✓ Constraints criadas.")


# ─────────────────────────────────────────────
# Carregadores de Nós
# ─────────────────────────────────────────────
def load_customers(session):
    log.info("Carregando Customer...")
    df = clean(pd.read_csv(csv("olist_customers_dataset.csv")))
    rows = df.to_dict("records")

    q = """
    UNWIND $rows AS r
    MERGE (c:Customer {customer_id: r.customer_id})
    SET c.unique_id = r.customer_unique_id,
        c.city      = r.customer_city,
        c.state     = r.customer_state,
        c.zip_code  = r.customer_zip_code_prefix
    """
    run_batched(session, q, rows, "Customers")
    log.info(f"✓ {len(rows)} customers.")


def load_geolocations(session):
    log.info("Carregando Geolocation (pode demorar — ~1M linhas)...")
    df = clean(pd.read_csv(csv("olist_geolocation_dataset.csv")))

    # Deduplica por zip_code — mantém 1 coordenada por CEP
    df = df.drop_duplicates(subset=["geolocation_zip_code_prefix"])
    rows = df.rename(columns={
        "geolocation_zip_code_prefix": "zip_code",
        "geolocation_lat":             "lat",
        "geolocation_lng":             "lng",
        "geolocation_city":            "city",
        "geolocation_state":           "state",
    }).to_dict("records")

    q = """
    UNWIND $rows AS r
    MERGE (g:Geolocation {zip_code: r.zip_code})
    SET g.lat   = r.lat,
        g.lng   = r.lng,
        g.city  = r.city,
        g.state = r.state
    """
    run_batched(session, q, rows, "Geolocations")
    log.info(f"✓ {len(rows)} geolocations (CEPs únicos).")


def load_categories(session):
    log.info("Carregando Category...")
    df = clean(pd.read_csv(csv("product_category_name_translation.csv")))
    rows = df.rename(columns={
        "product_category_name":         "name_pt",
        "product_category_name_english": "name_en",
    }).to_dict("records")

    q = """
    UNWIND $rows AS r
    MERGE (cat:Category {name: r.name_pt})
    SET cat.name_en = r.name_en
    """
    run_batched(session, q, rows, "Categories")
    log.info(f"✓ {len(rows)} categorias.")


def load_products(session):
    log.info("Carregando Product...")
    df = clean(pd.read_csv(csv("olist_products_dataset.csv")))
    rows = df.rename(columns={
        "product_category_name":         "category",
        "product_name_lenght":           "name_len",
        "product_description_lenght":    "desc_len",
        "product_photos_qty":            "photos_qty",
        "product_weight_g":              "weight_g",
        "product_length_cm":             "length_cm",
        "product_height_cm":             "height_cm",
        "product_width_cm":              "width_cm",
    }).to_dict("records")

    q = """
    UNWIND $rows AS r
    MERGE (p:Product {product_id: r.product_id})
    SET p.category   = r.category,
        p.name_len   = r.name_len,
        p.desc_len   = r.desc_len,
        p.photos_qty = r.photos_qty,
        p.weight_g   = r.weight_g,
        p.length_cm  = r.length_cm,
        p.height_cm  = r.height_cm,
        p.width_cm   = r.width_cm
    """
    run_batched(session, q, rows, "Products")
    log.info(f"✓ {len(rows)} produtos.")


def load_sellers(session):
    log.info("Carregando Seller...")
    df = clean(pd.read_csv(csv("olist_sellers_dataset.csv")))
    rows = df.rename(columns={
        "seller_zip_code_prefix": "zip_code",
        "seller_city":            "city",
        "seller_state":           "state",
    }).to_dict("records")

    q = """
    UNWIND $rows AS r
    MERGE (s:Seller {seller_id: r.seller_id})
    SET s.city     = r.city,
        s.state    = r.state,
        s.zip_code = r.zip_code
    """
    run_batched(session, q, rows, "Sellers")
    log.info(f"✓ {len(rows)} sellers.")


def load_orders(session):
    log.info("Carregando Order...")
    df = clean(pd.read_csv(csv("olist_orders_dataset.csv")))
    rows = df.to_dict("records")

    q = """
    UNWIND $rows AS r
    MERGE (o:Order {order_id: r.order_id})
    SET o.status                    = r.order_status,
        o.purchase_timestamp        = r.order_purchase_timestamp,
        o.approved_at               = r.order_approved_at,
        o.delivered_carrier_date    = r.order_delivered_carrier_date,
        o.delivered_customer_date   = r.order_delivered_customer_date,
        o.estimated_delivery_date   = r.order_estimated_delivery_date
    """
    run_batched(session, q, rows, "Orders")
    log.info(f"✓ {len(rows)} orders.")


def load_payments(session):
    log.info("Carregando Payment...")
    df = clean(pd.read_csv(csv("olist_order_payments_dataset.csv")))
    df["payment_id"] = df["order_id"] + "_" + df["payment_sequential"].astype(str)
    rows = df.to_dict("records")

    q = """
    UNWIND $rows AS r
    MERGE (pay:Payment {payment_id: r.payment_id})
    SET pay.type         = r.payment_type,
        pay.installments = r.payment_installments,
        pay.value        = r.payment_value,
        pay.sequential   = r.payment_sequential
    """
    run_batched(session, q, rows, "Payments")
    log.info(f"✓ {len(rows)} payments.")


def load_reviews(session):
    log.info("Carregando Review...")
    df = clean(pd.read_csv(csv("olist_order_reviews_dataset.csv")))
    # Remove duplicatas de review_id (há duplicatas no dataset original)
    df = df.drop_duplicates(subset=["review_id"])
    rows = df.to_dict("records")

    q = """
    UNWIND $rows AS r
    MERGE (rev:Review {review_id: r.review_id})
    SET rev.score         = r.review_score,
        rev.comment_title = r.review_comment_title,
        rev.comment       = r.review_comment_message,
        rev.creation_date = r.review_creation_date,
        rev.answer_date   = r.review_answer_timestamp
    """
    run_batched(session, q, rows, "Reviews")
    log.info(f"✓ {len(rows)} reviews.")


# ─────────────────────────────────────────────
# Carregadores de Relacionamentos
# ─────────────────────────────────────────────
def load_rel_customer_placed_order(session):
    log.info("Relacionamento: (Customer)-[:PLACED]->(Order)...")
    df = clean(pd.read_csv(csv("olist_orders_dataset.csv")))[["customer_id", "order_id"]]
    rows = df.to_dict("records")

    q = """
    UNWIND $rows AS r
    MATCH (c:Customer {customer_id: r.customer_id})
    MATCH (o:Order    {order_id:    r.order_id})
    MERGE (c)-[:PLACED]->(o)
    """
    run_batched(session, q, rows, "PLACED")


def load_rel_order_items(session):
    log.info("Relacionamento: Order → OrderItem → Product / Seller...")
    df = clean(pd.read_csv(csv("olist_order_items_dataset.csv")))
    df["item_id"] = df["order_id"] + "_" + df["order_item_id"].astype(str)
    rows = df.to_dict("records")

    # Cria nó OrderItem e liga tudo de uma vez
    q = """
    UNWIND $rows AS r
    MATCH (o:Order   {order_id:   r.order_id})
    MATCH (p:Product {product_id: r.product_id})
    MATCH (s:Seller  {seller_id:  r.seller_id})
    MERGE (oi:OrderItem {item_id: r.item_id})
    SET oi.price          = r.price,
        oi.freight_value  = r.freight_value,
        oi.shipping_limit = r.shipping_limit_date
    MERGE (o)-[:CONTAINS]->(oi)
    MERGE (oi)-[:REFERENCES]->(p)
    MERGE (oi)-[:FULFILLED_BY]->(s)
    """
    run_batched(session, q, rows, "OrderItems")


def load_rel_order_payments(session):
    log.info("Relacionamento: (Order)-[:PAID_WITH]->(Payment)...")
    df = clean(pd.read_csv(csv("olist_order_payments_dataset.csv")))
    df["payment_id"] = df["order_id"] + "_" + df["payment_sequential"].astype(str)
    rows = df[["order_id", "payment_id"]].to_dict("records")

    q = """
    UNWIND $rows AS r
    MATCH (o:Order   {order_id:   r.order_id})
    MATCH (pay:Payment {payment_id: r.payment_id})
    MERGE (o)-[:PAID_WITH]->(pay)
    """
    run_batched(session, q, rows, "PAID_WITH")


def load_rel_order_reviews(session):
    log.info("Relacionamento: (Order)-[:HAS_REVIEW]->(Review)...")
    df = clean(pd.read_csv(csv("olist_order_reviews_dataset.csv")))
    df = df.drop_duplicates(subset=["review_id"])[["order_id", "review_id"]]
    rows = df.to_dict("records")

    q = """
    UNWIND $rows AS r
    MATCH (o:Order  {order_id:  r.order_id})
    MATCH (rev:Review {review_id: r.review_id})
    MERGE (o)-[:HAS_REVIEW]->(rev)
    """
    run_batched(session, q, rows, "HAS_REVIEW")


def load_rel_product_category(session):
    log.info("Relacionamento: (Product)-[:BELONGS_TO]->(Category)...")
    df = clean(pd.read_csv(csv("olist_products_dataset.csv")))[["product_id", "product_category_name"]]
    df = df.dropna(subset=["product_category_name"])
    rows = df.to_dict("records")

    q = """
    UNWIND $rows AS r
    MATCH (p:Product  {product_id: r.product_id})
    MERGE (cat:Category {name: r.product_category_name})
    MERGE (p)-[:BELONGS_TO]->(cat)
    """
    run_batched(session, q, rows, "BELONGS_TO")


def load_rel_located_in(session):
    log.info("Relacionamento: Customer/Seller -[:LOCATED_IN]-> Geolocation...")

    # Customers
    df_c = clean(pd.read_csv(csv("olist_customers_dataset.csv")))[
        ["customer_id", "customer_zip_code_prefix"]
    ].rename(columns={"customer_zip_code_prefix": "zip_code"})
    rows_c = df_c.to_dict("records")

    q_c = """
    UNWIND $rows AS r
    MATCH (c:Customer    {customer_id: r.customer_id})
    MATCH (g:Geolocation {zip_code:    r.zip_code})
    MERGE (c)-[:LOCATED_IN]->(g)
    """
    run_batched(session, q_c, rows_c, "Customer LOCATED_IN")

    # Sellers
    df_s = clean(pd.read_csv(csv("olist_sellers_dataset.csv")))[
        ["seller_id", "seller_zip_code_prefix"]
    ].rename(columns={"seller_zip_code_prefix": "zip_code"})
    rows_s = df_s.to_dict("records")

    q_s = """
    UNWIND $rows AS r
    MATCH (s:Seller      {seller_id: r.seller_id})
    MATCH (g:Geolocation {zip_code:  r.zip_code})
    MERGE (s)-[:LOCATED_IN]->(g)
    """
    run_batched(session, q_s, rows_s, "Seller LOCATED_IN")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
def main():
    log.info(f"Conectando em {NEO4J_URI} como '{NEO4J_USER}'...")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    with driver.session() as session:
        start = time.time()

        # 1. Constraints
        create_constraints(session)

        # 2. Nós
        log.info("── FASE 1: Nós ──────────────────────────────")
        load_customers(session)
        load_geolocations(session)
        load_categories(session)
        load_products(session)
        load_sellers(session)
        load_orders(session)
        load_payments(session)
        load_reviews(session)

        # 3. Relacionamentos
        log.info("── FASE 2: Relacionamentos ──────────────────")
        load_rel_customer_placed_order(session)
        load_rel_order_items(session)
        load_rel_order_payments(session)
        load_rel_order_reviews(session)
        load_rel_product_category(session)
        load_rel_located_in(session)

        elapsed = time.time() - start
        log.info(f"Concluído em {elapsed:.1f}s")

        # 4. Resumo
        log.info("── RESUMO DO GRAFO ──────────────────────────")
        result = session.run("MATCH (n) RETURN labels(n)[0] AS label, count(n) AS total ORDER BY total DESC")
        for record in result:
            log.info(f"   {record['label']:15s} → {record['total']:,} nós")

        result = session.run("MATCH ()-[r]->() RETURN type(r) AS rel, count(r) AS total ORDER BY total DESC")
        for record in result:
            log.info(f"   {record['rel']:20s} → {record['total']:,} rels")

    driver.close()


if __name__ == "__main__":
    main()