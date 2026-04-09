import os
import time
import logging
from typing import List, Dict, Any
import pandas as pd
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, SessionExpired, TransientError

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "olist1234")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "neo4j")
DATA_DIR = os.getenv("DATA_DIR", "./data/silver")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
RETRY_BASE_SECONDS = float(os.getenv("RETRY_BASE_SECONDS", "2"))
TX_TIMEOUT_SECONDS = int(os.getenv("TX_TIMEOUT_SECONDS", "300"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def csv(filename: str):
    return os.path.join(DATA_DIR, filename)


def clean(df):
    return df.where(pd.notna(df), None)


def to_str(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    return df


def to_datetime_str(series):
    s = pd.to_datetime(series, errors="coerce")
    return s.dt.strftime("%Y-%m-%d %H:%M:%S")


def batches(rows, size):
    for i in range(0, len(rows), size):
        yield rows[i:i+size]


def retryable_run(session, query, rows, label):
    total = max(1, (len(rows) + BATCH_SIZE - 1) // BATCH_SIZE)

    for i, batch in enumerate(batches(rows, BATCH_SIZE), 1):
        attempt = 0
        while True:
            try:
                session.run(
                    "CALL { WITH $rows AS rows " + query + " } IN TRANSACTIONS",
                    rows=batch,
                    timeout=TX_TIMEOUT_SECONDS,
                ).consume()
                break
            except (ServiceUnavailable, SessionExpired, TransientError):
                attempt += 1
                if attempt > MAX_RETRIES:
                    raise
                time.sleep(RETRY_BASE_SECONDS * attempt)


CONSTRAINTS = [
    "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Customer) REQUIRE c.customer_id IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (o:Order) REQUIRE o.order_id IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Product) REQUIRE p.product_id IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Seller) REQUIRE s.seller_id IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (r:Review) REQUIRE r.review_id IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (pay:Payment) REQUIRE pay.payment_id IS UNIQUE",
    "CREATE CONSTRAINT IF NOT EXISTS FOR (oi:OrderItem) REQUIRE oi.item_id IS UNIQUE",
]


def create_schema(session):
    for s in CONSTRAINTS:
        session.run(s).consume()


def load_customers(session):
    df = clean(pd.read_csv(csv("olist_customers_dataset.csv")))
    df = to_str(df, ["customer_id"])

    rows = df.to_dict("records")

    q = """
    UNWIND rows AS r
    MERGE (c:Customer {customer_id:r.customer_id})
    SET c.city=r.customer_city, c.state=r.customer_state
    """

    retryable_run(session, q, rows, "customers")


def load_orders(session):
    df = clean(pd.read_csv(csv("olist_orders_dataset.csv")))
    df = to_str(df, ["order_id", "customer_id"])

    for c in ["order_purchase_timestamp","order_approved_at","order_delivered_customer_date","order_estimated_delivery_date"]:
        df[c] = to_datetime_str(df[c]) if c in df else None

    rows = df.to_dict("records")

    q = """
    UNWIND rows AS r
    MERGE (o:Order {order_id:r.order_id})
    SET o.status=r.order_status
    """

    retryable_run(session, q, rows, "orders")


def load_products(session):
    df = clean(pd.read_csv(csv("olist_products_dataset.csv")))
    df = to_str(df, ["product_id", "product_category_name"])

    rows = df.to_dict("records")

    q = """
    UNWIND rows AS r
    MERGE (p:Product {product_id:r.product_id})
    SET p.category=r.product_category_name
    """

    retryable_run(session, q, rows, "products")


def load_sellers(session):
    df = clean(pd.read_csv(csv("olist_sellers_dataset.csv")))
    df = to_str(df, ["seller_id"])

    rows = df.to_dict("records")

    q = """
    UNWIND rows AS r
    MERGE (s:Seller {seller_id:r.seller_id})
    SET s.city=r.seller_city, s.state=r.seller_state
    """

    retryable_run(session, q, rows, "sellers")


def load_order_items(session):
    df = clean(pd.read_csv(csv("olist_order_items_dataset.csv")))
    df = to_str(df, ["order_id","product_id","seller_id"])

    df["item_id"] = df["order_id"] + "_" + df["order_item_id"].astype(str)

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["freight_value"] = pd.to_numeric(df["freight_value"], errors="coerce")

    rows = df.dropna(subset=["price","product_id","seller_id"]).to_dict("records")

    q = """
    UNWIND rows AS r
    MATCH (o:Order {order_id:r.order_id})
    MATCH (p:Product {product_id:r.product_id})
    MATCH (s:Seller {seller_id:r.seller_id})
    WITH o,p,s,r
    WHERE o IS NOT NULL AND p IS NOT NULL AND s IS NOT NULL
    MERGE (oi:OrderItem {item_id:r.item_id})
    SET oi.price=toFloat(r.price)
    MERGE (o)-[:CONTAINS]->(oi)
    MERGE (oi)-[:REFERENCES]->(p)
    MERGE (oi)-[:FULFILLED_BY]->(s)
    """

    retryable_run(session, q, rows, "order_items")


def load_payments(session):
    df = clean(pd.read_csv(csv("olist_order_payments_dataset.csv")))
    df = to_str(df, ["order_id","payment_type"])

    df["payment_id"] = df["order_id"] + "_" + df["payment_sequential"].astype(str)

    rows = df.to_dict("records")

    q = """
    UNWIND rows AS r
    MERGE (p:Payment {payment_id:r.payment_id})
    SET p.type=r.payment_type,
        p.value=toFloat(r.payment_value),
        p.installments=toInteger(r.payment_installments)
    """

    retryable_run(session, q, rows, "payments")


def load_relations(session):
    df = clean(pd.read_csv(csv("olist_orders_dataset.csv")))
    df = to_str(df, ["customer_id","order_id"])

    rows = df.to_dict("records")

    q = """
    UNWIND rows AS r
    MATCH (c:Customer {customer_id:r.customer_id})
    MATCH (o:Order {order_id:r.order_id})
    MERGE (c)-[:PLACED]->(o)
    """

    retryable_run(session, q, rows, "relations")


def validate(session):
    for l in ["Customer","Order","Product","Seller","OrderItem","Payment"]:
        print(l, session.run(f"MATCH (n:{l}) RETURN count(n) AS c").single()["c"])


def main():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    with driver.session(database=NEO4J_DATABASE) as session:
        create_schema(session)

        load_customers(session)
        load_sellers(session)
        load_products(session)
        load_orders(session)
        load_payments(session)
        load_order_items(session)
        load_relations(session)

        validate(session)

    driver.close()


if __name__ == "__main__":
    main()