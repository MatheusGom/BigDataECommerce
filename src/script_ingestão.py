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

LOAD_GEOLOCATIONS = os.getenv("LOAD_GEOLOCATIONS", "true").lower() == "true"
LOAD_LOCATED_IN = os.getenv("LOAD_LOCATED_IN", "true").lower() == "true"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

log = logging.getLogger(__name__)


def csv(filename: str) -> str:
    return os.path.join(DATA_DIR, filename)


def clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    return df.where(pd.notna(df), None)


def batches(rows: List[Dict[str, Any]], size: int):
    for i in range(0, len(rows), size):
        yield rows[i:i + size]


def to_datetime_str(series: pd.Series) -> pd.Series:
    s = pd.to_datetime(series, errors="coerce")
    s = s.dt.strftime("%Y-%m-%d %H:%M:%S")
    return s.where(pd.notna(s), None)


def retryable_run(session, query: str, rows: List[Dict[str, Any]], label: str):
    total_batches = (len(rows) + BATCH_SIZE - 1) // BATCH_SIZE

    for idx, batch in enumerate(batches(rows, BATCH_SIZE), start=1):
        attempt = 0

        while True:
            try:
                session.run(
                    "CALL { WITH $rows AS rows " + query + " } IN TRANSACTIONS",
                    rows=batch,
                    timeout=TX_TIMEOUT_SECONDS,
                ).consume()

                if idx % 50 == 0 or idx == total_batches:
                    log.info("%s: batch %s/%s concluído", label, idx, total_batches)

                break

            except (ServiceUnavailable, SessionExpired, TransientError) as e:
                attempt += 1

                if attempt > MAX_RETRIES:
                    raise

                wait_s = RETRY_BASE_SECONDS * attempt
                log.warning("%s retry %s/%s: %s", label, attempt, MAX_RETRIES, str(e))
                time.sleep(wait_s)


CONSTRAINTS_AND_INDEXES = [
    "CREATE CONSTRAINT customer_id_unique IF NOT EXISTS FOR (n:Customer) REQUIRE n.customer_id IS UNIQUE",
    "CREATE CONSTRAINT order_id_unique IF NOT EXISTS FOR (n:Order) REQUIRE n.order_id IS UNIQUE",
    "CREATE CONSTRAINT product_id_unique IF NOT EXISTS FOR (n:Product) REQUIRE n.product_id IS UNIQUE",
    "CREATE CONSTRAINT seller_id_unique IF NOT EXISTS FOR (n:Seller) REQUIRE n.seller_id IS UNIQUE",
    "CREATE CONSTRAINT category_name_unique IF NOT EXISTS FOR (n:Category) REQUIRE n.name IS UNIQUE",
    "CREATE CONSTRAINT review_id_unique IF NOT EXISTS FOR (n:Review) REQUIRE n.review_id IS UNIQUE",
    "CREATE CONSTRAINT payment_id_unique IF NOT EXISTS FOR (n:Payment) REQUIRE n.payment_id IS UNIQUE",
    "CREATE CONSTRAINT order_item_id_unique IF NOT EXISTS FOR (n:OrderItem) REQUIRE n.item_id IS UNIQUE",
    "CREATE INDEX geolocation_zip_idx IF NOT EXISTS FOR (n:Geolocation) ON (n.zip_code)",
]


def create_schema(session):
    for stmt in CONSTRAINTS_AND_INDEXES:
        session.run(stmt).consume()
    session.run("CALL db.awaitIndexes()").consume()


def clear_database(session):
    session.run("MATCH (n) DETACH DELETE n").consume()


def load_customers(session):
    df = clean(pd.read_csv(csv("olist_customers_dataset.csv")))
    df["customer_id"] = df["customer_id"].astype(str).str.strip()

    rows = df.to_dict("records")

    q = """
    UNWIND rows AS r
    MERGE (c:Customer {customer_id: r.customer_id})
    SET c.unique_id = r.customer_unique_id,
        c.city = r.customer_city,
        c.state = r.customer_state,
        c.zip_code = r.customer_zip_code_prefix
    """

    retryable_run(session, q, rows, "Customers")


def load_geolocations(session):
    if not LOAD_GEOLOCATIONS:
        return

    df = clean(pd.read_csv(csv("olist_geolocation_dataset.csv")))
    df = df.rename(columns={
        "geolocation_zip_code_prefix": "zip_code",
        "geolocation_lat": "lat",
        "geolocation_lng": "lng",
        "geolocation_city": "city",
        "geolocation_state": "state",
    })

    rows = df.to_dict("records")

    q = """
    UNWIND rows AS r
    MERGE (g:Geolocation {zip_code: r.zip_code, lat: r.lat, lng: r.lng})
    SET g.city = r.city,
        g.state = r.state
    """

    retryable_run(session, q, rows, "Geolocations")


def load_categories(session):
    df = clean(pd.read_csv(csv("product_category_name_translation.csv")))
    rows = df.rename(columns={
        "product_category_name": "name_pt",
        "product_category_name_english": "name_en",
    }).to_dict("records")

    q = """
    UNWIND rows AS r
    MERGE (cat:Category {name: r.name_pt})
    SET cat.name_en = r.name_en
    """

    retryable_run(session, q, rows, "Categories")


def load_products(session):
    df = clean(pd.read_csv(csv("olist_products_dataset.csv")))

    df["product_id"] = df["product_id"].astype(str).str.strip()
    df["product_category_name"] = df["product_category_name"].astype(str).str.strip()

    rows = df.rename(columns={
        "product_category_name": "category",
        "product_name_lenght": "name_len",
        "product_description_lenght": "desc_len",
        "product_photos_qty": "photos_qty",
        "product_weight_g": "weight_g",
        "product_length_cm": "length_cm",
        "product_height_cm": "height_cm",
        "product_width_cm": "width_cm",
    }).to_dict("records")

    q = """
    UNWIND rows AS r
    MERGE (p:Product {product_id: r.product_id})
    SET p.category = r.category,
        p.name_len = r.name_len,
        p.desc_len = r.desc_len,
        p.photos_qty = r.photos_qty,
        p.weight_g = r.weight_g,
        p.length_cm = r.length_cm,
        p.height_cm = r.height_cm,
        p.width_cm = r.width_cm
    """

    retryable_run(session, q, rows, "Products")


def load_sellers(session):
    df = clean(pd.read_csv(csv("olist_sellers_dataset.csv")))
    df["seller_id"] = df["seller_id"].astype(str).str.strip()

    rows = df.rename(columns={
        "seller_zip_code_prefix": "zip_code",
        "seller_city": "city",
        "seller_state": "state",
    }).to_dict("records")

    q = """
    UNWIND rows AS r
    MERGE (s:Seller {seller_id: r.seller_id})
    SET s.city = r.city,
        s.state = r.state,
        s.zip_code = r.zip_code
    """

    retryable_run(session, q, rows, "Sellers")


def load_orders(session):
    df = clean(pd.read_csv(csv("olist_orders_dataset.csv")))

    df["order_id"] = df["order_id"].astype(str).str.strip()
    df["customer_id"] = df["customer_id"].astype(str).str.strip()

    for col in [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]:
        if col in df.columns:
            df[col] = to_datetime_str(df[col])

    rows = df.to_dict("records")

    q = """
    UNWIND rows AS r
    MERGE (o:Order {order_id: r.order_id})
    SET o.status = r.order_status,
        o.purchase_timestamp = r.order_purchase_timestamp,
        o.approved_at = r.order_approved_at,
        o.delivered_carrier_date = r.order_delivered_carrier_date,
        o.delivered_customer_date = r.order_delivered_customer_date,
        o.estimated_delivery_date = r.order_estimated_delivery_date
    """

    retryable_run(session, q, rows, "Orders")


def load_order_items(session):
    df = clean(pd.read_csv(csv("olist_order_items_dataset.csv")))

    df["order_id"] = df["order_id"].astype(str).str.strip()
    df["product_id"] = df["product_id"].astype(str).str.strip()
    df["seller_id"] = df["seller_id"].astype(str).str.strip()

    df["item_id"] = df["order_id"] + "_" + df["order_item_id"].astype(str)

    if "shipping_limit_date" in df.columns:
        df["shipping_limit_date"] = to_datetime_str(df["shipping_limit_date"])

    rows = df.to_dict("records")

    q = """
    UNWIND rows AS r
    MATCH (o:Order {order_id: r.order_id})
    MATCH (p:Product {product_id: r.product_id})
    MATCH (s:Seller {seller_id: r.seller_id})
    WITH o, p, s, r
    WHERE o IS NOT NULL AND p IS NOT NULL AND s IS NOT NULL
    MERGE (oi:OrderItem {item_id: r.item_id})
    SET oi.price = toFloat(r.price),
        oi.freight_value = toFloat(r.freight_value),
        oi.shipping_limit = r.shipping_limit_date
    MERGE (o)-[:CONTAINS]->(oi)
    MERGE (oi)-[:REFERENCES]->(p)
    MERGE (oi)-[:FULFILLED_BY]->(s)
    """

    retryable_run(session, q, rows, "OrderItems")


def load_payments(session):
    df = clean(pd.read_csv(csv("olist_order_payments_dataset.csv")))

    df["payment_id"] = df["order_id"].astype(str).str.strip() + "_" + df["payment_sequential"].astype(str)

    rows = df.to_dict("records")

    q = """
    UNWIND rows AS r
    MERGE (pay:Payment {payment_id: r.payment_id})
    SET pay.type = r.payment_type,
        pay.installments = r.payment_installments,
        pay.value = r.payment_value,
        pay.sequential = r.payment_sequential
    """

    retryable_run(session, q, rows, "Payments")


def load_reviews(session):
    df = clean(pd.read_csv(csv("olist_order_reviews_dataset.csv")))
    df = df.drop_duplicates(subset=["review_id"])

    df["order_id"] = df["order_id"].astype(str).str.strip()

    if "review_creation_date" in df.columns:
        df["review_creation_date"] = to_datetime_str(df["review_creation_date"])
    if "review_answer_timestamp" in df.columns:
        df["review_answer_timestamp"] = to_datetime_str(df["review_answer_timestamp"])

    rows = df.to_dict("records")

    q = """
    UNWIND rows AS r
    MERGE (rev:Review {review_id: r.review_id})
    SET rev.score = r.review_score,
        rev.comment_title = r.review_comment_title,
        rev.comment = r.review_comment_message,
        rev.creation_date = r.review_creation_date,
        rev.answer_date = r.review_answer_timestamp
    """

    retryable_run(session, q, rows, "Reviews")


def load_relations(session):
    df = clean(pd.read_csv(csv("olist_orders_dataset.csv")))

    df["order_id"] = df["order_id"].astype(str).str.strip()
    df["customer_id"] = df["customer_id"].astype(str).str.strip()

    rows = df[["customer_id", "order_id"]].to_dict("records")

    q = """
    UNWIND rows AS r
    MATCH (c:Customer {customer_id: r.customer_id})
    MATCH (o:Order {order_id: r.order_id})
    MERGE (c)-[:PLACED]->(o)
    """

    retryable_run(session, q, rows, "PLACED")


def validate_counts(session):
    checks = {
        "Customer": "MATCH (n:Customer) RETURN count(n) AS total",
        "Order": "MATCH (n:Order) RETURN count(n) AS total",
        "Product": "MATCH (n:Product) RETURN count(n) AS total",
        "Seller": "MATCH (n:Seller) RETURN count(n) AS total",
        "OrderItem": "MATCH (n:OrderItem) RETURN count(n) AS total",
    }

    for k, q in checks.items():
        print(k, session.run(q).single()["total"])


def main():
    driver = GraphDatabase.driver(
        NEO4J_URI,
        auth=(NEO4J_USER, NEO4J_PASSWORD),
    )

    with driver.session(database=NEO4J_DATABASE) as session:
        create_schema(session)

        load_customers(session)
        load_geolocations(session)
        load_categories(session)
        load_products(session)
        load_sellers(session)
        load_orders(session)
        load_payments(session)
        load_reviews(session)

        load_relations(session)
        load_order_items(session)

        validate_counts(session)

    driver.close()


if __name__ == "__main__":
    main()