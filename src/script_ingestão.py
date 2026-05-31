import os
import logging
import pandas as pd
from neo4j import GraphDatabase

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "olist1234")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "neo4j")
DATA_DIR = os.getenv("DATA_DIR", "./data/silver")

BATCH_SIZE = 500

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

def csv(f): return os.path.join(DATA_DIR, f)

def run(session, query, rows, label):
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i+BATCH_SIZE]
        session.run(query, rows=batch).consume()
        if i % (BATCH_SIZE * 10) == 0:
            log.info(f"{label} {i}/{len(rows)}")

def clear(session):
    session.run("MATCH (n) DETACH DELETE n").consume()

def load_customers(session):
    df = pd.read_csv(csv("olist_customers_dataset.csv"))
    df = df.dropna(subset=["customer_id"])
    df["customer_id"] = df["customer_id"].astype(str).str.strip()
    rows = df.to_dict("records")
    q = """
    UNWIND $rows AS r
    MERGE (c:Customer {customer_id:r.customer_id})
    SET c.city=r.customer_city,
        c.state=r.customer_state,
        c.zip=r.customer_zip_code_prefix
    """
    run(session, q, rows, "customers")

def load_orders(session):
    df = pd.read_csv(csv("olist_orders_dataset.csv"))
    df = df.dropna(subset=["order_id","customer_id"])
    df["order_id"] = df["order_id"].astype(str).str.strip()
    df["customer_id"] = df["customer_id"].astype(str).str.strip()
    rows = df.to_dict("records")
    q = """
    UNWIND $rows AS r
    MERGE (o:Order {order_id:r.order_id})
    SET o.status=r.order_status,
        o.purchase=r.order_purchase_timestamp,
        o.delivered=r.order_delivered_customer_date,
        o.estimated=r.order_estimated_delivery_date
    """
    run(session, q, rows, "orders")

def load_products(session):
    df = pd.read_csv(csv("olist_products_dataset.csv"))
    df = df.dropna(subset=["product_id"])
    df["product_id"] = df["product_id"].astype(str).str.strip()
    rows = df.to_dict("records")
    q = """
    UNWIND $rows AS r
    MERGE (p:Product {product_id:r.product_id})
    SET p.category=r.product_category_name
    """
    run(session, q, rows, "products")

def load_sellers(session):
    df = pd.read_csv(csv("olist_sellers_dataset.csv"))
    df = df.dropna(subset=["seller_id"])
    df["seller_id"] = df["seller_id"].astype(str).str.strip()
    rows = df.to_dict("records")
    q = """
    UNWIND $rows AS r
    MERGE (s:Seller {seller_id:r.seller_id})
    SET s.city=r.seller_city,
        s.state=r.seller_state,
        s.zip=r.seller_zip_code_prefix
    """
    run(session, q, rows, "sellers")

def load_geolocation(session):
    df = pd.read_csv(csv("olist_geolocation_dataset.csv"))
    df = df.dropna(subset=["geolocation_zip_code_prefix"])
    df = df.rename(columns={
        "geolocation_zip_code_prefix": "zip",
        "geolocation_city": "city",
        "geolocation_state": "state"
    })
    df = df.drop_duplicates(subset=["zip"])
    rows = df.to_dict("records")
    q = """
    UNWIND $rows AS r
    MERGE (g:Geolocation {zip:r.zip})
    SET g.city=r.city,
        g.state=r.state
    """
    run(session, q, rows, "geolocation")

def load_items(session):
    df = pd.read_csv(csv("olist_order_items_dataset.csv"))
    df = df.dropna(subset=["order_id","product_id","seller_id"])
    df["order_id"] = df["order_id"].astype(str).str.strip()
    df["product_id"] = df["product_id"].astype(str).str.strip()
    df["seller_id"] = df["seller_id"].astype(str).str.strip()
    df["item_id"] = df["order_id"] + "_" + df["order_item_id"].astype(str)
    rows = df.to_dict("records")
    q = """
    UNWIND $rows AS r
    MATCH (o:Order {order_id:r.order_id})
    MATCH (p:Product {product_id:r.product_id})
    MATCH (s:Seller {seller_id:r.seller_id})
    MERGE (i:OrderItem {item_id:r.item_id})
    SET i.price=toFloat(r.price),
        i.freight=toFloat(r.freight_value)
    MERGE (o)-[:CONTAINS]->(i)
    MERGE (i)-[:REFERENCES]->(p)
    MERGE (i)-[:FULFILLED_BY]->(s)
    """
    run(session, q, rows, "items")

def load_payments(session):
    df = pd.read_csv(csv("olist_order_payments_dataset.csv"))
    df = df.dropna(subset=["order_id","payment_sequential"])
    df["order_id"] = df["order_id"].astype(str).str.strip()
    df["payment_id"] = df["order_id"] + "_" + df["payment_sequential"].astype(str)
    rows = df.to_dict("records")
    q = """
    UNWIND $rows AS r
    MERGE (p:Payment {payment_id:r.payment_id})
    SET p.type=r.payment_type,
        p.value=toFloat(r.payment_value)
    """
    run(session, q, rows, "payments")

def load_reviews(session):
    df = pd.read_csv(csv("olist_order_reviews_dataset.csv"))
    df = df.drop_duplicates("review_id")
    df = df.dropna(subset=["review_id"])
    rows = df.to_dict("records")
    q = """
    UNWIND $rows AS r
    MERGE (rev:Review {review_id:r.review_id})
    SET rev.score=toInteger(r.review_score)
    """
    run(session, q, rows, "reviews")

def rel_customer_orders(session):
    df = pd.read_csv(csv("olist_orders_dataset.csv"))
    df = df.dropna(subset=["customer_id","order_id"])
    df["customer_id"] = df["customer_id"].astype(str).str.strip()
    df["order_id"] = df["order_id"].astype(str).str.strip()
    rows = df.to_dict("records")
    q = """
    UNWIND $rows AS r
    MATCH (c:Customer {customer_id:r.customer_id})
    MATCH (o:Order {order_id:r.order_id})
    MERGE (c)-[:PLACED]->(o)
    """
    run(session, q, rows, "placed")

def rel_payments(session):
    df = pd.read_csv(csv("olist_order_payments_dataset.csv"))
    df = df.dropna(subset=["order_id","payment_sequential"])
    df["order_id"] = df["order_id"].astype(str).str.strip()
    df["payment_id"] = df["order_id"] + "_" + df["payment_sequential"].astype(str)
    rows = df.to_dict("records")
    q = """
    UNWIND $rows AS r
    MATCH (o:Order {order_id:r.order_id})
    MATCH (p:Payment {payment_id:r.payment_id})
    MERGE (o)-[:PAID_WITH]->(p)
    """
    run(session, q, rows, "paid_with")

def rel_geolocation(session):
    df_c = pd.read_csv(csv("olist_customers_dataset.csv"))
    df_c = df_c.dropna(subset=["customer_id","customer_zip_code_prefix"])
    df_c["customer_id"] = df_c["customer_id"].astype(str).str.strip()
    rows_c = df_c.rename(columns={"customer_zip_code_prefix": "zip"}).to_dict("records")
    q_c = """
    UNWIND $rows AS r
    MATCH (c:Customer {customer_id:r.customer_id})
    MATCH (g:Geolocation {zip:r.zip})
    MERGE (c)-[:LOCATED_IN]->(g)
    """
    run(session, q_c, rows_c, "customer_located")

    df_s = pd.read_csv(csv("olist_sellers_dataset.csv"))
    df_s = df_s.dropna(subset=["seller_id","seller_zip_code_prefix"])
    df_s["seller_id"] = df_s["seller_id"].astype(str).str.strip()
    rows_s = df_s.rename(columns={"seller_zip_code_prefix": "zip"}).to_dict("records")
    q_s = """
    UNWIND $rows AS r
    MATCH (s:Seller {seller_id:r.seller_id})
    MATCH (g:Geolocation {zip:r.zip})
    MERGE (s)-[:LOCATED_IN]->(g)
    """
    run(session, q_s, rows_s, "seller_located")

def main():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with driver.session(database=NEO4J_DATABASE) as session:
        clear(session)
        load_customers(session)
        load_orders(session)
        load_products(session)
        load_sellers(session)
        load_geolocation(session)
        load_items(session)
        load_payments(session)
        load_reviews(session)
        rel_customer_orders(session)
        rel_payments(session)
        rel_geolocation(session)
    driver.close()

if __name__ == "__main__":
    main()