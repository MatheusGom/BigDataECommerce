import os
import time
import logging
import pandas as pd
from tqdm import tqdm
from neo4j import GraphDatabase

NEO4J_URI      = os.getenv("NEO4J_URI",      "bolt://localhost:7687")
NEO4J_USER     = os.getenv("NEO4J_USER",     "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "olist1234")
DATA_DIR       = os.getenv("DATA_DIR",       "./data/silver")
BATCH_SIZE     = 100

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

def csv(filename: str) -> str:
    return os.path.join(DATA_DIR, filename)

def clean(df: pd.DataFrame) -> pd.DataFrame:
    return df.where(pd.notna(df), other=None)

def batches(lst: list, size: int):
    for i in range(0, len(lst), size):
        yield lst[i : i + size]

def run_batched(session, query: str, rows: list, label: str):
    total = len(rows)
    for batch in tqdm(batches(rows, BATCH_SIZE), desc=label, total=-(-total // BATCH_SIZE)):
        session.run(query, rows=batch)

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
    for c in CONSTRAINTS:
        session.run(c)

def load_customers(session):
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

def load_geolocations(session):
    df = clean(pd.read_csv(csv("olist_geolocation_dataset.csv")))
    rows = df.rename(columns={
        "geolocation_zip_code_prefix": "zip_code",
        "geolocation_lat":             "lat",
        "geolocation_lng":             "lng",
        "geolocation_city":            "city",
        "geolocation_state":           "state",
    }).to_dict("records")
    q = """
    UNWIND $rows AS r
    CREATE (g:Geolocation)
    SET g.zip_code = r.zip_code,
        g.lat      = r.lat,
        g.lng      = r.lng,
        g.city     = r.city,
        g.state    = r.state
    """
    run_batched(session, q, rows, "Geolocations")

def load_categories(session):
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

def load_products(session):
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

def load_sellers(session):
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

def load_orders(session):
    df = clean(pd.read_csv(csv("olist_orders_dataset.csv")))
    date_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")
            df[col] = df[col].where(df[col].notna(), None)
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

def load_payments(session):
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

def load_reviews(session):
    df = clean(pd.read_csv(csv("olist_order_reviews_dataset.csv")))
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

def load_rel_customer_placed_order(session):
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
    df = clean(pd.read_csv(csv("olist_order_items_dataset.csv")))
    df["item_id"] = df["order_id"] + "_" + df["order_item_id"].astype(str)
    rows = df.to_dict("records")
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
    df_c = clean(pd.read_csv(csv("olist_customers_dataset.csv")))[
        ["customer_id", "customer_zip_code_prefix"]
    ].rename(columns={"customer_zip_code_prefix": "zip_code"})
    rows_c = df_c.to_dict("records")
    q_c = """
    UNWIND $rows AS r
    MATCH (c:Customer {customer_id: r.customer_id})
    MATCH (g:Geolocation)
    WHERE g.zip_code = r.zip_code
    WITH c, g LIMIT 1
    MERGE (c)-[:LOCATED_IN]->(g)
    """
    run_batched(session, q_c, rows_c, "Customer LOCATED_IN")

    df_s = clean(pd.read_csv(csv("olist_sellers_dataset.csv")))[
        ["seller_id", "seller_zip_code_prefix"]
    ].rename(columns={"seller_zip_code_prefix": "zip_code"})
    rows_s = df_s.to_dict("records")
    q_s = """
    UNWIND $rows AS r
    MATCH (s:Seller {seller_id: r.seller_id})
    MATCH (g:Geolocation)
    WHERE g.zip_code = r.zip_code
    WITH s, g LIMIT 1
    MERGE (s)-[:LOCATED_IN]->(g)
    """
    run_batched(session, q_s, rows_s, "Seller LOCATED_IN")

def main():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with driver.session() as session:
        create_constraints(session)
        load_customers(session)
        load_geolocations(session)
        load_categories(session)
        load_products(session)
        load_sellers(session)
        load_orders(session)
        load_payments(session)
        load_reviews(session)
        load_rel_customer_placed_order(session)
        load_rel_order_items(session)
        load_rel_order_payments(session)
        load_rel_order_reviews(session)
        load_rel_product_category(session)
        load_rel_located_in(session)
    driver.close()

if __name__ == "__main__":
    main()