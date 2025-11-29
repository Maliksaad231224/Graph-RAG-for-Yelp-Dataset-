"""
Import Functions for Olist Dataset
Each function handles one CSV file and creates nodes/relationships in batches
"""
import pandas as pd
import os

BATCH_SIZE = 5000  # Process 5000 rows at a time
SAMPLE_PERCENTAGE = 0.35  # Use 35% of data to stay under 200K node limit

def get_data_path():
    """Get the path to the Data Preprocessing folder"""
    return os.path.join(os.path.dirname(__file__), '..', 'Data Preprocessing')


def import_customers(neo4j_conn):
    """Import customers from CSV and create Customer nodes"""
    print("\n=== Importing Customers ===")
    
    csv_path = os.path.join(get_data_path(), 'olist_customers_dataset_enhanced_sample.csv')
    df = pd.read_csv(csv_path)
    total_rows = len(df)
    print(f"Processing {total_rows} customers...")
    
    query = """
    UNWIND $batch AS row
    MERGE (c:Customer {id: row.customer_id})
    SET c.unique_id = row.customer_unique_id,
        c.zip_code = row.customer_zip_code_prefix,
        c.city = row.customer_city,
        c.state = row.customer_state,
        c.total_orders = toInteger(row.total_orders),
        c.first_purchase = row.first_purchase_date,
        c.last_purchase = row.last_purchase_date,
        c.lifetime_days = toInteger(row.customer_lifetime_days),
        c.total_spending = toFloat(row.total_spending),
        c.avg_order_value = toFloat(row.avg_order_value),
        c.avg_review_score = toFloat(row.avg_review_score),
        c.review_count = toInteger(row.review_count),
        c.segment = row.customer_segment
    """
    
    # Process in batches
    for i in range(0, total_rows, BATCH_SIZE):
        batch = df.iloc[i:i+BATCH_SIZE].to_dict('records')
        neo4j_conn.execute_write(query, {'batch': batch})
        print(f"  Processed {min(i+BATCH_SIZE, total_rows)}/{total_rows} customers")
    
    print(f"✓ Imported {total_rows} customers\n")


def import_products(neo4j_conn):
    """Import products from CSV and create Product nodes"""
    print("\n=== Importing Products ===")
    
    csv_path = os.path.join(get_data_path(), 'olist_products_dataset_enhanced_sample.csv')
    df = pd.read_csv(csv_path)
    total_rows = len(df)
    print(f"Processing {total_rows} products...")
    
    query = """
    UNWIND $batch AS row
    MERGE (p:Product {id: row.product_id})
    SET p.category = row.product_category_name,
        p.category_english = row.product_category_name_english,
        p.name_length = toInteger(row.product_name_lenght),
        p.description_length = toInteger(row.product_description_lenght),
        p.photos_qty = toInteger(row.product_photos_qty),
        p.weight_g = toInteger(row.product_weight_g),
        p.length_cm = toInteger(row.product_length_cm),
        p.height_cm = toInteger(row.product_height_cm),
        p.width_cm = toInteger(row.product_width_cm),
        p.volume_cm3 = toFloat(row.product_volume_cm3),
        p.density = toFloat(row.product_density_g_per_cm3),
        p.complexity_score = toFloat(row.product_complexity_score),
        p.size_category = row.size_category,
        p.units_sold = toInteger(row.units_sold),
        p.total_revenue = toFloat(row.total_revenue),
        p.avg_price = toFloat(row.avg_price),
        p.avg_rating = toFloat(row.avg_product_rating)
    """
    
    for i in range(0, total_rows, BATCH_SIZE):
        batch = df.iloc[i:i+BATCH_SIZE].to_dict('records')
        neo4j_conn.execute_write(query, {'batch': batch})
        print(f"  Processed {min(i+BATCH_SIZE, total_rows)}/{total_rows} products")
    
    print(f"✓ Imported {total_rows} products\n")


def import_sellers(neo4j_conn):
    """Import sellers from CSV and create Seller nodes"""
    print("\n=== Importing Sellers ===")
    
    csv_path = os.path.join(get_data_path(), 'olist_sellers_dataset_enhanced_sample.csv')
    df = pd.read_csv(csv_path)
    total_rows = len(df)
    print(f"Processing {total_rows} sellers...")
    
    query = """
    UNWIND $batch AS row
    MERGE (s:Seller {id: row.seller_id})
    SET s.zip_code = row.seller_zip_code_prefix,
        s.city = row.seller_city,
        s.state = row.seller_state,
        s.total_orders = toInteger(row.total_orders),
        s.total_revenue = toFloat(row.total_revenue),
        s.avg_item_price = toFloat(row.avg_item_price),
        s.avg_rating = toFloat(row.avg_rating),
        s.tier = row.seller_tier,
        s.on_time_deliveries = toInteger(row.on_time_deliveries),
        s.on_time_rate = toFloat(row.on_time_delivery_rate)
    """
    
    for i in range(0, total_rows, BATCH_SIZE):
        batch = df.iloc[i:i+BATCH_SIZE].to_dict('records')
        neo4j_conn.execute_write(query, {'batch': batch})
        print(f"  Processed {min(i+BATCH_SIZE, total_rows)}/{total_rows} sellers")
    
    print(f"✓ Imported {total_rows} sellers\n")


def import_orders(neo4j_conn):
    """Import orders from CSV and create Order nodes + relationships to Customer"""
    print("\n=== Importing Orders ===")
    
    csv_path = os.path.join(get_data_path(), 'olist_orders_dataset_enhanced_sample.csv')
    df = pd.read_csv(csv_path)
    total_rows = len(df)
    print(f"Processing {total_rows} orders...")
    
    query = """
    UNWIND $batch AS row
    MERGE (o:Order {id: row.order_id})
    SET o.status = row.order_status,
        o.purchase_timestamp = row.order_purchase_timestamp,
        o.approved_at = row.order_approved_at,
        o.delivered_carrier_date = row.order_delivered_carrier_date,
        o.delivered_customer_date = row.order_delivered_customer_date,
        o.estimated_delivery_date = row.order_estimated_delivery_date,
        o.approval_to_delivery_days = toFloat(row.approval_to_delivery_days),
        o.estimated_delivery_days = toInteger(row.estimated_delivery_days),
        o.delivery_delay_days = toFloat(row.delivery_delay_days),
        o.item_total = toFloat(row.order_item_total),
        o.freight_total = toFloat(row.order_freight_total),
        o.total_with_freight = toFloat(row.order_total_with_freight),
        o.num_items = toFloat(row.num_items),
        o.review_score = toFloat(row.order_review_score),
        o.delivery_performance = row.delivery_performance,
        o.customer_segment = row.customer_segment
    
    WITH o, row
    MATCH (c:Customer {id: row.customer_id})
    MERGE (c)-[:PLACED]->(o)
    """
    
    for i in range(0, total_rows, BATCH_SIZE):
        batch = df.iloc[i:i+BATCH_SIZE].to_dict('records')
        neo4j_conn.execute_write(query, {'batch': batch})
        print(f"  Processed {min(i+BATCH_SIZE, total_rows)}/{total_rows} orders")
    
    print(f"✓ Imported {total_rows} orders with relationships\n")


def import_order_items(neo4j_conn):
    """Import order items and create relationships to Order, Product, and Seller"""
    print("\n=== Importing Order Items ===")
    
    csv_path = os.path.join(get_data_path(), 'olist_order_items_dataset_enhanced_sample.csv')
    df = pd.read_csv(csv_path)
    total_rows = len(df)
    print(f"Processing {total_rows} order items...")
    
    query = """
    UNWIND $batch AS row
    MERGE (oi:OrderItem {id: row.order_id + '_' + toString(row.order_item_id)})
    SET oi.order_item_id = toInteger(row.order_item_id),
        oi.shipping_limit_date = row.shipping_limit_date,
        oi.price = toFloat(row.price),
        oi.freight_value = toFloat(row.freight_value),
        oi.estimated_profit = toFloat(row.estimated_profit),
        oi.item_total = toFloat(row.item_total_with_freight),
        oi.price_category = row.price_category
    
    WITH oi, row
    MATCH (o:Order {id: row.order_id})
    MATCH (p:Product {id: row.product_id})
    MATCH (s:Seller {id: row.seller_id})
    MERGE (o)-[:CONTAINS]->(oi)
    MERGE (oi)-[:FOR_PRODUCT]->(p)
    MERGE (oi)-[:SOLD_BY]->(s)
    """
    
    for i in range(0, total_rows, BATCH_SIZE):
        batch = df.iloc[i:i+BATCH_SIZE].to_dict('records')
        neo4j_conn.execute_write(query, {'batch': batch})
        print(f"  Processed {min(i+BATCH_SIZE, total_rows)}/{total_rows} order items")
    
    print(f"✓ Imported {total_rows} order items with relationships\n")


def import_reviews(neo4j_conn):
    """Import reviews and create relationships to Order"""
    print("\n=== Importing Reviews ===")
    
    csv_path = os.path.join(get_data_path(), 'olist_order_reviews_dataset_enhanced_sample.csv')
    df = pd.read_csv(csv_path)
    total_rows = len(df)
    print(f"Processing {total_rows} reviews...")
    
    query = """
    UNWIND $batch AS row
    MERGE (r:Review {id: row.review_id})
    SET r.score = toInteger(row.review_score),
        r.comment_title = row.review_comment_title,
        r.comment_message = row.review_comment_message,
        r.creation_date = row.review_creation_date,
        r.answer_timestamp = row.review_answer_timestamp,
        r.sentiment = row.review_sentiment,
        r.has_comment = toBoolean(row.has_comment),
        r.response_time_hours = toFloat(row.response_time_hours)
    
    WITH r, row
    MATCH (o:Order {id: row.order_id})
    MERGE (o)-[:HAS_REVIEW]->(r)
    """
    
    for i in range(0, total_rows, BATCH_SIZE):
        batch = df.iloc[i:i+BATCH_SIZE].to_dict('records')
        neo4j_conn.execute_write(query, {'batch': batch})
        print(f"  Processed {min(i+BATCH_SIZE, total_rows)}/{total_rows} reviews")
    
    print(f"✓ Imported {total_rows} reviews with relationships\n")


def import_payments(neo4j_conn):
    """Import payments and create relationships to Order"""
    print("\n=== Importing Payments ===")
    
    csv_path = os.path.join(get_data_path(), 'olist_order_payments_dataset_enhanced_sample.csv')
    df = pd.read_csv(csv_path)
    total_rows = len(df)
    print(f"Processing {total_rows} payments...")
    
    query = """
    UNWIND $batch AS row
    MERGE (pay:Payment {id: row.order_id + '_' + toString(row.payment_sequential)})
    SET pay.sequential = toInteger(row.payment_sequential),
        pay.type = row.payment_type,
        pay.installments = toInteger(row.payment_installments),
        pay.value = toFloat(row.payment_value),
        pay.percentage = toFloat(row.payment_percentage),
        pay.method_category = row.payment_method_category,
        pay.installment_risk = row.installment_risk
    
    WITH pay, row
    MATCH (o:Order {id: row.order_id})
    MERGE (o)-[:PAID_WITH]->(pay)
    """
    
    for i in range(0, total_rows, BATCH_SIZE):
        batch = df.iloc[i:i+BATCH_SIZE].to_dict('records')
        neo4j_conn.execute_write(query, {'batch': batch})
        print(f"  Processed {min(i+BATCH_SIZE, total_rows)}/{total_rows} payments")
    
    print(f"✓ Imported {total_rows} payments with relationships\n")
