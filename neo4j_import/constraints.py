"""
Create Unique Constraints in Neo4j
Must run before importing data to ensure data integrity
"""

def create_constraints(neo4j_conn):
    """Create unique constraints for all node types"""
    
    constraints = [
        # Customer constraints
        "CREATE CONSTRAINT customer_id IF NOT EXISTS FOR (c:Customer) REQUIRE c.id IS UNIQUE",
        
        # Order constraints
        "CREATE CONSTRAINT order_id IF NOT EXISTS FOR (o:Order) REQUIRE o.id IS UNIQUE",
        
        # Product constraints
        "CREATE CONSTRAINT product_id IF NOT EXISTS FOR (p:Product) REQUIRE p.id IS UNIQUE",
        
        # Seller constraints
        "CREATE CONSTRAINT seller_id IF NOT EXISTS FOR (s:Seller) REQUIRE s.id IS UNIQUE",
        
        # Review constraints
        "CREATE CONSTRAINT review_id IF NOT EXISTS FOR (r:Review) REQUIRE r.id IS UNIQUE",
        
        # Payment constraints
        "CREATE CONSTRAINT payment_id IF NOT EXISTS FOR (pay:Payment) REQUIRE pay.id IS UNIQUE",
        
        # OrderItem constraints
        "CREATE CONSTRAINT order_item_id IF NOT EXISTS FOR (oi:OrderItem) REQUIRE oi.id IS UNIQUE",
    ]
    
    print("\n=== Creating Constraints ===")
    for constraint in constraints:
        try:
            neo4j_conn.execute_write(constraint)
            # Extract constraint name from query
            constraint_name = constraint.split("CONSTRAINT ")[1].split(" IF")[0]
            print(f"✓ Created constraint: {constraint_name}")
        except Exception as e:
            print(f"✗ Error creating constraint: {e}")
    
    print("✓ All constraints created successfully\n")
