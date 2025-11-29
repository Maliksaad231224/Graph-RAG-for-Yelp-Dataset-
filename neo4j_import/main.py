"""
Main Orchestrator for Neo4j Knowledge Graph Import
Runs all import steps in the correct order
"""
import time
from config import Neo4jConnection
from constraints import create_constraints
from importers import (
    import_customers,
    import_products,
    import_sellers,
    import_orders,
    import_order_items,
    import_reviews,
    import_payments
)


def verify_import(neo4j_conn):
    """Verify the import by counting nodes and relationships"""
    print("\n=== Verifying Import ===")
    
    # Count nodes
    node_counts = neo4j_conn.execute_query("""
        MATCH (n)
        RETURN labels(n)[0] as label, count(n) as count
        ORDER BY label
    """)
    
    print("\nNode Counts:")
    total_nodes = 0
    for record in node_counts:
        count = record['count']
        total_nodes += count
        print(f"  {record['label']}: {count:,}")
    print(f"  TOTAL: {total_nodes:,}")
    
    # Count relationships
    rel_counts = neo4j_conn.execute_query("""
        MATCH ()-[r]->()
        RETURN type(r) as relationship, count(r) as count
        ORDER BY relationship
    """)
    
    print("\nRelationship Counts:")
    total_rels = 0
    for record in rel_counts:
        count = record['count']
        total_rels += count
        print(f"  {record['relationship']}: {count:,}")
    print(f"  TOTAL: {total_rels:,}")
    
    print("\n✓ Verification complete\n")


def main():
    """Main execution function"""
    start_time = time.time()
    
    print("="*60)
    print("  Olist E-commerce Knowledge Graph Import")
    print("="*60)
    
    # Connect to Neo4j
    neo4j = Neo4jConnection()
    
    try:
        # Establish connection
        neo4j.connect()
        
        # Create constraints
        create_constraints(neo4j)
        
        # Import data in order (dimension tables first, then fact tables)
        import_customers(neo4j)
        import_products(neo4j)
        import_sellers(neo4j)
        import_orders(neo4j)
        import_order_items(neo4j)
        import_reviews(neo4j)
        import_payments(neo4j)
        
        # Verify import
        verify_import(neo4j)
        
        # Calculate execution time
        elapsed_time = time.time() - start_time
        minutes = int(elapsed_time // 60)
        seconds = int(elapsed_time % 60)
        
        print("="*60)
        print(f"✓ Import completed successfully in {minutes}m {seconds}s")
        print("="*60)
        
    except Exception as e:
        print(f"\n✗ Error during import: {e}")
        raise
    
    finally:
        # Close connection
        neo4j.close()


if __name__ == "__main__":
    main()
