# Olist E-commerce Knowledge Graph

This project creates a Knowledge Graph from the Olist e-commerce dataset using Neo4j.

## Project Structure

```
Graph-RAG-for-Yelp-Dataset/
├── Data Preprocessing/          # CSV datasets
├── neo4j_import/               # Import scripts
│   ├── config.py              # Neo4j connection
│   ├── constraints.py         # Database constraints
│   ├── importers.py           # Import functions
│   └── main.py               # Main orchestrator
├── .env                       # Neo4j credentials
└── requirements.txt           # Python dependencies
```

## Setup

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure Neo4j credentials:**
   - Credentials are already set in `.env` file
   - Make sure your Neo4j AuraDB instance is running

3. **Run the import:**
   ```bash
   cd neo4j_import
   python main.py
   ```

## What Gets Imported

### Nodes:
- **Customer** (99,443 nodes) - Customer profiles with segments
- **Product** (32,953 nodes) - Product catalog with attributes
- **Seller** (3,097 nodes) - Seller profiles with tiers
- **Order** (99,443 nodes) - Order transactions
- **OrderItem** (112,652 nodes) - Individual items in orders
- **Review** (104,721 nodes) - Customer reviews
- **Payment** (103,888 nodes) - Payment transactions

### Relationships:
- `(Customer)-[:PLACED]->(Order)` - Customer placed an order
- `(Order)-[:CONTAINS]->(OrderItem)` - Order contains items
- `(OrderItem)-[:FOR_PRODUCT]->(Product)` - Item is for a product
- `(OrderItem)-[:SOLD_BY]->(Seller)` - Item sold by seller
- `(Order)-[:HAS_REVIEW]->(Review)` - Order has a review
- `(Order)-[:PAID_WITH]->(Payment)` - Order paid with payment method

## Import Details

- **Batch Processing:** 5,000 rows per batch
- **Total Records:** ~556,000 nodes + relationships
- **Estimated Time:** 10-15 minutes
- **Database:** Neo4j AuraDB (cloud)

## Verification

After import, the script displays:
- Node counts by type
- Relationship counts by type
- Total execution time

## Sample Queries

Once imported, you can run queries like:

```cypher
// Find top customers by spending
MATCH (c:Customer)
RETURN c.id, c.total_spending, c.segment
ORDER BY c.total_spending DESC
LIMIT 10

// Find products with best ratings
MATCH (p:Product)
WHERE p.avg_rating > 4.5
RETURN p.category_english, p.avg_rating, p.total_revenue
ORDER BY p.total_revenue DESC
LIMIT 10

// Analyze seller performance
MATCH (s:Seller)
RETURN s.tier, s.avg_rating, s.on_time_rate
ORDER BY s.total_revenue DESC
LIMIT 10
```
