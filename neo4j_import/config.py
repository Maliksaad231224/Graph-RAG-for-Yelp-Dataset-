"""
Neo4j Database Configuration
Simple connection setup for Neo4j AuraDB
"""
import os
from neo4j import GraphDatabase
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Neo4jConnection:
    def __init__(self):
        self.uri = os.getenv("NEO4J_URI")
        self.username = os.getenv("NEO4J_USERNAME")
        self.password = os.getenv("NEO4J_PASSWORD")
        self.database = os.getenv("NEO4J_DATABASE", "neo4j")
        self.driver = None
        
    def connect(self):
        """Establish connection to Neo4j"""
        try:
            self.driver = GraphDatabase.driver(
                self.uri,
                auth=(self.username, self.password)
            )
            # Test connection
            self.driver.verify_connectivity()
            print(f"✓ Connected to Neo4j at {self.uri}")
            return self.driver
        except Exception as e:
            print(f"✗ Failed to connect to Neo4j: {e}")
            raise
    
    def close(self):
        """Close the Neo4j connection"""
        if self.driver:
            self.driver.close()
            print("✓ Neo4j connection closed")
    
    def execute_query(self, query, parameters=None):
        """Execute a single Cypher query"""
        with self.driver.session(database=self.database) as session:
            result = session.run(query, parameters or {})
            return result.data()
    
    def execute_write(self, query, parameters=None):
        """Execute a write transaction"""
        with self.driver.session(database=self.database) as session:
            result = session.execute_write(
                lambda tx: tx.run(query, parameters or {}).consume()
            )
            return result
