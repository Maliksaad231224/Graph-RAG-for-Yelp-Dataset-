"""
Test Neo4j Connection
Quick script to verify Neo4j connectivity before running full import
"""
import os
import sys
from dotenv import load_dotenv
from neo4j import GraphDatabase

# Load environment variables
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

def test_connection():
    uri = os.getenv("NEO4J_URI")
    username = os.getenv("NEO4J_USERNAME")
    password = os.getenv("NEO4J_PASSWORD")
    
    print(f"Testing connection to: {uri}")
    print(f"Username: {username}")
    print(f"Password: {'*' * len(password)}")
    
    try:
        driver = GraphDatabase.driver(uri, auth=(username, password))
        driver.verify_connectivity()
        
        # Test a simple query
        with driver.session() as session:
            result = session.run("RETURN 'Connection successful!' as message")
            message = result.single()["message"]
            print(f"\n✓ {message}")
            print("✓ Neo4j is ready for import!")
        
        driver.close()
        return True
        
    except Exception as e:
        print(f"\n✗ Connection failed: {e}")
        print("\nPossible solutions:")
        print("1. Check if Neo4j AuraDB instance is running at https://console.neo4j.io")
        print("2. Verify credentials in .env file")
        print("3. Wait 60 seconds after creating the instance")
        print("4. Check your internet connection")
        return False

if __name__ == "__main__":
    success = test_connection()
    sys.exit(0 if success else 1)
