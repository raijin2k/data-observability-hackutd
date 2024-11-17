import pymongo
from elasticsearch import Elasticsearch
import redis
import psycopg2
from datetime import datetime
import logging
import sys
import os
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseInitializer:
    def __init__(self):
        load_dotenv()
        self.mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
        self.es_client = Elasticsearch(["http://localhost:9200"])
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0)
        self.timescale_conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="password",
            host="localhost",
            port="5432"
        )

    def cleanup_all(self):
        """Cleanup all existing data and structures"""
        logger.info("Starting cleanup process...")
        
        # Cleanup MongoDB
        try:
            self.mongo_client.drop_database('data_observability')
            logger.info("MongoDB cleanup completed")
        except Exception as e:
            logger.error(f"MongoDB cleanup failed: {str(e)}")

        # Cleanup Elasticsearch
        try:
            indices = ['data_creation', 'data_access', 'data_movement', 'data_usage']
            for index in indices:
                if self.es_client.indices.exists(index=index):
                    self.es_client.indices.delete(index=index)
            logger.info("Elasticsearch cleanup completed")
        except Exception as e:
            logger.error(f"Elasticsearch cleanup failed: {str(e)}")

        # Cleanup Redis
        try:
            self.redis_client.flushall()
            logger.info("Redis cleanup completed")
        except Exception as e:
            logger.error(f"Redis cleanup failed: {str(e)}")

        # Cleanup TimescaleDB
        try:
            cur = self.timescale_conn.cursor()
            cur.execute("DROP TABLE IF EXISTS data_movements CASCADE;")
            self.timescale_conn.commit()
            logger.info("TimescaleDB cleanup completed")
        except Exception as e:
            logger.error(f"TimescaleDB cleanup failed: {str(e)}")
        finally:
            cur.close()

    def init_mongodb(self):
        """Initialize MongoDB collections"""
        try:
            db = self.mongo_client['data_observability']
            
            # Create collections with schemas
            db.create_collection('data_creation', validator={
                '$jsonSchema': {
                    'bsonType': 'object',
                    'required': ['timestamp', 'data_id', 'source'],
                    'properties': {
                        'timestamp': {'bsonType': 'date'},
                        'data_id': {'bsonType': 'string'},
                        'source': {'bsonType': 'string'},
                        'metadata': {'bsonType': 'object'}
                    }
                }
            })
            
            # Create indexes
            db.data_creation.create_index([('timestamp', pymongo.ASCENDING)])
            db.data_creation.create_index([('data_id', pymongo.ASCENDING)])
            
            logger.info("MongoDB initialization completed successfully")
        except Exception as e:
            logger.error(f"MongoDB initialization failed: {str(e)}")
            raise

    def init_elasticsearch(self):
        """Initialize Elasticsearch indices"""
        try:
            indices = ['data_creation', 'data_access', 'data_movement', 'data_usage']
            
            for index in indices:
                if not self.es_client.indices.exists(index=index):
                    self.es_client.indices.create(
                        index=index,
                        body={
                            "mappings": {
                                "properties": {
                                    "timestamp": {"type": "date"},
                                    "data_id": {"type": "keyword"},
                                    "source": {"type": "keyword"},
                                    "user_id": {"type": "keyword"},
                                    "action": {"type": "keyword"},
                                    "metadata": {"type": "object"}
                                }
                            }
                        }
                    )
            
            logger.info("Elasticsearch initialization completed successfully")
        except Exception as e:
            logger.error(f"Elasticsearch initialization failed: {str(e)}")
            raise

    def init_timescaledb(self):
        """Initialize TimescaleDB tables"""
        try:
            cur = self.timescale_conn.cursor()
            
            # Create the base table with proper primary key
            cur.execute("""
                CREATE TABLE IF NOT EXISTS data_movements (
                    id SERIAL,
                    timestamp TIMESTAMPTZ NOT NULL,
                    data_id VARCHAR(50),
                    source VARCHAR(100),
                    destination VARCHAR(100),
                    status VARCHAR(20),
                    metadata JSONB,
                    PRIMARY KEY (id, timestamp)
                );
            """)
            
            # Create hypertable
            cur.execute("""
                SELECT create_hypertable('data_movements', 'timestamp', 
                    if_not_exists => TRUE,
                    migrate_data => TRUE
                );
            """)
            
            # Create index including timestamp
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_data_movements_lookup 
                ON data_movements(data_id, timestamp DESC);
            """)
            
            self.timescale_conn.commit()
            logger.info("TimescaleDB initialization completed successfully")
        except Exception as e:
            logger.error(f"TimescaleDB initialization failed: {str(e)}")
            raise
        finally:
            cur.close()

    def cleanup_connections(self):
        """Close all database connections"""
        try:
            self.mongo_client.close()
            self.timescale_conn.close()
            logger.info("Database connections closed successfully")
        except Exception as e:
            logger.error(f"Error closing database connections: {str(e)}")

def main():
    initializer = DatabaseInitializer()
    try:
        # Ask for confirmation before cleanup
        response = input("Do you want to clean up existing data? (yes/no): ").lower()
        if response == 'yes':
            initializer.cleanup_all()
        
        logger.info("Starting database initialization...")
        initializer.init_mongodb()
        initializer.init_elasticsearch()
        initializer.init_timescaledb()
        logger.info("Database initialization completed successfully")
        
    except Exception as e:
        logger.error(f"Database initialization failed: {str(e)}")
        sys.exit(1)
    finally:
        initializer.cleanup_connections()

if __name__ == "__main__":
    main()