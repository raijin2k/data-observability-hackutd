import pymongo
from elasticsearch import Elasticsearch
import redis
import psycopg2
from datetime import datetime, timedelta
import random
import uuid
import json

class DataPopulator:
    def __init__(self):
        self.mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
        self.db = self.mongo_client['data_observability']
        self.es_client = Elasticsearch(["http://localhost:9200"])
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0)
        self.timescale_conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="password",
            host="localhost",
            port="5432"
        )

    def generate_sample_data(self, days=7):
        """Generate sample data for the last n days"""
        try:
            start_date = datetime.now() - timedelta(days=days)
            print(f"Generating sample data from {start_date} to {datetime.now()}")
            
            # Sample data parameters
            sources = ['web', 'mobile', 'api', 'batch']
            users = [f'user_{i}' for i in range(1, 6)]
            actions = ['view', 'edit', 'delete', 'create']
            statuses = ['completed', 'in_progress', 'failed']
            data_types = ['user_profile', 'transaction', 'report', 'analytics']
            departments = ['sales', 'marketing', 'finance', 'operations']
            
            # Clear existing Redis data
            print("Clearing existing Redis data...")
            self.redis_client.flushall()
            
            # Initialize Redis counters
            self.redis_client.set('usage:total_records', '0')
            self.redis_client.hset('usage:by_source', mapping={
                'web': '0',
                'mobile': '0',
                'api': '0',
                'batch': '0'
            })
            self.redis_client.hset('usage:by_type', mapping={
                'read': '0',
                'write': '0',
                'delete': '0',
                'update': '0'
            })
            
            # Generate data for each day
            current_date = start_date
            records_created = 0
            
            while current_date < datetime.now():
                # Generate different patterns for different hours
                for hour in range(24):
                    # Simulate busier hours during workday
                    if 9 <= hour <= 17:
                        num_records = random.randint(15, 30)  # Busier during work hours
                    else:
                        num_records = random.randint(5, 15)   # Less busy outside work hours
                    
                    timestamp = current_date + timedelta(hours=hour)
                    
                    # Generate records for this hour
                    for _ in range(num_records):
                        data_id = str(uuid.uuid4())
                        source = random.choice(sources)
                        user = random.choice(users)
                        action = random.choice(actions)
                        data_type = random.choice(data_types)
                        department = random.choice(departments)
                        
                        # Add some randomness to data size
                        data_size = random.randint(100, 10000)
                        
                        # MongoDB - Creation events
                        creation_record = {
                            "timestamp": timestamp,
                            "data_id": data_id,
                            "source": source,
                            "metadata": {
                                "type": data_type,
                                "size": data_size,
                                "department": department,
                                "creator": user
                            }
                        }
                        self.db.data_creation.insert_one(creation_record)
                        
                        # Elasticsearch - Access events
                        access_record = {
                            "timestamp": timestamp.isoformat(),
                            "data_id": data_id,
                            "user_id": user,
                            "action": action,
                            "source": source,
                            "metadata": {
                                "type": data_type,
                                "department": department,
                                "access_point": random.choice(['dashboard', 'api', 'mobile_app', 'web_portal'])
                            }
                        }
                        self.es_client.index(
                            index="data_access",
                            document=access_record
                        )
                        
                        # TimescaleDB - Movement events
                        dest_source = random.choice([s for s in sources if s != source])
                        status = random.choice(statuses)
                        
                        with self.timescale_conn.cursor() as cur:
                            cur.execute("""
                                INSERT INTO data_movements 
                                (timestamp, data_id, source, destination, status, metadata)
                                VALUES (%s, %s, %s, %s, %s, %s)
                            """, (
                                timestamp,
                                data_id,
                                source,
                                dest_source,
                                status,
                                json.dumps({
                                    "type": data_type,
                                    "size": data_size,
                                    "department": department,
                                    "transfer_method": random.choice(['sync', 'async', 'batch'])
                                })
                            ))
                        
                        # Redis - Current metrics
                        self.redis_client.incr('usage:total_records')
                        self.redis_client.hincrby('usage:by_source', source, 1)
                        
                        # Map actions to types for Redis metrics
                        action_type_map = {
                            'view': 'read',
                            'edit': 'write',
                            'delete': 'delete',
                            'create': 'write'
                        }
                        self.redis_client.hincrby('usage:by_type', action_type_map[action], 1)
                        
                        records_created += 1
                        
                    # Commit TimescaleDB transactions for each hour
                    self.timescale_conn.commit()
                    
                    # Print progress
                    if records_created % 100 == 0:
                        print(f"Created {records_created} records... Current timestamp: {timestamp}")
                
                current_date += timedelta(days=1)
            
            print(f"\nData generation completed:")
            print(f"- Total records created: {records_created}")
            print(f"- Date range: {start_date} to {datetime.now()}")
            print(f"- Sources used: {sources}")
            print(f"- Data types: {data_types}")
            print(f"- Users: {users}")
            
            # Verify data
            print("\nVerifying data counts:")
            print(f"MongoDB records: {self.db.data_creation.count_documents({})}")
            print(f"Redis total records: {self.redis_client.get('usage:total_records').decode()}")
            print("Redis source distribution:", {
                k.decode(): v.decode() 
                for k, v in self.redis_client.hgetall('usage:by_source').items()
            })
            print("Redis type distribution:", {
                k.decode(): v.decode() 
                for k, v in self.redis_client.hgetall('usage:by_type').items()
            })
            
        except Exception as e:
            print(f"Error generating sample data: {str(e)}")
            raise

def main():
    populator = DataPopulator()
    populator.generate_sample_data()

if __name__ == "__main__":
    main()