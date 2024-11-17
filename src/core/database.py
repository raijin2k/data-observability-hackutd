from elasticsearch import Elasticsearch
import pymongo
import redis
import psycopg2
from datetime import datetime, timedelta
import pandas as pd
import json
from .config import Config

class DatabaseManager:
    def __init__(self):
        self.mongo_client = pymongo.MongoClient(Config.MONGO_URI)
        self.db = self.mongo_client['data_observability']
        self.es_client = Elasticsearch([Config.ES_HOST])
        self.redis_client = redis.Redis(
            host=Config.REDIS_HOST, 
            port=Config.REDIS_PORT, 
            db=0
        )
        self.timescale_conn = psycopg2.connect(**Config.TIMESCALE_CONFIG)

    def get_creation_metrics(self, start_date, end_date):
        """Get data creation metrics from MongoDB"""
        try:
            pipeline = [
                {
                    "$match": {
                        "timestamp": { "$gte": start_date, "$lte": end_date }
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$timestamp"}},
                            "hour": {"$hour": "$timestamp"},
                            "source": "$source"
                        },
                        "count": {"$sum": 1}
                    }
                },
                {"$sort": {"_id.date": 1, "_id.hour": 1}}
            ]
            
            results = list(self.db.data_creation.aggregate(pipeline))
            
            # Process the results into a more usable format
            df_data = []
            by_source = {}
            by_hour = {}
            
            for r in results:
                hour = r['_id']['hour']
                source = r['_id']['source']
                count = r['count']
                date = r['_id']['date']
                
                df_data.append({
                    'date': date,
                    'hour': hour,
                    'source': source,
                    'count': count
                })
                
                # Aggregate by source
                by_source[source] = by_source.get(source, 0) + count
                
                # Aggregate by hour
                by_hour[hour] = by_hour.get(hour, 0) + count
            
            return {
                'total_count': sum(by_source.values()),
                'by_source': by_source,
                'by_hour': by_hour,
                'trend_data': df_data
            }
        except Exception as e:
            print(f"Error getting creation metrics: {str(e)}")
            return None
        
    def get_movement_data(self, start_date, end_date):
        """Get data movement tracking from TimescaleDB"""
        try:
            query = """
                SELECT 
                    time_bucket('1 hour', timestamp) AS hour,
                    source,
                    destination,
                    status,
                    COUNT(*) as movement_count
                FROM data_movements
                WHERE timestamp BETWEEN %s AND %s
                GROUP BY hour, source, destination, status
                ORDER BY hour;
            """
            
            with self.timescale_conn.cursor() as cur:
                cur.execute(query, (start_date, end_date))
                columns = [desc[0] for desc in cur.description]
                results = [dict(zip(columns, row)) for row in cur.fetchall()]
                
                return {
                    'movements': results,
                    'summary': {
                        'total_movements': sum(r['movement_count'] for r in results),
                        'by_status': pd.DataFrame(results)['status'].value_counts().to_dict(),
                        'by_source': pd.DataFrame(results)['source'].value_counts().to_dict()
                    }
                }
        except Exception as e:
            print(f"Error getting movement data: {str(e)}")
            return None

    def get_access_patterns(self, start_date, end_date):
        """Get access patterns from Elasticsearch"""
        try:
            query = {
                "query": {
                    "range": {
                        "timestamp": {
                            "gte": start_date.isoformat(),
                            "lte": end_date.isoformat()
                        }
                    }
                },
                "aggs": {
                    "access_by_hour": {
                        "date_histogram": {
                            "field": "timestamp",
                            "fixed_interval": "1h"  # Changed from interval to fixed_interval
                        }
                    },
                    "access_by_user": {
                        "terms": {
                            "field": "user_id.keyword"
                        }
                    },
                    "access_by_action": {
                        "terms": {
                            "field": "action.keyword"
                        }
                    }
                }
            }
            
            results = self.es_client.search(
                index="data_access",
                body=query,
                size=0
            )
            
            return {
                'by_hour': {
                    bucket['key_as_string']: bucket['doc_count']
                    for bucket in results['aggregations']['access_by_hour']['buckets']
                },
                'by_user': {
                    bucket['key']: bucket['doc_count']
                    for bucket in results['aggregations']['access_by_user']['buckets']
                },
                'by_action': {
                    bucket['key']: bucket['doc_count']
                    for bucket in results['aggregations']['access_by_action']['buckets']
                }
            }
        except Exception as e:
            print(f"Error getting access patterns: {str(e)}")
            return None

    def get_usage_analytics(self, start_date, end_date):
        """Get usage analytics combining data from all sources"""
        try:
            # Get real-time metrics from Redis
            current_metrics = {
                'total_records': int(self.redis_client.get('usage:total_records') or 0),
                'by_source': {
                    k.decode(): int(v.decode())
                    for k, v in self.redis_client.hgetall('usage:by_source').items()
                }
            }
            
            # Get historical metrics from Elasticsearch
            es_query = {
                "query": {
                    "range": {
                        "timestamp": {
                            "gte": start_date.isoformat(),
                            "lte": end_date.isoformat()
                        }
                    }
                },
                "aggs": {
                    "usage_over_time": {
                        "date_histogram": {
                            "field": "timestamp",
                            "fixed_interval": "1h"
                        }
                    }
                }
            }
            
            es_results = self.es_client.search(
                index="data_access",  # Changed from data_usage to data_access
                body=es_query,
                size=0
            )
            
            return {
                'current_metrics': current_metrics,
                'historical_metrics': {
                    'over_time': {
                        bucket['key_as_string']: bucket['doc_count']
                        for bucket in es_results['aggregations']['usage_over_time']['buckets']
                    }
                }
            }
        except Exception as e:
            print(f"Error getting usage analytics: {str(e)}")
            return None

    def record_access(self, user_id, data_id, action):
        """Record a data access event"""
        try:
            access_record = {
                "timestamp": datetime.utcnow(),
                "user_id": user_id,
                "data_id": data_id,
                "action": action
            }
            
            # Store in Elasticsearch
            self.es_client.index(
                index="data_access",
                document=access_record
            )
            
            # Update Redis counters
            self.redis_client.incr(f"access_count:{data_id}")
            self.redis_client.incr(f"user_access_count:{user_id}")
            
            return True
        except Exception as e:
            print(f"Error recording access: {str(e)}")
            return False

    def record_movement(self, data_id, source, destination):
        """Record a data movement event"""
        try:
            with self.timescale_conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO data_movements 
                    (timestamp, data_id, source, destination, status)
                    VALUES (%s, %s, %s, %s, %s)
                """, (datetime.utcnow(), data_id, source, destination, 'completed'))
            
            self.timescale_conn.commit()
            return True
        except Exception as e:
            print(f"Error recording movement: {str(e)}")
            return False

    def close(self):
        """Close all database connections"""
        try:
            self.mongo_client.close()
            self.timescale_conn.close()
            self.redis_client.close()
        except Exception as e:
            print(f"Error closing connections: {str(e)}")

class SimpleAnomalyDetector:
    def __init__(self):
        # Relative multipliers for thresholds
        self.multipliers = {
            'work_hours': range(9, 18),  # 9 AM to 6 PM
            'work_hours_threshold': 1.1, # 25% above average
            'off_hours_threshold': 0.9,   # 30% below average
            'high_load': 1.1,             # 50% above average - needs attention
            'low_load': 0.9               # 50% below average - potential resource waste
        }

    def analyze_load_patterns(self, hourly_data: dict) -> dict:
        """
        Analyze load patterns using dynamic thresholds based on averages
        """
        if not hourly_data:
            return None
            
        # Calculate average loads
        values = list(hourly_data.values())
        avg_load = sum(values) / len(values)
        
        # Calculate separate averages for work/off hours
        work_hours_values = [
            hourly_data[str(hour)] 
            for hour in self.multipliers['work_hours'] 
            if str(hour) in hourly_data
        ]
        
        off_hours_values = [
            hourly_data[str(hour)] 
            for hour in range(24) 
            if hour not in self.multipliers['work_hours'] 
            and str(hour) in hourly_data
        ]
        
        work_hours_avg = sum(work_hours_values) / len(work_hours_values) if work_hours_values else avg_load
        off_hours_avg = sum(off_hours_values) / len(off_hours_values) if off_hours_values else avg_load

        # Analyze each hour
        load_patterns = []
        for hour, count in hourly_data.items():
            hour = int(hour)
            
            # Determine baseline for this hour
            is_work_hour = hour in self.multipliers['work_hours']
            baseline = work_hours_avg if is_work_hour else off_hours_avg
            expected_threshold = (baseline * self.multipliers['work_hours_threshold'] 
                                if is_work_hour 
                                else baseline * self.multipliers['off_hours_threshold'])
            
            # Calculate load ratio against overall average
            load_ratio = count / avg_load
            
            # Determine load status
            if load_ratio >= self.multipliers['high_load']:
                status = 'high'
                action = 'scale_up'
            elif load_ratio <= self.multipliers['low_load']:
                status = 'low'
                action = 'scale_down'
            else:
                status = 'normal'
                action = 'maintain'
                
            load_patterns.append({
                'hour': hour,
                'count': count,
                'load_ratio': load_ratio,
                'status': status,
                'action': action,
                'is_work_hour': is_work_hour,
                'expected_threshold': expected_threshold
            })
        
        # Sort by count for easy identification of peaks
        load_patterns.sort(key=lambda x: x['count'], reverse=True)
        
        return {
            'patterns': load_patterns,
            'summary': {
                'average_load': avg_load,
                'work_hours_avg': work_hours_avg,
                'off_hours_avg': off_hours_avg,
                'peak_hours': load_patterns[:3],  # Top 3 busiest hours
                'quiet_hours': load_patterns[-3:],  # 3 quietest hours
                'high_load_hours': sum(1 for p in load_patterns if p['status'] == 'high'),
                'low_load_hours': sum(1 for p in load_patterns if p['status'] == 'low')
            }
        }