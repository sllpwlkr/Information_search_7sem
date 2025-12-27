from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError
import hashlib
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class Database:
    def __init__(self, config):
        db_config = config['db']
        self.client = MongoClient(
            host=db_config['host'],
            port=db_config['port'],
            username=db_config.get('username', 'admin'),
            password=db_config.get('password', 'admin123'),
            authSource=db_config.get('database', 'search_engine')
        )
        self.db = self.client[db_config['database']]
        self.collection = self.db[db_config['collection']]
        
        logger.info(f"Подключение к MongoDB: {db_config['host']}:{db_config['port']}")
    
    def _create_indexes(self):
        self.collection.create_index([("normalized_url", ASCENDING)], unique=True)
        self.collection.create_index([("updated_at", ASCENDING)])
        self.collection.create_index([("status", ASCENDING)])
        self.collection.create_index([("crawled_at", ASCENDING)])
        logger.info("Индексы созданы")
    
    def compute_hash(self, text):
        return hashlib.md5(text.encode('utf-8')).hexdigest()
    
    def normalize_url(self, url):
        return url.split('#')[0].split('?')[0]
    
    def document_exists(self, normalized_url):
        return self.collection.count_documents(
            {"normalized_url": normalized_url}
        ) > 0
    
    def get_document(self, normalized_url):
        return self.collection.find_one({"normalized_url": normalized_url})
    
    def save_document(self, data):
        normalized_url = data['normalized_url']
        existing = self.get_document(normalized_url)
        
        if existing:
            update_data = {
                "$set": {
                    "raw_html": data.get('raw_html'),
                    "clean_text": data.get('clean_text'),
                    "updated_at": data['updated_at'],
                    "content_hash": data.get('content_hash'),
                    "metadata": data.get('metadata', {})
                }
            }
            result = self.collection.update_one(
                {"_id": existing["_id"]},
                update_data
            )
            logger.debug(f"Документ обновлен: {normalized_url}")
            return result.modified_count > 0
        else:
            try:
                result = self.collection.insert_one(data)
                logger.debug(f"Документ сохранен: {normalized_url}")
                return result.inserted_id
            except DuplicateKeyError:
                logger.warning(f"Документ уже существует: {normalized_url}")
                return None
    
    def get_documents_for_revisit(self, interval_days):
        import time
        cutoff_time = time.time() - (interval_days * 86400)
        
        return list(self.collection.find({
            "updated_at": {"$lt": cutoff_time}
        }))
    
    def get_visited_urls(self, limit=10000):
        return list(self.collection.find(
            {},
            {"normalized_url": 1, "_id": 0}
        ).limit(limit))
    
    def get_statistics(self):
        total = self.collection.count_documents({})
        processed = self.collection.count_documents({"status": "processed"})
        
        return {
            "total_documents": total,
            "processed_documents": processed,
            "pending_documents": total - processed
        }
    
    def close(self):
        self.client.close()
        logger.info("Подключение к MongoDB закрыто")