db = db.getSiblingDB('search_engine');

db.createUser({
  user: 'admin',
  pwd: 'admin123',
  roles: [
    { role: 'readWrite', db: 'search_engine' },
    { role: 'dbAdmin', db: 'search_engine' }
  ]
});

db.createCollection('documents');

db.documents.createIndex({ "normalized_url": 1 }, { unique: true });
db.documents.createIndex({ "updated_at": 1 });
db.documents.createIndex({ "status": 1 });
db.documents.createIndex({ "content_hash": 1 });