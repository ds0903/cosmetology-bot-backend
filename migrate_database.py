#!/usr/bin/env python3
"""
Database migration script to add zip_history columns
"""

import sys
import os

# Add the app directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

from sqlalchemy import text
from app.database import engine, SessionLocal
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def migrate_database():
    """Add zip_history and last_compression_at columns to client_last_activity table"""
    
    db = SessionLocal()
    
    try:
        logger.info("Starting database migration...")
        
        # Check if columns already exist
        check_zip_history = text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'client_last_activity' 
            AND column_name = 'zip_history'
        """)
        
        result = db.execute(check_zip_history).fetchone()
        
        if result:
            logger.info("zip_history column already exists, skipping migration")
            return
        
        # Add the new columns
        logger.info("Adding zip_history column to client_last_activity table...")
        db.execute(text("""
            ALTER TABLE client_last_activity 
            ADD COLUMN zip_history TEXT NULL
        """))
        
        logger.info("Adding last_compression_at column to client_last_activity table...")
        db.execute(text("""
            ALTER TABLE client_last_activity 
            ADD COLUMN last_compression_at TIMESTAMP NULL
        """))
        
        db.commit()
        logger.info("✅ Database migration completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Migration failed: {e}")
        db.rollback()
        raise
    finally:
        db.close()

if __name__ == "__main__":
    print("🔧 Database Migration Script")
    print("This will add zip_history and last_compression_at columns")
    
    try:
        migrate_database()
        print("🎉 Migration completed!")
    except Exception as e:
        print(f"💥 Migration failed: {e}")
        sys.exit(1)
