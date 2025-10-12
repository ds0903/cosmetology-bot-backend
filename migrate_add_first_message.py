"""
Migration script to add is_first_message column to dialogues table
"""
from app.database import engine
from sqlalchemy import text
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def migrate():
    """Add is_first_message column to dialogues table"""
    with engine.connect() as conn:
        try:
            # Check if column already exists
            result = conn.execute(text("""
                SELECT COUNT(*) 
                FROM information_schema.columns 
                WHERE table_name='dialogues' 
                AND column_name='is_first_message'
            """))
            
            if result.scalar() > 0:
                logger.info("Column is_first_message already exists, skipping migration")
                return
            
            # Add the column
            logger.info("Adding is_first_message column to dialogues table...")
            conn.execute(text("""
                ALTER TABLE dialogues 
                ADD COLUMN is_first_message BOOLEAN DEFAULT FALSE
            """))
            conn.commit()
            
            logger.info("Successfully added is_first_message column")
            
        except Exception as e:
            logger.error(f"Error during migration: {e}")
            conn.rollback()
            raise

if __name__ == "__main__":
    migrate()
    logger.info("Migration completed!")
