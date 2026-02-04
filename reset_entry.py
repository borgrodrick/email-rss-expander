import sqlite3
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_PATH = "articles.db"
ENTRY_ID = "urn:kill-the-newsletter:usj34hqxf8ounvnzj4qd"

def reset_entry():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # Enable foreign key support to be safe, though we will delete manually
        cursor.execute("PRAGMA foreign_keys = ON")
        
        # 1. Delete associated articles first
        cursor.execute("DELETE FROM articles WHERE feed_entry_id = ?", (ENTRY_ID,))
        articles_deleted = cursor.rowcount
        logger.info(f"Deleted {articles_deleted} articles associated with {ENTRY_ID}")
        
        # 2. Delete the entry record
        cursor.execute("DELETE FROM entries WHERE entry_id = ?", (ENTRY_ID,))
        entries_deleted = cursor.rowcount
        if entries_deleted > 0:
            logger.info(f"Successfully deleted entry record: {ENTRY_ID}")
        else:
            logger.warning(f"Entry not found in entries table: {ENTRY_ID}")
            
        conn.commit()
        
    except Exception as e:
        logger.error(f"Error resetting entry: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    reset_entry()
