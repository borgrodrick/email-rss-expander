import sqlite3
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

DB_PATH = "articles.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Table to track processed feed entries to avoid re-processing
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS entries (
            entry_id TEXT PRIMARY KEY,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Table to store crawled articles
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            feed_entry_id TEXT,
            email_source TEXT,
            article_source_domain TEXT,
            title TEXT,
            content TEXT,
            summary TEXT,
            tags TEXT,
            image_url TEXT,
            original_link TEXT UNIQUE,
            content_hash TEXT,
            published_date TEXT,
            feed_source_date TEXT,
            author TEXT,
            reading_time INTEGER,
            crawl_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(feed_entry_id) REFERENCES entries(entry_id)
        )
    ''')
    
    # Table to store failed crawls to verify before retrying
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS failed_crawls (
            url TEXT PRIMARY KEY,
            error_code TEXT,
            attempted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Ensure index on url for fast lookups (though PRIMARY KEY implies it, explicit index ensures intent)
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_failed_url ON failed_crawls(url)')

    # Simple migration: Add feed_source_date if it doesn't exist
    try:
        cursor.execute("ALTER TABLE articles ADD COLUMN feed_source_date TEXT")
    except sqlite3.OperationalError:
        pass

    # Simple migration: Add author and reading_time if they don't exist
    try:
        cursor.execute("ALTER TABLE articles ADD COLUMN author TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        cursor.execute("ALTER TABLE articles ADD COLUMN reading_time INTEGER")
    except sqlite3.OperationalError:
        pass
    
    conn.commit()
    conn.close()
    logger.info(f"Database initialized at {DB_PATH}")

def is_crawl_failed(url):
    """Check if a URL has previously failed crawling."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    # Efficient lookup using the index/primary key
    cursor.execute("SELECT 1 FROM failed_crawls WHERE url = ?", (url,))
    exists = cursor.fetchone() is not None
    conn.close()
    return exists

def mark_crawl_failed(url, error_code):
    """Mark a URL as failed to prevent retries."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR REPLACE INTO failed_crawls (url, error_code) VALUES (?, ?)", 
        (url, str(error_code))
    )
    conn.commit()
    conn.close()

def entry_exists(entry_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM entries WHERE entry_id = ?", (entry_id,))
    exists = cursor.fetchone() is not None
    conn.close()
    return exists

def article_exists(url=None, content_hash=None):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    exists = False
    
    if url:
        cursor.execute("SELECT 1 FROM articles WHERE original_link = ?", (url,))
        if cursor.fetchone():
            exists = True
            
    if not exists and content_hash:
        cursor.execute("SELECT 1 FROM articles WHERE content_hash = ?", (content_hash,))
        if cursor.fetchone():
            exists = True
            
    conn.close()
    return exists

def mark_entry_processed(entry_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("INSERT OR IGNORE INTO entries (entry_id) VALUES (?)", (entry_id,))
    conn.commit()
    conn.close()

def save_article(article_data):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            INSERT INTO articles (
                feed_entry_id, email_source, article_source_domain, title, 
                content, summary, tags, image_url, original_link, content_hash, published_date, feed_source_date,
                author, reading_time
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            article_data.get('feed_entry_id'),
            article_data.get('email_source'),
            article_data.get('article_source_domain'),
            article_data.get('title'),
            article_data.get('content'),
            article_data.get('summary'),
            article_data.get('tags'),  # Stored as comma-separated string or JSON string
            article_data.get('image_url'),
            article_data.get('original_link'),
            article_data.get('content_hash'),
            article_data.get('published_date'),
            article_data.get('feed_source_date'),
            article_data.get('author'),
            article_data.get('reading_time')
        ))
        conn.commit()
        logger.info(f"Saved article: {article_data.get('title')}")
    except sqlite3.IntegrityError:
        logger.info(f"Article already exists (duplicate link): {article_data.get('original_link')}")
    finally:
        conn.close()

def get_non_spam_articles(limit=50):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Filter out articles that have 'spam' in their tags
    # Sort first by the actual email arrival time (feed_source_date), then by article time
    cursor.execute('''
        SELECT * FROM articles 
        WHERE tags NOT LIKE '%spam%' 
        ORDER BY crawl_date DESC
        LIMIT ?
    ''', (limit,))
    
    rows = cursor.fetchall()
    conn.close()
    return rows
