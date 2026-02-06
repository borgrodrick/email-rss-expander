import sqlite3
import logging
from newspaper import Article
from newspaper import Config
import lxml.html

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_PATH = 'articles.db'

def get_non_spam_articles(limit=50):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Filter out articles that have 'spam' in their tags
    # Sort first by the actual email arrival time (feed_source_date), then by article time
    cursor.execute('''
        SELECT id, original_link, title FROM articles 
        WHERE tags NOT LIKE '%spam%' 
        ORDER BY crawl_date DESC
        LIMIT ?
    ''', (limit,))
    
    rows = cursor.fetchall()
    conn.close()
    return rows

def clean_html_content(node):
    """
    Extracts HTML from a lxml node and cleans it for RSS.
    Removes attributes like class, id, style to keep it clean.
    """
    if node is None:
        return ""
    
    # Iterate over all elements and strip attributes
    for element in node.iter():
        # Keep href and src, strip others
        keys = list(element.attrib.keys())
        for key in keys:
            if key not in ['href', 'src', 'alt', 'title']:
                del element.attrib[key]
                
    # Serialize to string
    return lxml.html.tostring(node, encoding='unicode', method='html')

def backfill_content():
    logger.info("Starting backfill of content (HTML)...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get recent 50 articles
    logger.info("Selecting top 50 recent articles...")
    
    articles = get_non_spam_articles(limit=50)
    
    logger.info(f"Found {len(articles)} articles to update.")
    
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    config = Config()
    config.browser_user_agent = user_agent
    config.request_timeout = 10

    count = 0
    updated = 0
    
    for article_id, url, title in articles:
        count += 1
        if not url:
            continue
            
        try:
            # logger.info(f"[{count}/{len(articles)}] Fetching: {title[:30]}...")
            article = Article(url, config=config)
            article.download()
            article.parse()
            
            html_content = ""
            if article.top_node is not None:
                html_content = clean_html_content(article.top_node)
            else:
                logger.warning(f"No top_node for {url}, falling back to text.")
                html_content = article.text
            
            if html_content:
                cursor.execute("UPDATE articles SET content = ? WHERE id = ?", (html_content, article_id))
                conn.commit()
                updated += 1
            
        except Exception as e:
            logger.error(f"Failed to fetch/parse {url}: {e}")

        if count % 5 == 0:
            logger.info(f"Processed {count}/{len(articles)}...")

    conn.close()
    logger.info(f"Backfill complete. Updated {updated} articles.")

if __name__ == "__main__":
    backfill_content()
