import logging
import os
import sys
import requests
import hashlib
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
from newspaper import Article
from feedgen.feed import FeedGenerator
from datetime import datetime
from urllib.parse import urlparse
from dotenv import load_dotenv

# Import helper modules
import db
import filters
import gemini

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

FEED_URL = "https://kill-the-newsletter.com/feeds/km69ge1d7gq6c4rhg5uv.xml"
OUTPUT_FILE = "output.xml"

# Function to fix kill-the-newsletter date format if needed, or use current time if parsing fails
# But for now we will try to pass the raw date string or simple parsing
def parse_date(date_str):
    return date_str

def main():
    logger.info("Starting Email RSS Expander")
    
    # Check for API Key
    if not os.environ.get("GEMINI_API_KEY"):
        logger.warning("GEMINI_API_KEY environment variable not found. Gemini features will fail.")

    # 1. Initialize DB and Filters
    db.init_db()
    link_filter = filters.LinkFilter()
    
    # 2. Fetch Feed
    logger.info(f"Fetching feed from {FEED_URL}...")
    xml_content = None
    try:
        response = requests.get(FEED_URL)
        response.raise_for_status()
        xml_content = response.content
    except Exception as e:
        logger.error(f"Failed to fetch feed: {e}")
        # Fallback to local file for testing/dev or if feed is down
        local_path = "sample-input/km69ge1d7gq6c4rhg5uv.xml"
        if os.path.exists(local_path):
            logger.info(f"Falling back to local file: {local_path}")
            with open(local_path, 'rb') as f:
                xml_content = f.read()
        else:
            logger.error("No local backup available. Exiting.")
            return

    # 3. Parse XML
    try:
        if not xml_content:
            return
        root = ET.fromstring(xml_content)
        # Handle namespaces if present, usually atom has namespace {http://www.w3.org/2005/Atom}
        # We can strip namespaces or just use wildcard find
        # For simplicity in ElementTree with namespaces, it's often easier to strip them or use a helper
        # Let's try to just find 'entry' tags. If default namespace is used, we need to handle it.
        # Quick hack: parsing with BeautifulSoup might be easier for namespace-heavy XML
        soup = BeautifulSoup(xml_content, 'xml')
        entries = soup.find_all('entry')
    except Exception as e:
        logger.error(f"Failed to parse XML: {e}")
        return
    
    logger.info(f"Found {len(entries)} entries in the feed.")

    # 4. Process Entries
    for entry in entries:
        entry_id = entry.find('id').text if entry.find('id') else None
        if not entry_id:
            logger.warning("Entry found without ID, skipping.")
            continue
            
        if db.entry_exists(entry_id):
            logger.info(f"Entry {entry_id} already processed. Skipping.")
            continue
            
        logger.info(f"Processing new entry: {entry_id}")
        
        email_title = entry.find('title').text if entry.find('title') else "No Title"
        content_tag = entry.find('content')
        if not content_tag:
            logger.warning("No content tag found in entry.")
            continue
            
        html_content = content_tag.text # BeautifulSoup automatically decodes entities
        
        # Extract feed entry date (prefer updated, fallback to published)
        entry_date = entry.find('updated').text if entry.find('updated') else None
        if not entry_date:
            entry_date = entry.find('published').text if entry.find('published') else datetime.now().isoformat()

        # Link Extraction
        link_soup = BeautifulSoup(html_content, 'html.parser')
        links = link_soup.find_all('a', href=True)
        
        unique_urls = set()
        for link in links:
            raw_url = link['href']
            # Unwrap potential redirects
            unwrapped_url = link_filter.unwrap_redirect(raw_url)
            
            link_text = link.get_text(strip=True)
            if link_filter.is_valid_url(unwrapped_url, link_text=link_text):
                normalized_url = link_filter.normalize_url(unwrapped_url)
                unique_urls.add(normalized_url)
        
        logger.info(f"Found {len(unique_urls)} potential article links in email '{email_title}'")
        
        # Crawl Articles
        for url in unique_urls:
            # Check if URL exists in DB or has failed previously
            if db.article_exists(url=url):
                logger.info(f"Skipping duplicate URL: {url}")
                continue
            
            if db.is_crawl_failed(url):
                logger.info(f"Skipping previously failed URL: {url}")
                continue
            
            logger.info(f"Crawling article: {url}")
            try:
                article = Article(url)
                article.download()
                article.parse()
                
                # Extract Data
                title = article.title
                text = article.text
                
                if not text or len(text.strip()) < 100:
                    logger.warning(f"Skipping article with insufficient content: {url}")
                    continue

                # Content Hashing for Deduplication
                content_hash = hashlib.sha256(text.encode('utf-8')).hexdigest()
                
                if db.article_exists(content_hash=content_hash):
                    logger.info(f"Skipping duplicate content (hash match): {url}")
                    continue

                publish_date = str(article.publish_date) if article.publish_date else datetime.now().isoformat()
                image = article.top_image
                source_domain = urlparse(url).netloc
                
                # Extract Author(s)
                authors = ", ".join(article.authors) if article.authors else "Unknown Author"

                # Calculate Reading Time
                # Standard reading speed is ~200-250 wpm
                text_len = len(text.split())
                reading_time = max(1, round(text_len / 200))

                # Gemini Analysis
                analysis = gemini.analyze_article(title, text[:4000])
                summary = analysis.get("summary", "")
                tags = analysis.get("tags", [])
                
                article_data = {
                    'feed_entry_id': entry_id,
                    'email_source': email_title,
                    'article_source_domain': source_domain,
                    'title': title,
                    'content': text,
                    'summary': summary,
                    'tags': ",".join(tags),
                    'image_url': image,
                    'original_link': url,
                    'content_hash': content_hash,
                    'published_date': publish_date,
                    'feed_source_date': entry_date,
                    'author': authors,
                    'reading_time': reading_time
                }
                
                db.save_article(article_data)
                
            except Exception as e:
                logger.error(f"Failed to process article {url}: {e}")
                error_msg = str(e)
                if "403" in error_msg or "401" in error_msg:
                    logger.warning(f"Marking URL as failed (403/401): {url}")
                    db.mark_crawl_failed(url, "403/401 Forbidden/Unauthorized")
        
        # Mark entry as processed
        db.mark_entry_processed(entry_id)

    # 5. Generate Output Output
    logger.info("Generating RSS feed...")
    fg = FeedGenerator()
    fg.title('Curated Email Articles')
    fg.link(href=FEED_URL, rel='alternate')
    fg.description('Aggregated articles from email newsletters, filtered and summarized.')
    
    articles = db.get_non_spam_articles(limit=50)
    for row in articles:
        fe = fg.add_entry()
        fe.title(row['title'])
        fe.link(href=row['original_link'])
        
        # Prepare metadata for description
        source_domain = row['article_source_domain']
        email_source = row['email_source']
        author = row['author'] if 'author' in row.keys() and row['author'] else "Unknown"
        reading_time = row['reading_time'] if 'reading_time' in row.keys() and row['reading_time'] else "?"
        
        description_html = f"""
        <p><strong>Summary:</strong> {row['summary']}</p>
        <p><strong>Tags:</strong> {row['tags']}</p>
        <p>
            <strong>Source:</strong> {source_domain}<br/>
            <strong>Author:</strong> {author}<br/>
            <strong>Reading Time:</strong> ~{reading_time} min
        </p>
        <img src='{row['image_url']}' style='max-width:100%;'/>
        <br/>
        <p><small>Via: {email_source}</small></p>
        """
        
        fe.description(description_html)
        
        # Content is the full view: Prepend summary and image to the main text
        full_content_html = f"""
        <div style="font-style: italic; padding: 10px; border-left: 4px solid #ccc; margin-bottom: 20px;">
            <p><strong>Summary:</strong> {row['summary']}</p>
            <p>
                <strong>Source:</strong> {source_domain}<br/>
                <strong>Author:</strong> {author}<br/>
                <strong>Reading Time:</strong> ~{reading_time} min
            </p>
        </div>
        <img src='{row['image_url']}' style='max-width:100%; margin-bottom: 20px;'/>
        <hr/>
        {row['content']}
        """
        fe.content(content=full_content_html, type='CDATA')
        
        # Handle date parsing
        try:
            # Try to parse ISO format if possible
            pub_date = datetime.fromisoformat(row['published_date'])
            # Ensure timezone
            if pub_date.tzinfo is None:
                pub_date = pub_date.astimezone()
            fe.pubDate(pub_date)
        except Exception:
            # Fallback to current time if parsing fails
            fe.pubDate(datetime.now().astimezone())
            
        fe.guid(row['original_link'])
    
    try:
        fg.rss_file(OUTPUT_FILE)
        logger.info(f"Successfully wrote RSS feed to {OUTPUT_FILE}")
    except Exception as e:
        logger.error(f"Failed to write RSS file: {e}")

if __name__ == "__main__":
    main()
