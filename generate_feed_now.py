from feedgen.feed import FeedGenerator
import db
from datetime import datetime
import logging

# Configure logging to console
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

OUTPUT_FILE = "output.xml"
FEED_URL = "https://kill-the-newsletter.com/feeds/km69ge1d7gq6c4rhg5uv.xml"

def generate_now():
    logger.info("Generating RSS feed from current DB...")
    fg = FeedGenerator()
    fg.title('Curated Email Articles (Partial)')
    fg.link(href=FEED_URL, rel='alternate')
    fg.description('Aggregated articles from email newsletters.')
    
    articles = db.get_non_spam_articles(limit=50)
    count = 0
    for row in articles:
        fe = fg.add_entry()
        fe.title(row['title'])
        fe.link(href=row['original_link'])
        fe.description(f"<p><strong>Summary:</strong> {row['summary']}</p><p><strong>Tags:</strong> {row['tags']}</p><img src='{row['image_url']}'/><br/><p><small>Source: {row['article_source_domain']} via {row['email_source']}</small></p>")
        
        try:
            pub_date = datetime.fromisoformat(row['published_date'])
            if pub_date.tzinfo is None:
                pub_date = pub_date.astimezone()
            fe.pubDate(pub_date)
        except Exception:
            fe.pubDate(datetime.now().astimezone())
            
        fe.guid(row['original_link'])
        count += 1
    
    fg.rss_file(OUTPUT_FILE)
    logger.info(f"Successfully wrote RSS feed to {OUTPUT_FILE} with {count} items.")

if __name__ == "__main__":
    generate_now()
