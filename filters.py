from adblockparser import AdblockRules
import logging
import os
import requests
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

logger = logging.getLogger(__name__)

EASYLIST_PATH = "easylist.txt"
EASYLIST_URL = "https://easylist.to/easylist/easylist.txt"

class LinkFilter:
    def __init__(self):
        self.rules = self._load_rules()
        self.blocked_substrings = [
            "unsubscribe", "preferences", "view in browser", "privacy policy",
            "login", "signin", "signup", "register"
        ]
        self.blocked_domains = [
            "twitter.com", "facebook.com", "linkedin.com", "instagram.com", "tiktok.com",
            "youtube.com", "google.com", "bing.com", "yahoo.com",
            "kill-the-newsletter.com"
        ]
        self.blocked_extensions = [
            ".png", ".jpg", ".jpeg", ".gif", ".svg", ".css", ".js", ".ico"
        ]

    def _load_rules(self):
        if not os.path.exists(EASYLIST_PATH):
            logger.info("Downloading EasyList...")
            try:
                response = requests.get(EASYLIST_URL)
                response.raise_for_status()
                with open(EASYLIST_PATH, 'wb') as f:
                    f.write(response.content)
            except Exception as e:
                logger.error(f"Failed to download EasyList: {e}")
                return None
        
        with open(EASYLIST_PATH, 'r', encoding='utf-8') as f:
            raw_rules = f.read().splitlines()
        
        return AdblockRules(raw_rules)

    def is_valid_url(self, url, link_text=None):
        # 1. Check basic file extensions
        if any(url.lower().endswith(ext) for ext in self.blocked_extensions):
            return False

        # 2. Check heuristics (substrings) in URL
        url_lower = url.lower()
        if any(sub in url_lower for sub in self.blocked_substrings):
            return False

        # 3. Check heuristics (substrings) in Link Text
        if link_text:
            text_lower = link_text.lower()
            if any(sub in text_lower for sub in self.blocked_substrings):
                return False

        # 4. Check blocked domains (simple check, could use tldextract for better precision)
        if any(domain in url_lower for domain in self.blocked_domains):
            return False

        # 5. Check Adblock rules
        if self.rules and self.rules.should_block(url):
            return False

        return True

    def normalize_url(self, url):
        """
        Removes tracking parameters and normalizes URL for deduplication.
        """
        try:
            parsed = urlparse(url)
            # Filter out tracking query params
            query_params = parse_qsl(parsed.query, keep_blank_values=True)
            filtered_params = []
            tracking_keys = [
                'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content',
                'fbclid', 'gclid', 'ref', 'source'
            ]
            
            for key, value in query_params:
                if key.lower() not in tracking_keys:
                    filtered_params.append((key, value))
            
            # Reconstruct URL
            new_query = urlencode(filtered_params)
            
            # Drop fragments unless essential (usually safe to drop for articles)
            normalized = urlunparse((
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                parsed.params,
                new_query,
                '' # Drop fragment
            ))
            
            # Strip trailing slash for consistency
            if normalized.endswith('/'):
                normalized = normalized[:-1]
                
            return normalized
        except Exception:
            return url
