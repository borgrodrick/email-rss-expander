from google import genai
from google.genai import types
import os
import logging
import json

logger = logging.getLogger(__name__)

def get_client():
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        logger.error("GEMINI_API_KEY not found.")
        return None
    return genai.Client(api_key=api_key)

def analyze_article(title, content_snippet):
    """
    Analyzes the article content using Gemini to generate a summary and tags.
    """
    client = get_client()
    if not client:
        return {"summary": "Gemini API not configured.", "tags": []}

    model_id = 'gemini-3-flash-preview'
    
    prompt = f"""
    You are an intelligent RSS feed curator. 
    Analyze the following article content.
    
    Article Title: {title}
    Content Snippet: {content_snippet[:4000]}... (truncated)

    Tasks:
    1. Write a concise summary of the article
    2. Generate a list of the top 5 most relevant tags (topics, companies, people).
    3. CRITICAL: If the content looks like an advertisement, a newsletter intro that isn't an article, a "subscribe now" prompt, or spam, YOU MUST include the tag 'spam' in the tags list.

    Return the result as a VALID JSON object with the following structure:
    {{
        "summary": "The summary text...",
        "tags": ["tag1", "tag2", "spam"]
    }}
    """
    
    try:
        response = client.models.generate_content(
            model=model_id,
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json"
            )
        )
        result = json.loads(response.text)
        return result
    except Exception as e:
        logger.error(f"Error calling Gemini: {e}")
        return {"summary": "Error generating summary.", "tags": []}
