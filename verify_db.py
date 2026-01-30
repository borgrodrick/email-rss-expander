import sqlite3
import os

DB_PATH = "articles.db"

def check_db():
    if not os.path.exists(DB_PATH):
        print("DB does not exist.")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT count(*) FROM entries")
        entries_count = cursor.fetchone()[0]
        print(f"Entries processed: {entries_count}")
        
        cursor.execute("SELECT count(*) FROM articles")
        articles_count = cursor.fetchone()[0]
        print(f"Articles saved: {articles_count}")
        
        cursor.execute("SELECT title, summary, tags FROM articles ORDER BY id DESC LIMIT 3")
        rows = cursor.fetchall()
        print("\nLast 3 articles details:")
        for row in rows:
            print(f"Title: {row[0]}")
            print(f"Summary: {row[1]}")
            print(f"Tags: {row[2]}")
            print("-" * 20)

    except Exception as e:
        print(f"Error reading DB: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    check_db()
