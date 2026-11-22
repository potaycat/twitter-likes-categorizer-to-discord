#!/usr/bin/env python3
"""
Migration script to import lost tweets from like-lost.json into like.db
Sets status based on tweet content:
- 'failed-suspended' for suspended accounts
- 'failed-privated' for private/restricted accounts
"""

import json
import sqlite3
from datetime import datetime
from pathlib import Path

# Paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
LOST_TWEETS_FILE = PROJECT_ROOT / 'like-lost.json'
DB_FILE = PROJECT_ROOT / 'likes.db'

def determine_status(full_text):
    """Determine status based on the fullText content."""
    if 'suspended account' in full_text:
        return 'failed-suspended'
    elif 'account owner limits' in full_text:
        return 'failed-privated'
    else:
        # Fallback for any other {learnmore} messages
        return 'failed-unavailable'

def import_lost_tweets():
    """Import lost tweets into the database."""

    # Load the lost tweets JSON
    print(f"Loading lost tweets from {LOST_TWEETS_FILE}...")
    with open(LOST_TWEETS_FILE, 'r', encoding='utf-8') as f:
        lost_tweets = json.load(f)

    print(f"Found {len(lost_tweets)} lost tweets to import")

    # Connect to database
    print(f"Connecting to database: {DB_FILE}")
    conn = sqlite3.connect(str(DB_FILE), isolation_level=None)
    cursor = conn.cursor()

    # Process each lost tweet
    updated_count = 0
    inserted_count = 0

    for entry in lost_tweets:
        like = entry['like']
        tweet_id = like['tweetId']
        full_text = like['fullText']
        expanded_url = like['expandedUrl']

        # Determine the status
        status = determine_status(full_text)

        # Check if entry already exists
        cursor.execute('SELECT tweet_id FROM processed_likes WHERE tweet_id = ?', (tweet_id,))
        exists = cursor.fetchone() is not None

        if exists:
            # Update existing entry
            cursor.execute('''
                UPDATE processed_likes
                SET status = ?,
                    tweet_url = ?,
                    processed_at = ?
                WHERE tweet_id = ?
            ''', (status, expanded_url, datetime.now().isoformat(), tweet_id))
            updated_count += 1
        else:
            # Insert new entry with minimal required fields
            cursor.execute('''
                INSERT INTO processed_likes
                (tweet_id, category, confidence, status, tweet_url, classifier_version, processed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                tweet_id,
                'lost',  # category for lost tweets
                0.0,     # confidence
                status,
                expanded_url,
                'migration',  # classifier_version
                datetime.now().isoformat()
            ))
            inserted_count += 1

    # Commit changes
    conn.commit()

    # Print summary
    print(f"\nMigration complete:")
    print(f"  - Inserted: {inserted_count} new entries")
    print(f"  - Updated: {updated_count} existing entries")
    print(f"  - Total processed: {len(lost_tweets)}")

    # Show status breakdown
    cursor.execute('''
        SELECT status, COUNT(*)
        FROM processed_likes
        WHERE status LIKE 'failed-%'
        GROUP BY status
    ''')
    print(f"\nStatus breakdown:")
    for status, count in cursor.fetchall():
        print(f"  - {status}: {count}")

    conn.close()
    print(f"\nDatabase closed. Migration successful!")

if __name__ == '__main__':
    import_lost_tweets()
