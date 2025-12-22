import os
import time
import json
import argparse
import logging
from datetime import datetime
from typing import List, Dict

import pandas as pd
import praw
import prawcore
import socket

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Initialize PRAW (script app)
reddit = praw.Reddit(
    client_id="pqoKREKzIuDGIVYmk7p4Rw",
    client_secret="IG_I1MkfFz9YNwEDLIA-VGqyZDBhyQ",
    username="cabbitnation",
    password="8{lN&9?Yry4!",
    user_agent="sCS6350Proj"
)

def safe_sleep(seconds: float):
    """Sleep but allow KeyboardInterrupt to bubble up quickly."""
    try:
        time.sleep(seconds)
    except KeyboardInterrupt:
        raise

def fetch_subreddit_posts(subreddit_name: str, limit: int = 100, sort: str = "new") -> List[Dict]:
    sub = reddit.subreddit(subreddit_name)
    if sort == "hot":
        submissions = sub.hot(limit=limit)
    elif sort == "new":
        submissions = sub.new(limit=limit)
    elif sort == "top":
        submissions = sub.top(limit=limit)
    elif sort == "rising":
        submissions = sub.rising(limit=limit)
    else:
        submissions = sub.new(limit=limit)

    results = []
    for i, submission in enumerate(submissions, start=1):
        # small pause to be polite
        if i % 30 == 0:
            safe_sleep(1.0)

        try:
            submission_dict = {
                "id": submission.id,
                "title": submission.title,
                "selftext": getattr(submission, "selftext", ""),
                "score": getattr(submission, "score", None), # upvotes-downvotes
                "upvote_estimated": submission.upvote_ratio,
                "upvote_ratio": getattr(submission, "upvote_ratio", None),
                "num_comments": getattr(submission, "num_comments", None),
                "created_utc": getattr(submission, "created_utc", None),
                "author": str(submission.author) if submission.author else None,
                "url": getattr(submission, "url", ""),
                "subreddit": subreddit_name,
                "over_18": getattr(submission, "over_18", False),
                "is_self": getattr(submission, "is_self", False),
            }
            results.append(submission_dict)
        except Exception as e:
            logging.warning("Skipped a submission due to exception: %s", e)
    return results

def fetch_comments_for_submission(submission_id: str, max_comments: int = 200) -> List[Dict]:
    try:
        submission = reddit.submission(id=submission_id)
        # expand "more comments" to a reasonable extent
        submission.comments.replace_more(limit=0)
        comments = []
        for i, comment in enumerate(submission.comments.list()):
            if i >= max_comments:
                break
            comments.append({
                "submission_id": submission_id,
                "comment_id": comment.id,
                "author": str(comment.author) if comment.author else None,
                "body": getattr(comment, "body", ""),
                "created_utc": getattr(comment, "created_utc", None),
                "score": getattr(comment, "score", None),
                "parent_id": getattr(comment, "parent_id", None),
            })
        return comments
    except prawcore.exceptions.NotFound:
        logging.warning("Submission %s not found.", submission_id)
        return []
    except Exception as e:
        logging.error("Error fetching comments for %s: %s", submission_id, e)
        return []

def save_json(data, filename):
    with open(filename, "w", encoding="utf8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logging.info("Saved JSON to %s", filename)

def save_csv_from_records(records, filename):
    if not records:
        logging.info("No records to save for %s", filename)
        return
    df = pd.DataFrame(records)
    df.to_csv(filename, index=False)
    logging.info("Saved CSV to %s (rows=%d)", filename, len(df))

def main(args):
    logging.info("Starting scrape: subreddit=%s limit=%d sort=%s", args.subreddit, args.limit, args.sort)
    attempts = 0
    max_attempts = 5
    backoff = 2.0

    while attempts < max_attempts:
        try:
            posts = fetch_subreddit_posts(args.subreddit, limit=args.limit, sort=args.sort)
            break
        except (prawcore.exceptions.RateLimitExceeded, prawcore.exceptions.ServerError, socket.timeout) as e:
            attempts += 1
            logging.warning("Transient error: %s — retrying in %.1fs (%d/%d)", e, backoff, attempts, max_attempts)
            safe_sleep(backoff)
            backoff *= 2
    else:
        logging.error("Failed to fetch posts after %d attempts. Exiting.", max_attempts)
        return

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    base = f"{args.subreddit}_{timestamp}"
    save_json(posts, base + "_posts.json")
    save_csv_from_records(posts, base + "_posts.csv")

    # Fetch comments for top N posts if requested
    comments_records = []
    for idx, p in enumerate(posts[: args.comment_posts]):
        sid = p["id"]
        logging.info("Fetching comments for post %d/%d id=%s", idx+1, min(len(posts), args.comment_posts), sid)
        c = fetch_comments_for_submission(sid, max_comments=args.max_comments_per_post)
        comments_records.extend(c)
        safe_sleep(args.pause_between_posts)

    if comments_records:
        save_json(comments_records, base + "_comments.json")
        save_csv_from_records(comments_records, base + "_comments.csv")
    logging.info("Done.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simple PRAW Reddit scraper")
    parser.add_argument("--subreddit", "-s", default="learnpython", help="Subreddit name (no r/)")
    parser.add_argument("--limit", "-n", type=int, default=200, help="Number of posts to fetch")
    parser.add_argument("--sort", default="new", choices=["new", "hot", "top", "rising"], help="Sort method")
    parser.add_argument("--comment-posts", "-c", type=int, default=5, help="Number of posts to fetch comments for")
    parser.add_argument("--max-comments-per-post", type=int, default=200, help="Max comments per post")
    parser.add_argument("--pause-between-posts", type=float, default=1.0, help="Seconds to pause between posts when fetching comments")
    args = parser.parse_args()
    main(args)