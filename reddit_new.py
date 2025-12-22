subreddit_name = "cricket"
print("Subreddit set to:",subreddit_name)
import praw
import prawcore
import csv
from datetime import datetime

reddit = praw.Reddit(
    client_id = "pqoKREKzIuDGIVYmk7p4Rw",
    client_secret="IG_I1MkfFz9YNwEDLIA-VGqyZDBhyQ",
    user_agent="sCS6350Proj"
)

def fetch_post():
    print("Fetching posts from:",subreddit_name)
    # print("This is where reddit code will go")
    subreddit = reddit.subreddit(subreddit_name)
    with open("post.csv","w",encoding="utf-8",newline="") as csvfile:
        writter = csv.writer(csvfile)
        # writter.writerow(["title","score","created_utc","readable_time","comment_1","comment_2","comment_3"])
        count = 0
        posts_data=[]
        for post in subreddit.hot(limit=10):
            # keeping fetched data together so that can be used for filtering data later
            posts_data.append({
                "title": post.title,
                "score":post.score,
                "num_comments": post.num_comments,
                "created_utc": post.created_utc
            })
            post.comments.replace_more(limit=0)
            top_comments = [c.body.replace("\n"," ") for c in post.comments[:3]]
            # if fewer exist
            while len(top_comments)<3:
                top_comments.append("")
            count+=1
            readable = datetime.fromtimestamp(post.created_utc).strftime("%Y-%m-%d %H:%M:%S")
            writter.writerow([post.id,post.title, post.score, post.created_utc,readable,top_comments[0],top_comments[1],top_comments[2]])
            print("Fetched and saved post:",post.title)
        
        high_engaging_posts = [
            post for post in posts_data if post["num_comments"]>20
        ]
        print("Total number of posts fetched: ",count)
        print("Length of post data: ",len(posts_data))
        print(posts_data[0])
        print("High engaging posts: ",high_engaging_posts)

fetch_post()