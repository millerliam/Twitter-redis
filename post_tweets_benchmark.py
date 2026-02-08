import argparse
import time
from twitter_api import TwitterRedisAPI

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True, help="Path to tweets.csv")
    parser.add_argument("--target", type=int, default=1_000_000, help="How many tweets to insert")
    args = parser.parse_args()

    api = TwitterRedisAPI(host="localhost", port=6379, db=0)

    start = time.perf_counter()
    count = 0

    with open(args.csv, "r", encoding="utf-8") as f:
        header = f.readline()  # skip header
        for line in f:
            line = line.rstrip("\n")
            if not line:
                continue

            # split on FIRST comma only (tweet text may contain commas)
            user_id_str, tweet_text = line.split(",", 1)
            api.post_tweet(int(user_id_str), tweet_text)

            count += 1
            if count % 50_000 == 0:
                elapsed = time.perf_counter() - start
                print(f"{count:,} inserted | {count/elapsed:,.1f} postTweet calls/sec")

            if count >= args.target:
                break

    elapsed = time.perf_counter() - start
    print("\nPOST TWEET RESULTS")
    print(f"tweets inserted: {count:,}")
    print(f"seconds: {elapsed:.3f}")
    print(f"postTweet calls/sec: {count/elapsed:,.1f}")

if __name__ == "__main__":
    main()
