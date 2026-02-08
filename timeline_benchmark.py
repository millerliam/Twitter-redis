import argparse
import time
from twitter_api import TwitterRedisAPI

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--requests", type=int, default=50_000, help="Number of timeline requests")
    args = parser.parse_args()

    api = TwitterRedisAPI(host="localhost", port=6379, db=0)

    start = time.perf_counter()
    done = 0

    for _ in range(args.requests):
        user_id = api.get_random_follower_id()
        if user_id is None:
            raise RuntimeError("FOLLOWS is empty. Load follows.csv first.")

        _timeline = api.get_home_timeline(user_id)
        done += 1

        if done % 5_000 == 0:
            elapsed = time.perf_counter() - start
            print(f"{done:,} timelines | {done/elapsed:,.1f} getHomeTimeline calls/sec")

    elapsed = time.perf_counter() - start
    print("\nTIMELINE RESULTS")
    print(f"timeline calls: {done:,}")
    print(f"seconds: {elapsed:.3f}")
    print(f"getHomeTimeline calls/sec: {done/elapsed:,.1f}")

if __name__ == "__main__":
    main()
