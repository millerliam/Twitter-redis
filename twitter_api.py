from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional

import random

import mysql.connector

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    # Allow MySQL-only usage without redis installed.
    redis = None  # type: ignore


@dataclass(frozen=True)
class Tweet:
    """
    Represents a single tweet returned by the API.

    Parameters:
        tweet_id (int): Unique identifier for the tweet.
        user_id (int): ID of the user who posted the tweet.
        tweet_ts (str): Timestamp when the tweet was created.
        tweet_text (str): Content of the tweet.

    Returns:
        Tweet: An immutable Tweet object.
    """
    tweet_id: int
    user_id: int
    tweet_ts: str
    tweet_text: str


class TwitterMySQLAPI:
    """
    MySQL-backed implementation of the Twitter API.

    Benchmark code must interact ONLY with this API.
    The underlying database implementation is hidden.
    """

    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        database: str,
        port: int = 3306,
    ) -> None:
        """
        Initialize the API and open a persistent database connection.

        Parameters:
            host (str): MySQL host name.
            user (str): MySQL username.
            password (str): MySQL password.
            database (str): Database name.
            port (int): MySQL port (default 3306).

        Returns:
            None
        """
        self.conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port,
            autocommit=False,
        )
        self.cur = self.conn.cursor()

    def post_tweet(self, user_id: int, tweet_text: str) -> int:
        """
        Insert a single tweet into the database.

        Parameters:
            user_id (int): ID of the user posting the tweet.
            tweet_text (str): Text content of the tweet.

        Returns:
            int: The tweet_id of the newly inserted tweet.
        """
        self.cur.execute(
            "INSERT INTO TWEET (user_id, tweet_text) VALUES (%s, %s)",
            (user_id, tweet_text),
        )
        self.conn.commit()
        return self.cur.lastrowid

    def get_home_timeline(self, user_id: int) -> List[Tweet]:
        """
        Retrieve the home timeline for a user.

        Parameters:
            user_id (int): ID of the user requesting the timeline.

        Returns:
            List[Tweet]: Up to 10 most recent tweets from followed users.
        """
        self.cur.execute(
            """
            SELECT t.tweet_id, t.user_id, t.tweet_ts, t.tweet_text
            FROM FOLLOWS f
            JOIN TWEET t ON t.user_id = f.followee_id
            WHERE f.follower_id = %s
            ORDER BY t.tweet_ts DESC
            LIMIT 10
            """,
            (user_id,),
        )
        rows = self.cur.fetchall()
        return [Tweet(*row) for row in rows]

    def get_random_follower_id(self) -> Optional[int]:
        """
        Select a random follower ID without using ORDER BY RAND().

        Returns:
            Optional[int]: A follower_id if one exists, otherwise None.
        """
        self.cur.execute("SELECT MIN(follower_id), MAX(follower_id) FROM FOLLOWS")
        row = self.cur.fetchone()
        if not row or row[0] is None:
            return None

        min_id, max_id = row
        rand_id = random.randint(min_id, max_id)

        self.cur.execute(
            """
            SELECT follower_id
            FROM FOLLOWS
            WHERE follower_id >= %s
            ORDER BY follower_id
            LIMIT 1
            """,
            (rand_id,),
        )
        row = self.cur.fetchone()
        return int(row[0]) if row else None

    def load_follows_csv(self, csv_path: str, has_header: bool = True) -> int:
        """
        Load follow relationships from a CSV file.

        Parameters:
            csv_path (str): Path to the CSV file.
            has_header (bool): Whether the CSV has a header row.

        Returns:
            int: Number of rows inserted (duplicates ignored).
        """
        inserted = 0
        with open(csv_path, "r", encoding="utf-8") as f:
            if has_header:
                next(f, None)
            for line in f:
                line = line.strip()
                if not line:
                    continue
                follower_id_str, followee_id_str = line.split(",", 1)
                self.cur.execute(
                    """
                    INSERT IGNORE INTO FOLLOWS (follower_id, followee_id)
                    VALUES (%s, %s)
                    """,
                    (int(follower_id_str), int(followee_id_str)),
                )
                inserted += self.cur.rowcount
        self.conn.commit()
        return inserted

    def close(self) -> None:
        """
        Close the database cursor and connection.

        Returns:
            None
        """
        self.cur.close()
        self.conn.close()


class TwitterRedisAPI:
    """Redis-backed implementation of the Twitter API.

    Key schema (all keys are strings):
      - tweet:{tweet_id} (HASH): user_id, tweet_ts, tweet_text
      - followers:{user_id} (SET): follower_ids
      - following:{user_id} (SET): followee_ids (optional, but handy)
      - timeline:{user_id} (ZSET): member=tweet_id, score=timestamp_ms
      - all_followers (SET): all follower_ids (for get_random_follower_id)
      - next_tweet_id (STRING): INCR counter
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
        decode_responses: bool = True,
        timeline_max_size: Optional[int] = None,
    ) -> None:
        """Initialize Redis connection.

        Parameters:
            host (str): Redis host.
            port (int): Redis port.
            db (int): Redis DB number.
            password (Optional[str]): Redis password.
            decode_responses (bool): Return strings instead of bytes.
            timeline_max_size (Optional[int]): If set, trims timelines to keep
                only the most recent N tweet ids (helps memory).
        """
        if redis is None:
            raise ImportError(
                "redis package is not installed. Add 'redis' to requirements.txt and reinstall."
            )

        self.r = redis.Redis(
            host=host,
            port=port,
            db=db,
            password=password,
            decode_responses=decode_responses,
        )
        self.timeline_max_size = timeline_max_size

    @staticmethod
    def _now_ts() -> Dict[str, object]:
        """Return both a sortable numeric timestamp and a human-readable string."""
        now = datetime.now(timezone.utc)
        ts_ms = int(now.timestamp() * 1000)
        return {"ts_ms": ts_ms, "ts_str": now.isoformat()}

    @staticmethod
    def _tweet_key(tweet_id: int) -> str:
        return f"tweet:{tweet_id}"

    @staticmethod
    def _followers_key(user_id: int) -> str:
        return f"followers:{user_id}"

    @staticmethod
    def _following_key(user_id: int) -> str:
        return f"following:{user_id}"

    @staticmethod
    def _timeline_key(user_id: int) -> str:
        return f"timeline:{user_id}"

    def post_tweet(self, user_id: int, tweet_text: str) -> int:
        """Post a tweet and fan-out the tweet id to follower timelines."""
        tweet_id = int(self.r.incr("next_tweet_id"))
        ts = self._now_ts()
        ts_ms = int(ts["ts_ms"])
        ts_str = str(ts["ts_str"])

        tweet_key = self._tweet_key(tweet_id)
        author_tl = self._timeline_key(user_id)
        followers_key = self._followers_key(user_id)

        # Store tweet + add to author's own timeline.
        pipe = self.r.pipeline(transaction=False)
        pipe.hset(
            tweet_key,
            mapping={
                "user_id": str(user_id),
                "tweet_ts": ts_str,
                "tweet_text": tweet_text,
            },
        )
        pipe.zadd(author_tl, {str(tweet_id): ts_ms})
        if self.timeline_max_size is not None:
            # Keep most recent N (rank 0 is oldest, so remove everything before -N)
            pipe.zremrangebyrank(author_tl, 0, -self.timeline_max_size - 1)
        pipe.execute()

        # Fan-out to followers. For scale, iterate without loading the whole set.
        # Using scan_iter avoids pulling all followers into memory.
        pipe = self.r.pipeline(transaction=False)
        ops = 0
        for follower_id_str in self.r.sscan_iter(followers_key):
            try:
                follower_id = int(follower_id_str)
            except Exception:
                continue

            tl_key = self._timeline_key(follower_id)
            pipe.zadd(tl_key, {str(tweet_id): ts_ms})
            if self.timeline_max_size is not None:
                pipe.zremrangebyrank(tl_key, 0, -self.timeline_max_size - 1)
            ops += 1

            # Execute in chunks to keep pipeline size reasonable.
            if ops % 1000 == 0:
                pipe.execute()
                pipe = self.r.pipeline(transaction=False)

        if ops % 1000 != 0:
            pipe.execute()

        return tweet_id

    def get_home_timeline(self, user_id: int) -> List[Tweet]:
        """Return up to 10 most recent tweets already materialized in user's timeline."""
        tl_key = self._timeline_key(user_id)
        tweet_ids = self.r.zrevrange(tl_key, 0, 9)
        if not tweet_ids:
            return []

        pipe = self.r.pipeline(transaction=False)
        for tid in tweet_ids:
            pipe.hgetall(self._tweet_key(int(tid)))
        rows = pipe.execute()

        out: List[Tweet] = []
        for tid, h in zip(tweet_ids, rows):
            if not h:
                continue
            out.append(
                Tweet(
                    tweet_id=int(tid),
                    user_id=int(h.get("user_id", "0")),
                    tweet_ts=str(h.get("tweet_ts", "")),
                    tweet_text=str(h.get("tweet_text", "")),
                )
            )
        return out

    def get_random_follower_id(self) -> Optional[int]:
        """Return a random follower_id.

        """
        # SRANDMEMBER returns None if key doesn't exist / empty.
        v = self.r.srandmember("all_followers")
        return int(v) if v is not None else None

    def load_follows_csv(self, csv_path: str, has_header: bool = True) -> int:
        """Load follow relationships from a CSV file.

        Expected CSV format: follower_id,followee_id
        """
        inserted = 0
        pipe = self.r.pipeline(transaction=False)
        ops = 0

        with open(csv_path, "r", encoding="utf-8") as f:
            if has_header:
                next(f, None)
            for line in f:
                line = line.strip()
                if not line:
                    continue
                follower_id_str, followee_id_str = line.split(",", 1)
                follower_id = int(follower_id_str)
                followee_id = int(followee_id_str)

                # Add follower -> followee relationship.
                pipe.sadd(self._followers_key(followee_id), str(follower_id))
                pipe.sadd(self._following_key(follower_id), str(followee_id))
                pipe.sadd("all_followers", str(follower_id))
                ops += 3

                # Execute in chunks.
                if ops >= 3000:
                    results = pipe.execute()
                    # Count only new follow edges (the first SADD in each triple).
                    inserted += sum(int(x) for x in results[0::3])
                    pipe = self.r.pipeline(transaction=False)
                    ops = 0

        if ops:
            results = pipe.execute()
            inserted += sum(int(x) for x in results[0::3])

        return inserted

    def close(self) -> None:
        """Close Redis connection (no-op for redis-py, but kept for symmetry)."""
        try:
            self.r.close()  # redis-py >= 4
        except Exception:
            pass
