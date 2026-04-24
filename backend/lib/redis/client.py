import os

from redis.asyncio import ConnectionPool, Redis

from backend.lib.utils.common import none_throws


class RedisClient:
    def __init__(self) -> None:
        self.__connection_pool = ConnectionPool(
            host=none_throws(os.getenv("REDIS_HOST")),
            port=int(none_throws(os.getenv("REDIS_PORT"))),
            username=none_throws(os.getenv("REDIS_USERNAME")),
            password=none_throws(os.getenv("REDIS_PASSWORD")),
            decode_responses=True,
        )
        self.client = Redis(
            connection_pool=self.__connection_pool,
            decode_responses=True,
        )
