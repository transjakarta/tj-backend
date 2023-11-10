from fastapi import WebSocket
import redis.asyncio as aioredis
import asyncio


class RedisPubSubManager:
    """
        Initializes the RedisPubSubManager.

    Args:
        host (str): Redis server host.
        port (int): Redis server port.
        password (str): Redis server password.
    """

    def __init__(self, host="localhost", port=6379, password=None):
        self.redis_host = host
        self.redis_port = port
        self.redis_password = password
        self.redis_connection = None
        self.pubsub = None

    async def _get_redis_connection(self) -> aioredis.Redis:
        """
        Establishes a connection to Redis.

        Returns:
            aioredis.Redis: Redis connection object.
        """
        return aioredis.Redis(
            host=self.redis_host,
            port=self.redis_port,
            password=self.redis_password,
            auto_close_connection_pool=False,
        )

    async def connect(self) -> None:
        """
        Connects to the Redis server and initializes the pubsub client.
        """
        self.redis_connection = await self._get_redis_connection()
        self.pubsub = self.redis_connection.pubsub()

    async def _publish(self, channel: str, message: str) -> None:
        """
        Publishes a message to a specific Redis channel.

        Args:
            channel (str): Channel.
            message (str): Message to be published.
        """
        await self.redis_connection.publish(channel, message)

    async def subscribe(self, channel: str) -> aioredis.Redis:
        """
        Subscribes to a Redis channel.

        Args:
            channel (str): Channel to subscribe to.

        Returns:
            aioredis.ChannelSubscribe: PubSub object for the subscribed channel.
        """
        await self.pubsub.subscribe(channel)
        return self.pubsub

    async def unsubscribe(self, channel: str) -> None:
        """
        Unsubscribes from a Redis channel.

        Args:
            channel (str): Channel to unsubscribe from.
        """
        await self.pubsub.unsubscribe(channel)


class PubSubWebSocketManager:
    def __init__(self, redis_host="localhost", redis_port=6379, redis_password=None):
        """
        Initializes the WebSocketManager.

        Attributes:
            channels (dict): A dictionary to store WebSocket connections in different channel.
            pubsub_client (RedisPubSubManager): An instance of the RedisPubSubManager class for pub-sub functionality.
            subscribers (list): A list to store all the Redis PubSub subscribers.
        """
        self.channels: dict = {}
        self.pubsub_client = RedisPubSubManager(redis_host, redis_port, redis_password)
        self.subscribers = []

    async def subscribe_to_channel(self, channel: str, websocket: WebSocket) -> None:
        """
        Adds a user's WebSocket connection to a room.

        Args:
            channel (str): channel name.
            websocket (WebSocket): WebSocket connection object.
        """
        await websocket.accept()

        if channel in self.channels:
            self.channels[channel].append(websocket)
        else:
            self.channels[channel] = [websocket]

            await self.pubsub_client.connect()
            pubsub_subscriber = await self.pubsub_client.subscribe(channel)
            self.subscribers.append(pubsub_subscriber)
            asyncio.create_task(self._pubsub_data_reader(pubsub_subscriber))

    async def broadcast_to_channel(self, channel: str, data: str) -> None:
        """
        Broadcasts a data to all connected WebSockets in a room.

        Args:
            channel (str): channel name.
            data (str): data to be broadcasted.
        """
        await self.pubsub_client.connect()
        await self.pubsub_client._publish(channel, data)

    async def disconnect_from_channel(self, channel: str, websocket: WebSocket) -> None:
        """
        Removes a user's WebSocket connection from a room.

        Args:
            channel (str): channel name.
            websocket (WebSocket): WebSocket connection object.
        """
        self.channels[channel].remove(websocket)

        if len(self.channels[channel]) == 0:
            del self.channels[channel]
            await self.pubsub_client.unsubscribe(channel)

    async def _pubsub_data_reader(self, pubsub_subscriber):
        """
        Reads and broadcasts messages received from Redis PubSub.

        Args:
            pubsub_subscriber (aioredis.ChannelSubscribe): PubSub object for the subscribed channel.
        """
        try:
            while True:
                message = await pubsub_subscriber.get_message(
                    ignore_subscribe_messages=True
                )
                if message is not None:
                    channel = message["channel"].decode("utf-8")
                    all_sockets = self.channels[channel]
                    for socket in all_sockets:
                        data = message["data"].decode("utf-8")
                        await socket.send_text(data)
        except Exception:
            pass

    async def send_text(self, message: str, websocket: WebSocket):
        """
        Sends a message to a WebSocket connection.

        Args:
            message (str): Message to be sent.
            websocket (WebSocket): WebSocket connection object.
        """
        await websocket.send_text(message)

    async def close_subscribers(self):
        """
        Closes the Redis PubSub subscribers.
        """
        for subscriber in self.subscribers:
            await subscriber.close()
