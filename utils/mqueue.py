import hashlib
import json
import threading
from datetime import datetime as dt
from typing import Any, Callable, Generic, Protocol, TypeVar

import requests
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict
from sqlalchemy import Column, DateTime, String
from sqlalchemy.sql import text as sql

from orcha.utils.sqlalchemy import postgres_scaffold, sqlalchemy_build


class Status:
    class Ack:
        SUCCESS = 'ack_success'
        FAIL = 'ack_failed'

    class RegisterConsumer:
        SUCCESS = 'register_consumer_success'
        FAIL = 'register_consumer_failed'

    class UnregisterConsumer:
        SUCCESS = 'unregister_consumer_success'
        FAIL = 'unregister_consumer_failed'
        NOT_REGISTERED = 'unregister_consumer_not_registered'

    class SendMessage:
        SUCCESS = 'send_message_success'
        FAIL = 'send_message_failed'
        NO_CHANNEL = 'send_message_no_channel'


class SendMessageInput(BaseModel):
    channel: str
    message: str


class SendAckInput(BaseModel):
    message_id: str


class RecieveMessageInput(BaseModel):
    message_id: str
    channel: str
    name: str
    message: str


class RegisterConsumerInput(BaseModel):
    channel: str
    consumer_name: str
    url: str


class UnregisterConsumerInput(BaseModel):
    channel: str
    consumer_name: str


_fastapi_app = FastAPI()


class FastApiApp():
    """
    This is a common FastAPI app instance that is used by the
    Broker and Consumer classes.
    Note:
    - This class cannot be instantiated directly as it uses a
    common FastAPI app instance.
    """

    bind_ip = None
    bind_port = None
    is_running = False

    def __init__(self):
        raise Exception('Cannot instantiate FastApiApp class directly')

    @staticmethod
    def setup(bind_ip: str, bind_port: int):
        # if we've already setup the FastAPI app, then make sure
        # we're trying to start it with the same details as expected
        if FastApiApp.bind_ip is not None:
            if FastApiApp.bind_ip != bind_ip or FastApiApp.bind_port != bind_port:
                raise Exception(f'''
                    FastAPI app already setup with different host and port
                    Existing: {FastApiApp.bind_ip}:{FastApiApp.bind_port}
                    New: {bind_ip}:{bind_port}
                ''')
            else:
                return None

        FastApiApp.bind_ip = bind_ip
        FastApiApp.bind_port = bind_port

    @staticmethod
    def run(in_thread: bool, autostart: bool = True):
        if FastApiApp.bind_ip is None or FastApiApp.bind_port is None:
            raise Exception('FastAPI app not setup')

        if FastApiApp.is_running:
            return None
        else:
            FastApiApp.is_running = True

        host, port = FastApiApp.bind_ip, FastApiApp.bind_port
        if in_thread:
            thread = threading.Thread(
                target=lambda: uvicorn.run(
                    app=_fastapi_app,
                    host=host,
                    port=port
                )
            )
            if autostart:
                thread.start()
            return thread
        else:
            uvicorn.run(_fastapi_app, host=FastApiApp.bind_ip, port=FastApiApp.bind_port)



class Message(Protocol):
    """
    Message protocol to ensure that messages are
    serializable to and from json so they can be
    sent and received from the message queue.
    """
    @classmethod
    def from_json(cls, json_str: str) -> 'Message':
        ...
    def to_json(self) -> str:
        ...

# Generic type for the channel class to ensure that the message type
# can be serialized to and from json and type hinted correctly.
T = TypeVar('T', bound=Message)


class Channel(Generic[T]):
    """
    A class that represents a channel and includes the type of message
    that can be sent on that channel.
    """
    def __init__(self, name: str, message_type: type[T]):
        self.name = name
        self.message_type = message_type


class Consumer():
    """
    Creates a FastAPI application that listens for messages
    from the message queue and calls the appropriate callbacks.
    Note:
    - This class cannot be instantiated directly as it uses a
    common FastAPI app instance.
    """

    consumer_thread = None
    consumer_host = None
    consumer_port = None
    message_callbacks: dict[str, list[Callable[[Channel, Message], Any]]] = {}
    registered_names: set[str] = set()
    broker_host = None
    broker_port = None
    channels: dict[str, Channel] = {}

    def __init__(self):
        raise Exception('Cannot instantiate Consumer class directly')

    @staticmethod
    def setup(
            broker_host: str, broker_port: int,
            consumer_host: str,
            consumer_port = 5800
        ):
        """
        Sets up the Consumer class with the broker and consumer host and port.
        """
        Consumer.broker_host = broker_host
        Consumer.broker_port = broker_port
        Consumer.consumer_host = consumer_host
        Consumer.consumer_port = consumer_port

    @staticmethod
    def run(consumer_bind_ip = '0.0.0.0', consumer_port = 5800):
        """
        Runs the FastAPI app in a separate thread to avoid blocking.
        """
        FastApiApp.setup(consumer_bind_ip, consumer_port)
        Consumer.consumer_thread = FastApiApp.run(in_thread=True)

    @staticmethod
    @_fastapi_app.post('/receive-message')
    def receive_message(data: RecieveMessageInput):
        message_id = data.message_id
        channel_name = data.channel
        channel = Consumer.channels.get(channel_name)
        if not channel:
            raise HTTPException(
                status_code=404,
                detail='Channel not found in Consumer'
            )
        # We don't know the message_type here so trying to decode
        # the message based on the channel name and then raising
        # and if it fails then let the client deal with the mismatch
        try:
            message = channel.message_type.from_json(data.message)
        except json.JSONDecodeError:
            raise HTTPException(
                status_code=400,
                detail='Invalid message format for provided channel name'
            )
        callbacks: list[Callable[[Channel, Message], Any]] = []
        if data.name not in Consumer.registered_names:
            # Make sure the consumer is registered for this channel
            # just a general safety check
            raise HTTPException(
                status_code=404,
                detail='Consumer not registered for this channel'
            )

        if channel_name in Consumer.message_callbacks:
            for callback in Consumer.message_callbacks[channel_name]:
                callbacks.append(callback)

        # process the callbacks in a separate thread to avoid blocking
        # and allow the consumer to complete the http request from the broker
        # NOTE: This is especially important to avoid a deadlock where
        # the broker is waiting for the message connection to close
        # and the consumer is waiting for the broker to respond to the ack
        # so both are waiting on each other and are guaranteed to time out.
        threading.Thread(
            target=Consumer._process_callbacks,
            args=(callbacks, message_id, channel.name, message)
        ).start()

        return 'done'

    @staticmethod
    def _process_callbacks(
            callbacks: list[Callable[[Channel, Message], Any]],
            message_id: str,
            channel: Channel,
            message: Message
        ):
        for callback in callbacks:
            callback(channel, message)
        # Ack out the loop, only need to ack once for the message
        # once all callbacks have been processed
        Consumer.ack_message(message_id)

    @staticmethod
    def register_consumer(
            consumer_name: str, channel: Channel | list[Channel],
            callback: Callable[[Channel, Message], Any]
        ):
        if isinstance(channel, Channel):
            channel = [channel]

        for c in channel:
            if c not in Consumer.message_callbacks:
                Consumer.message_callbacks[c.name] = []

            Consumer.channels[c.name] = c

            Consumer.message_callbacks[c.name].append(callback)
            Consumer.registered_names.add(consumer_name)

            data = RegisterConsumerInput(
                channel=c.name, consumer_name=consumer_name,
                url=f'{Consumer.consumer_host}:{Consumer.consumer_port}/receive-message'
            )
            response = requests.post(
                url=f'{Consumer.broker_host}:{Consumer.broker_port}/register-consumer',
                json=data.model_dump()
            )

            if response.status_code != 200:
                return Status.RegisterConsumer.FAIL

        return Status.RegisterConsumer.SUCCESS

    @staticmethod
    def ack_message(message_id: str):
        data = SendAckInput(message_id=message_id)
        response = requests.post(
            url=f'{Consumer.broker_host}:{Consumer.broker_port}/ack-message',
            json=data.model_dump()
        )

        if response.status_code == 200:
            return Status.Ack.SUCCESS
        return Status.Ack.FAIL


class Producer():
    """
    A class that sends messages to the message queue.
    """
    default_broker_host: str | None = None
    default_broker_port: int | None = None

    def __init__(
            self,
            broker_host: str | None = None,
            broker_port: int | None = None
        ):
        """
        Creates a new Producer instance either using the default class
        level broker host and port or the provided host and port.
        """
        self.broker_host = broker_host if broker_host else Producer.default_broker_host
        self.broker_port = broker_port if broker_port else Producer.default_broker_port

    def send_message(self, channel: Channel, message: Message):
        # make sure the message is of the correct type for the channel
        if not isinstance(message, channel.message_type):
            raise Exception('Message type does not match channel message type')

        data = SendMessageInput(channel=channel.name, message=message.to_json())
        response = requests.post(
            url=f'{self.broker_host}:{self.broker_port}/send-message',
            json=data.model_dump()
        )

        if response.status_code == 200:
            return response.text.replace('"', '')

        return Status.SendMessage.FAIL


class Broker():
    """
    The broker class is responsible for managing the message queue.
    Note:
    - This class cannot be instantiated directly as it uses a
    common FastAPI app instance.
    """

    Base = None

    @staticmethod
    def setup(
        mqueue_pg_host: str,
        mqeue_pg_port: int,
        mqueue_pg_name: str,
        mqueue_pg_user: str,
        mqueue_pg_pass: str,
        mqueue_pg_schema = 'message_queue'
    ) -> None:
        if Broker.Base is not None:
            return None

        Broker.schema = mqueue_pg_schema

        Broker.Base, Broker.engine, Broker.session_maker = postgres_scaffold(
            application_name='mqueue',
            db=mqueue_pg_name,
            server=mqueue_pg_host,
            schema=mqueue_pg_schema,
            user=mqueue_pg_user,
            passwd=mqueue_pg_pass,
        )

        global MessageRecord, ConsumerRecord

        class MessageRecord(Broker.Base):
            __tablename__ = 'messages'
            id = Column(String, primary_key=True)
            created_at = Column(DateTime)
            sent_at = Column(DateTime)
            acked_at = Column(DateTime)
            channel = Column(String)
            consumer_name = Column(String)
            message = Column(String)
            acked = Column(String)
            send_status = Column(String)


        class ConsumerRecord(Broker.Base):
            __tablename__ = 'consumers'
            channel = Column(String, primary_key=True)
            name = Column(String, primary_key=True)
            url = Column(String)

        sqlalchemy_build(Broker.Base, Broker.engine, Broker.schema)

        # populate the in-memory consumers dict
        Broker.consumer_cache = ConsumerCache()
        for consumer in Broker.get_consumers():
            Broker.consumer_cache.add_consumer(
                consumer.channel, consumer.name, consumer.url
            )

    @staticmethod
    def get_consumers():
        with Broker.session_maker.begin() as tx:
            db_consumers = tx.execute(sql('''
                SELECT * FROM message_queue.consumers
            ''')).all()

            # convert to a list of ConsumerItem objects
            consumers = [
                ConsumerItem.model_validate(consumer)
                for consumer in db_consumers
            ]
            return consumers

    @staticmethod
    def run(bind_ip: str, bind_port: int) -> None:
        if Broker.Base is None:
            raise Exception('Broker not setup')
        FastApiApp.setup(bind_ip, bind_port)
        FastApiApp.run(in_thread=True)

    @staticmethod
    @_fastapi_app.post('/register-consumer')
    def register_consumer(data: RegisterConsumerInput):
        channel = data.channel
        consumer_name = data.consumer_name
        url = data.url

        Broker.consumer_cache.add_consumer(channel, consumer_name, url)

        try:
            with Broker.session_maker.begin() as db:
                db.merge(ConsumerRecord(
                    channel=channel, name=consumer_name, url=url
                ))
                db.commit()
                return Status.RegisterConsumer.SUCCESS
        except Exception:
            return Status.RegisterConsumer.FAIL


    @staticmethod
    @_fastapi_app.post('/unregister-consumer')
    def unregister_consumer(data: UnregisterConsumerInput):
        channel = data.channel
        consumer_name = data.consumer_name

        Broker.consumer_cache.remove_consumer(channel, consumer_name)

        try:
            with Broker.session_maker.begin() as db:
                consumer_to_delete = db.query(Consumer).filter_by(
                    channel=channel, name=consumer_name).first()
                if consumer_to_delete:
                    db.delete(consumer_to_delete)
                    return Status.UnregisterConsumer.SUCCESS
                else:
                    return Status.UnregisterConsumer.NOT_REGISTERED
        except Exception:
            return Status.UnregisterConsumer.FAIL


    @staticmethod
    @_fastapi_app.post('/send-message')
    def send_message(data: SendMessageInput):
        consumers = Broker.consumer_cache.get_consumers(data.channel)
        if not consumers:
            return Status.SendMessage.NO_CHANNEL
        channel = data.channel
        message_str = data.message
        send_status = Status.SendMessage.SUCCESS
        send_time = dt.now()
        # We create all messages first, then write them to the db
        # then send them to the consumers.
        # If we send inside the session, the ack comes back and tries to updaet
        # a record that hasn't been written yet
        message_details: list[tuple[str, str, str]] = []
        with Broker.session_maker.begin() as db:
            consumers = Broker.consumer_cache.get_consumers(channel)
            for c in consumers:
                message_id = hashlib.md5(
                    f'{channel}{c.name}{message_str}{send_time}'.encode()
                ).hexdigest()
                message = MessageRecord(
                    id=message_id,
                    created_at=send_time,
                    sent_at=None,
                    acked_at=None,
                    channel=channel,
                    consumer_name=c.name,
                    message=message_str,
                    acked='false',
                    send_status='pending'
                )
                message_details.append((message_id, c.name, c.url))
                db.add(message)
        with Broker.session_maker.begin() as db:
            for id, name, url in message_details:
                r = Broker.send_message_to_consumer(
                    url=url,
                    id=id,
                    channel=channel,
                    name=name,
                    message_str=message_str
                )
                send_status = Status.SendMessage.SUCCESS if r.status_code == 200 else Status.SendMessage.FAIL

                db.execute(sql('''
                    UPDATE message_queue.messages
                    SET sent_at = :sent_at,
                        send_status = :send_status
                    WHERE id = :message_id
                ''').bindparams(
                    sent_at=send_time,
                    send_status=send_status,
                    message_id=id
                ))

        return send_status

    @staticmethod
    def send_message_to_consumer(url, id, channel, name, message_str):
        data = RecieveMessageInput(
            message_id=id,
            channel=channel,
            name=name,
            message=message_str
        )
        response = requests.post(url, json=data.model_dump(), timeout=5)
        return response


    @staticmethod
    @_fastapi_app.post('/ack-message')
    def ack_message(data: SendAckInput):
        with Broker.session_maker.begin() as db:
            # Binding acked_at to the current time
            # to make sure we use python time not db time
            result = db.execute(sql('''
                UPDATE message_queue.messages
                SET acked = :status,
                    acked_at = :acked_at
                WHERE id = :message_id
                RETURNING *
            ''').bindparams(
                status=Status.Ack.SUCCESS,
                acked_at=dt.now(),
                message_id=data.message_id
            ))
            message = result.fetchone()
            if message:
                return Status.Ack.SUCCESS
            else:
                return Status.Ack.FAIL


class MessageItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: str
    created_at: dt
    sent_at: dt
    acked_at: dt
    channel: str
    consumer_name: str
    message: str
    acked: str
    send_status: str


class ConsumerItem(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    channel: str
    name: str
    url: str


class ConsumerCache():
    def __init__(self):
        self.consumers: dict[str, dict[str, ConsumerItem]] = {}

    def has_channel(self, channel: str):
        return channel in self.consumers

    def add_consumer(self, channel: str, name: str, url):
        if channel not in self.consumers:
            self.consumers[channel] = {}

        self.consumers[channel][name] = ConsumerItem(
            channel=channel, name=name, url=url
        )

    def remove_consumer(self, channel: str, name: str):
        if channel in self.consumers:
            self.consumers[channel] = {
                n: self.consumers[channel][n]
                for n in self.consumers[channel]
                if n != name
            }

    def get_consumer(self, channel: str, name: str):
        return self.consumers[channel][name]

    def get_consumers(self, channel: str):
        consumers = self.consumers.get(channel, {})

        return [
            self.consumers[channel][name]
            for name in consumers
        ]
