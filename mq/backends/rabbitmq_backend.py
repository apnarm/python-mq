import pika
import select

from pika.exceptions import AMQPError, AMQPConnectionError, ChannelClosed, ConnectionClosed

from mq.backends import ExclusiveBackend, ConnectionAPI, MessageQueueAPI


INTERRUPTED_SYSTEM_CALL_ERROR = 4


class BlockingConnection(pika.BlockingConnection):

    def channel(self, *args, **kwargs):
        """
        Fix this to clean up closed channels. The normal Connection class
        does this, but BlockingConnection does not. I don't know why,
        it seems like a bug to me.

        """
        channel = super(BlockingConnection, self).channel(*args, **kwargs)
        self._add_channel_callbacks(channel.channel_number)
        return channel

    def disconnect(self):
        # Required until pika 0.9.14 is released.
        # See https://github.com/pika/pika/issues/426
        self._adapter_disconnect()


class MessageQueue(MessageQueueAPI):

    PERSISTENT_MESSAGE = pika.BasicProperties(delivery_mode=2)

    def __init__(self, *args, **kwargs):
        super(MessageQueue, self).__init__(*args, **kwargs)
        self._messages = []

    def ack(self, *message_identifiers):
        channel = self.connection.channel
        for delivery_tag in message_identifiers:
            channel.basic_ack(delivery_tag=delivery_tag)

    @property
    def alive(self):
        return self.connection.connection.is_open

    def all(self, no_ack=False, loop_callback=None):

        self._consuming = True

        connection = self.connection
        channel = connection.channel
        pika_connection = channel.connection

        # Start consuming messages and add them to the message list.
        messages = self._messages
        if no_ack:
            def consumer_callback(channel, method, properties, body):
                messages.append(body)
        else:
            def consumer_callback(channel, method, properties, body):
                messages.append((body, method.delivery_tag))
        consumer_tag = channel.basic_consume(
            consumer_callback=consumer_callback,
            queue=self.name,
            no_ack=no_ack,
        )

        try:

            # Keeping checking for received messages and stop if there are none.
            def poll_for_empty_queue():
                if messages:
                    pika_connection.add_timeout(0.01, poll_for_empty_queue)
                else:
                    self.stop_consuming()
            pika_connection.add_timeout(0.01, poll_for_empty_queue)

            # Now run the consumer and read messages out of the message list.
            if loop_callback:
                while self._consuming:
                    pika_connection.process_data_events()
                    loop_callback(self)
                    if messages:
                        for message in messages:
                            yield message
                        self._messages = messages = []
            else:
                while self._consuming:
                    pika_connection.process_data_events()
                    if messages:
                        for message in messages:
                            yield message
                        self._messages = messages = []

        except Exception:

            self.stop_consuming()
            raise

        finally:

            # Stop consuming messages in the background.
            try:
                channel.basic_cancel(consumer_tag)
            except connection.backend.connection_errors:
                pass

    def declare(self):
        self.connection.channel.queue_declare(queue=self.name, durable=True)

    def delete(self):
        try:
            self.connection.channel.queue_delete(queue=self.name)
        except ChannelClosed as error:
            error_code = error.args[0]
            if error_code != 404:
                raise

    def get(self, no_ack=False):

        channel = self.connection.channel
        method_frame, properties, body = channel.basic_get(
            queue=self.name,
            no_ack=no_ack,
        )

        if no_ack:
            return body
        else:
            delivery_tag = method_frame and method_frame.delivery_tag
            return body, delivery_tag

    def purge(self):
        self.connection.channel.queue_purge(queue=self.name)

    def _put_one(self, message_body):
        for first_attempt in (True, False):
            connection = self.connection
            try:
                if connection.connection.is_closed:
                    raise ConnectionClosed
                sent = connection.channel.basic_publish(
                    exchange='',
                    routing_key=self.name,
                    body=message_body,
                    properties=self.PERSISTENT_MESSAGE,
                    mandatory=True,
                )
                break
            except (ChannelClosed, ConnectionClosed):
                connection.close()
                if not first_attempt:
                    raise
        if not sent:
            raise AMQPError('Failed to put message on %r queue.' % self.name)

    def put(self, *message_bodies):
        for message_body in message_bodies:
            self._put_one(message_body)

    def release(self):
        """This is called when the message queue API context is finished."""

        if hasattr(self, '_consuming'):
            # This has been used for consuming. There is a problem with pika
            # where it keeps every queue reply (channel._replies) and never
            # cleans them up. Close the channel here, forcing it to clean up,
            # and a new one will be generated as needed.
            try:
                self.connection._close_channel()
            except self.connection.backend.connection_errors:
                pass
            del self._consuming

        super(MessageQueue, self).release()

    def start_consuming(self, callback, no_ack=False, loop_callback=None):

        self._consuming = True

        connection = self.connection
        channel = connection.channel
        pika_connection = channel.connection

        # Consume messages and pass them to the provided callback.
        if no_ack:
            def _callback(channel, method, properties, body):
                callback(body)
        else:
            def _callback(channel, method, properties, body):
                callback(body, method.delivery_tag)
        consumer_tag = channel.basic_consume(
            consumer_callback=_callback,
            queue=self.name,
            no_ack=no_ack,
        )

        try:

            # Keep consuming until another thread asks for it to stop.
            try:
                if loop_callback:
                    while self._consuming:
                        pika_connection.process_data_events()
                        loop_callback(self)
                else:
                    while self._consuming:
                        pika_connection.process_data_events()
            except select.error as error:
                if error.args[0] == INTERRUPTED_SYSTEM_CALL_ERROR:
                    raise SystemExit
                else:
                    raise

        except Exception:

            self.stop_consuming()
            raise

        finally:

            # Stop consuming messages in the background.
            try:
                channel.basic_cancel(consumer_tag)
            except connection.backend.connection_errors:
                pass

    def stop_consuming(self):
        self._consuming = False


class Connection(ConnectionAPI):

    def __init__(self, *args, **kwargs):
        super(Connection, self).__init__(*args, **kwargs)
        self._channel = None
        self._connection = None

    def _close_channel(self):
        channel = self._channel
        if channel is not None:
            try:
                channel.close()
            except self.backend.connection_errors:
                pass
            self._channel = None

    def _close_connection(self):
        connection = self._connection
        if connection is not None:
            if not connection.is_closed:
                try:
                    connection.close()
                except self.backend.connection_errors:
                    pass
            self._connection = None

    @property
    def channel(self):
        if self._channel is None:
            self._channel = self.connection.channel()
        return self._channel

    @property
    def connection(self):
        if self._connection is None:
            self._connection = BlockingConnection(self.params)
        return self._connection

    def close(self):
        self._close_channel()
        self._close_connection()
        self.closed = True


def create_backend(host, port):
    return ExclusiveBackend(
        label='RabbitMQ',
        connection_class=Connection,
        connection_params=pika.ConnectionParameters(
            host=host,
            port=port,
        ),
        message_queue_class=MessageQueue,
        connection_errors=(
            AMQPError,
            AMQPConnectionError,
            ChannelClosed,
            ConnectionClosed,
        ),
    )
