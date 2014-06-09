import contextlib
import qpid
import socket
import time

from Queue import Empty, Queue

from mq.backends import ExclusiveBackend, ConnectionAPI, MessageQueueAPI


class MessageQueue(MessageQueueAPI):

    _exchange = 'amq.direct'

    def __init__(self, *args, **kwargs):
        super(MessageQueue, self).__init__(*args, **kwargs)
        self.messages = Queue()

    @contextlib.contextmanager
    def _create_incoming_queue(self):
        """
        Create an incoming queue for message consumption. This automatically
        removes the object from Qpid's internal incoming dictionary when
        finished, to avoid a memory leak. The alternative is to reuse UUIDs
        but that could have side effects.

        """

        connection = self.connection
        session = self.connection.session
        uuid = str(qpid.datatypes.uuid4())

        incoming = session.incoming(uuid)
        session.message_subscribe(
            queue=self.name,
            destination=uuid,
        )

        try:
            yield incoming
        finally:
            try:
                incoming.stop()
            except connection.backend.connection_errors:
                pass
            with session.lock:
                try:
                    del session._incoming[uuid]
                except KeyError:
                    pass

    def ack(self, *message_identifiers):
        self.connection.session.message_accept(
            qpid.datatypes.RangedSet(*message_identifiers)
        )

    @property
    def alive(self):
        try:
            self.connection.session.sync()
        except self.connection.backend.connection_errors:
            return False
        else:
            return True

    def all(self, no_ack=False, loop_callback=None):

        self._consuming = True

        def _callback(message):
            self.messages.put(message)

        if no_ack:
            def return_value(message):
                self.ack(message.id)
                return message.body
        else:
            def return_value(message):
                return message.body, message.id

        try:

            with self._create_incoming_queue() as incoming:

                incoming.listen(_callback)
                incoming.start()

                try:
                    # Wait up to 1 second for the first message to arrive.
                    message = self.messages.get(timeout=1)
                except Empty:
                    return
                else:
                    yield return_value(message)

                # Allow another fraction of a second for more
                # messages to be consumed in the background.
                time.sleep(0.1)

                if loop_callback:
                    while self._consuming:
                        try:
                            message = self.messages.get(timeout=0)
                        except Empty:
                            break
                        else:
                            yield return_value(message)
                            loop_callback(self)
                else:
                    while self._consuming:
                        try:
                            message = self.messages.get(timeout=0)
                        except Empty:
                            break
                        else:
                            yield return_value(message)

        except Exception:
            self.stop_consuming()
            raise

    def declare(self):
        session = self.connection.session
        session.queue_declare(queue=self.name)
        session.exchange_bind(
            exchange=self._exchange,
            queue=self.name,
            binding_key=self.name,
        )

    def get(self, no_ack=False):
        for item in self.all(no_ack=no_ack):
            return item

    def purge(self):
        for message in self.all(no_ack=True):
            pass

    def _put_one(self, message_body):
        for first_attempt in (True, False):
            connection = self.connection
            try:
                delivery_properties = connection.session.delivery_properties(
                    routing_key=self.name,
                )
                message = qpid.datatypes.Message(delivery_properties, message_body)
                connection.session.message_transfer(
                    destination=self._exchange,
                    message=message,
                )
                break
            except qpid.session.SessionException:
                connection.close()
                if not first_attempt:
                    raise

    def put(self, *message_bodies):
        for message_body in message_bodies:
            self._put_one(message_body)

    def start_consuming(self, callback, no_ack=False, loop_callback=None):

        self._consuming = True

        if no_ack:
            def send_to_callback(message):
                self.ack(message.id)
                callback(message.body)
        else:
            def send_to_callback(message):
                callback(message.body, message.id)

        try:

            with self._create_incoming_queue() as incoming:

                incoming.listen(send_to_callback)
                incoming.start()

                if loop_callback:
                    while self._consuming:
                        try:
                            message = self.messages.get(timeout=1)
                        except Empty:
                            if not self.alive:
                                self.stop_consuming()
                        else:
                            send_to_callback(message)
                        loop_callback(self)
                else:
                    while self._consuming:
                        try:
                            message = self.messages.get(timeout=1)
                        except Empty:
                            if not self.alive:
                                self.stop_consuming()
                        else:
                            send_to_callback(message)

        except Exception:
            self.stop_consuming()
            raise

    def stop_consuming(self):
        self._consuming = False


class Connection(ConnectionAPI):

    def __init__(self, *args, **kwargs):
        super(Connection, self).__init__(*args, **kwargs)
        self._connection = None
        self._session = None

    @property
    def session(self):
        if self._session is None:
            uuid = str(qpid.datatypes.uuid4())
            session = self.connection.session(uuid)
            self._session = session
        return self._session

    @property
    def connection(self):
        if self._connection is None:

            host = self.params['host']
            port = self.params['port']
            username = self.params['username']
            password = self.params['password']

            qpid_socket = qpid.util.connect(host, port)
            connection = qpid.connection.Connection(
                sock=qpid_socket,
                username=username,
                password=password,
            )
            connection.start()

            self._connection = connection

        return self._connection

    def close(self):
        session = self._session
        if session is not None:
            if session.channel is not None:
                session.close()
            self._session = None
        connection = self._connection
        if connection is not None:
            try:
                connection.close()
            except socket.error:
                pass
            self._connection = None
        self.closed = True


def create_backend(host, port, username=None, password=None):
    return ExclusiveBackend(
        label='Qpid',
        connection_class=Connection,
        connection_params={
            'host': host,
            'port': port,
            'username': username,
            'password': password,
        },
        message_queue_class=MessageQueue,
        connection_errors=(
            qpid.exceptions.Closed,
            qpid.exceptions.Timeout,
            qpid.session.SessionException,
            socket.error,
        ),
    )
