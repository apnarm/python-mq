import contextlib
import functools
import threading


class Backend(object):
    """
    A backend with a single, shared connection. A lock is used to prevent
    multiple connection objects, but multiple threads can access that object
    once it is created.

    Use this when the connection_class is thread-safe.

    """

    label = None
    connection_class = None
    message_queue_class = None

    def __init__(self, label, connection_class, connection_params, message_queue_class, connection_errors):

        self.label = label
        self.connection_class = connection_class
        self.connection_params = connection_params
        self.message_queue_class = message_queue_class
        self.connection_errors = connection_errors

        self._lock = threading.RLock()

    def __str__(self):
        return self.label

    def clone(self):
        return self.__class__(
            self.label,
            self.connection_class,
            self.connection_params,
            self.message_queue_class,
            self.connection_errors,
        )

    @contextlib.contextmanager
    def open(self, queue_name):

        with self._lock:
            if not hasattr(self, '_connection'):
                self._connection = self.connection_class(self, self.connection_params)

        queue = self.message_queue_class(queue_name, self._connection)

        try:
            yield queue
        except self.connection_errors as error:
            self._connection.close()
            raise error
        finally:
            queue.release()

    ############################################################################
    # Subclasses may optionally implement buffering support.
    ############################################################################

    @contextlib.contextmanager
    def buffering(self):
        """
        A context manager that enables bufferring,
        if the backend supports it.

        """
        self.enable_buffering()
        try:
            yield
        finally:
            self.disable_buffering()

    def buffered(self, func):
        """
        A decorator that enables buffering for the decorated function,
        if the backend supports it.

        """

        @functools.wraps(func)
        def buffered_func(*args, **kwargs):
            with self.buffering():
                return func(*args, **kwargs)
        return buffered_func

    def disable_buffering(self):
        pass

    def enable_buffering(self):
        pass

    def flush_buffer(self, queue_name):
        pass

    def flush_buffers(self):
        pass


class ConnectionAPI(object):

    def __init__(self, backend, params):
        self.backend = backend
        self.params = params
        self.closed = False

    ############################################################################
    # Subclasses should implement everything below here.
    ############################################################################

    def close(self):
        raise NotImplementedError


class ExclusiveBackend(Backend):
    """
    Tries to use only one connection per process. If a thread requires
    simultaneous connections, that will be accommodated and then the extra
    connections will remain. That scenario is not expected to occur within
    a web request.

    Use this when the connection_class is not thread-safe.

    """

    @contextlib.contextmanager
    def open(self, queue_name):

        with self._lock:

            try:
                connection = self._connections.pop()
            except AttributeError:
                self._connections = []
                connection = self.connection_class(self, self.connection_params)
            except IndexError:
                connection = self.connection_class(self, self.connection_params)

            queue = self.message_queue_class(queue_name, connection)

            try:
                yield queue
            except self.connection_errors as error:
                connection.close()
                raise error
            finally:
                queue.release()
                if not connection.closed:
                    self._connections.append(connection)


class MessageQueueAPI(object):

    def __init__(self, name, connection):
        self.name = name
        self._connection = connection

    def __iter__(self):
        return self.all()

    @property
    def connection(self):
        connection = self._connection
        if connection is None:
            raise Exception('%r.connection has been released.' % self)
        else:
            return connection

    def release(self):
        self._connection = None

    ############################################################################
    # Subclasses should implement everything below here.
    ############################################################################

    def ack(self, *message_identifiers):
        raise NotImplementedError

    @property
    def alive(self):
        raise NotImplementedError

    def all(self, no_ack=False, loop_callback=None):
        raise NotImplementedError

    def declare(self):
        raise NotImplementedError

    def delete(self):
        raise NotImplementedError

    def get(self, no_ack=False):
        raise NotImplementedError

    def nack(self, *message_identifiers):
        raise NotImplementedError

    def purge(self):
        raise NotImplementedError

    def put(self, message_body):
        raise NotImplementedError

    def redeliver(self, *items):
        """
        Redeliver the items at the specified times. The item values must
        be tuples of (visibility_timeout_in_seconds, message_identifier)

        """
        raise NotImplementedError

    def can_redeliver(self, num_seconds):
        """
        Returns a boolean indicating whether the queue supports
        redelivering a message <num_seconds> in the future.

        """
        raise NotImplementedError

    def start_consuming(self, callback, no_ack=False, loop_callback=None):
        raise NotImplementedError

    def stop_consuming(self):
        raise NotImplementedError
