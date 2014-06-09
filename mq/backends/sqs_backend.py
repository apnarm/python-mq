import collections
import heapq
import logging
import time
import threading

from Queue import Empty, Full, Queue

from boto import sqs
from boto.sqs.batchresults import BatchResults as BotoBatchResults, ResultEntry
from boto.exception import NoAuthHandlerFound
from boto.sqs.message import Message as Base64Message

from mq.backends import Backend, ConnectionAPI, MessageQueueAPI
from mq.utils import chunk_list


# Limits defined by Amazon - don't change these unless they do.
AWS_MAX_BATCH_SIZE = 10
AWS_MAX_WAIT_TIME = 20
AWS_MAX_VISIBILITY_TIMEOUT = 60 * 60 * 12
AWS_MAX_MESSAGE_SIZE = 1024 * 256


# Use the maximum batch size per API request,
# to reduce our number of API calls.
CHANGE_MESSAGE_VISIBILITY_BATCH_SIZE = AWS_MAX_BATCH_SIZE
DELETE_MESSAGE_BATCH_SIZE = AWS_MAX_BATCH_SIZE
GET_MESSAGE_BATCH_SIZE = AWS_MAX_BATCH_SIZE
WRITE_MESSAGE_BATCH_SIZE = AWS_MAX_BATCH_SIZE

# Use the maximum long polling amount when consuming messages,
# to reduce our number of API calls.
LONG_POLL_TIMEOUT = AWS_MAX_WAIT_TIME

# This will specify the maximum delay between polling requests.
# The delay between poll requests will scale between 0 and the
# defined value, depending on how many messages are returned.
POLL_MAX_DELAY_SECONDS = 2

# This will specify how long we have to process a message once it is received.
# It will be redelivered after this time (unless it is updated or deleted).
VISIBILITY_TIMEOUT = 60 * 3

# This will specify how much padding we allow within the visibility
# timeout before getting worried and extending it again.
VISIBILITY_TIMEOUT_PADDING = 60
SAFE_MAX_VISIBILITY_TIMEOUT = AWS_MAX_VISIBILITY_TIMEOUT - VISIBILITY_TIMEOUT_PADDING

# Example:
# The visibility timeout is 3 minutes, and the padding is 1.5 minutes.
# A message gets consumed but nothing processes it for some reason.
# After 1.5 minutes, the consumer loop will extend the visibility timeout
# by another 3 minutes. Another 1.5 minutes pass, and it gets extended again.


if VISIBILITY_TIMEOUT > SAFE_MAX_VISIBILITY_TIMEOUT:
    # This would just break.
    raise ValueError('The visibility timeout is too high.')


if VISIBILITY_TIMEOUT < LONG_POLL_TIMEOUT:
    # The execution can block for the number of seconds defined by
    # LONG_POLL_TIMEOUT. If that is greater than the message visibility numbers
    # then it will result in messages being redelivered unintentionally.
    raise ValueError('The visibility timeout is too low.')


class QueueNotFound(Exception):
    pass


class BatchResults(BotoBatchResults):
    """
    There is a bug in BatchResults where it's too specific with the name.
    This one is more flexible and works with change_message_visibility_batch.

    """

    def startElement(self, name, attrs, connection):
        if name.endswith('ResultEntry'):
            entry = ResultEntry()
            self.results.append(entry)
            return entry
        if name == 'BatchResultErrorEntry':
            entry = ResultEntry()
            self.errors.append(entry)
            return entry
        return None


def change_message_visibility_batch(connection, queue, messages):
    """This just uses the updated BatchResults to avoid the bug there."""
    params = {}
    for (num, (message, num_seconds)) in enumerate(messages):
        prefix = 'ChangeMessageVisibilityBatchRequestEntry'
        p_name = '%s.%i.Id' % (prefix, num + 1)
        params[p_name] = message.id
        p_name = '%s.%i.ReceiptHandle' % (prefix, num + 1)
        params[p_name] = message.receipt_handle
        p_name = '%s.%i.VisibilityTimeout' % (prefix, num + 1)
        params[p_name] = num_seconds
    return connection.get_object('ChangeMessageVisibilityBatch',
                                 params, BatchResults,
                                 queue.id, verb='POST')


class MockMessage(object):
    """
    A mock class for dealing with messages where only
    the receipt handle is really needed/known.

    """

    def __init__(self, receipt_handle, batch_id=None):
        self.receipt_handle = receipt_handle
        self.id = batch_id


class MessageLengthError(Exception):
    pass


class Message(Base64Message):

    def get_body_encoded(self):
        encoded_body = super(Message, self).get_body_encoded()
        if len(encoded_body) > AWS_MAX_MESSAGE_SIZE:
            try:
                body_info = '%s... (%d bytes)' % (
                    repr(self.get_body())[:100],
                    len(encoded_body),
                )
            except Exception:
                body_info = '%d bytes' % len(encoded_body)
            raise MessageLengthError(
                'Base64 encoded SQS message exceeds AWS limit of %d bytes: %s' % (
                    AWS_MAX_MESSAGE_SIZE,
                    body_info,
                )
            )
        return encoded_body


class VisibilityTimeoutManager(object):

    VisibilityTimeoutItem = collections.namedtuple(
        typename='Item',
        field_names=('wakeup', 'remaining_timeout', 'identifier'),
    )

    def __init__(self, queue):
        self.queue = queue
        self.heap = []
        self.items = {}

    def add(self, visibility_timeout, *message_identifiers):

        # Remove any messages that are being replaced.
        self.remove(*message_identifiers)

        # Wake up after the specified timeout value, and then set the new
        # timeout to be the default VISIBILITY_TIMEOUT amount. Messages
        # which are not dealt with will be repeatedly pushed back so they
        # do not get redelivered.
        wakeup = int(time.time()) + visibility_timeout
        remaining_timeout = VISIBILITY_TIMEOUT

        # The wakeup value is a timestamp. At that point in time, the SQS API
        # should be given a new Visibility Timeout for this message, so that
        # it won't get redelivered. Use the padding value to ensure that
        # action is taken well BEFORE it gets redelivered.
        wakeup -= VISIBILITY_TIMEOUT_PADDING

        # Create items to keep track of each message,
        # and add them to the heapq list.
        for identifier in message_identifiers:
            item = self.VisibilityTimeoutItem(wakeup, remaining_timeout, identifier)
            heapq.heappush(self.heap, item)
            self.items[identifier] = item

    def remove(self, *message_identifiers):

        # Recreate the heapq list without the specified items.
        old_heap, self.heap = self.heap, []
        for item in old_heap:
            if item.identifier not in message_identifiers:
                heapq.heappush(self.heap, item)

        # Remove from the dictionary.
        for message_identifier in message_identifiers:
            if message_identifier in self.items:
                del self.items[message_identifier]

    def clear(self):
        self.heap = []
        self.items.clear()

    def extend_timeouts(self):

        # Repeat until nothing needs extending.
        while self.heap:

            now = int(time.time())

            # The heapq list is sorted so that the next (smallest) wakeup
            # value is first in the list. Use that value to know when to
            # stop extending timeouts.
            if self.heap[0].wakeup > now:
                return

            redeliver_items = []
            remove_identifiers = []
            for item in heapq.nsmallest(CHANGE_MESSAGE_VISIBILITY_BATCH_SIZE, self.heap):

                # Determine when to redeliver this message. Because this loops over
                # a set number of items, it will probably process more items than
                # necessary. For those items, determine how early they are being
                # processed and extend the visibility timeout by that amount.
                num_seconds = item.remaining_timeout
                remaining = item.wakeup - now
                if remaining > 0:
                    num_seconds += remaining

                remove_identifiers.append(item.identifier)
                redeliver_items.append((num_seconds, item.identifier))

            # Remove all of them because only some of them will be redelivered.
            # This has gone crazy and could use a good refactoring.
            self.remove(*remove_identifiers)

            # Extend the visibility of these messages.
            self.queue.redeliver(*redeliver_items)

    def remaining(self, message_identifier):
        if message_identifier in self.items:
            now = int(time.time())
            item = self.items[message_identifier]
            remaining = item.wakeup - now
            if remaining > 0:
                return remaining
        return None


class MessageQueue(MessageQueueAPI):

    def __init__(self, name, connection):
        super(MessageQueue, self).__init__(name, connection)
        self.timeouts = None

    @property
    def _aws_queue_name(self):
        return ('%s-%s' % (
            self.connection.params['queue_name_prefix'],
            self.name,
        )).replace('.', '-')

    @property
    def _boto_queue(self):
        queue = self.connection.get_queue(aws_queue_name=self._aws_queue_name)
        if queue is None:
            raise QueueNotFound('Queue "%s" not found.' % self._aws_queue_name)
        return queue

    @property
    def _boto_connection(self):
        return self.connection.boto_connection

    def ack(self, *message_identifiers):

        if len(message_identifiers) > DELETE_MESSAGE_BATCH_SIZE:
            for message_identifiers in chunk_list(message_identifiers, DELETE_MESSAGE_BATCH_SIZE):
                self.ack(*message_identifiers)
            return

        if len(message_identifiers) == 1:
            receipt_handle = message_identifiers[0]
            message = MockMessage(receipt_handle)
            result = self._boto_queue.delete_message(message)
            assert result, 'Failed to accept %s' % receipt_handle

        elif message_identifiers:
            messages = []
            for num, receipt_handle in enumerate(message_identifiers, start=1):
                message = MockMessage(receipt_handle, batch_id=num)
                messages.append(message)
            result = self._boto_queue.delete_message_batch(messages)
            assert result, 'Result is %r' % result
            assert len(result.errors) == 0, 'Errors are %r' % result.errors
            assert len(result.results) == len(messages), 'Got %r expected roughly %r' % (result.results, message_identifiers)

        if self.timeouts:
            self.timeouts.remove(*message_identifiers)

    @property
    def alive(self):
        return True

    def all(self, no_ack=False, loop_callback=None):

        receipt_handles = []

        try:

            while True:

                messages = self._boto_queue.get_messages(
                    num_messages=GET_MESSAGE_BATCH_SIZE,
                    visibility_timeout=VISIBILITY_TIMEOUT,
                )
                if not messages:
                    break

                if no_ack:

                    for message in messages:
                        receipt_handles.append(message.receipt_handle)
                        yield message.get_body()

                    self.ack(*receipt_handles)
                    receipt_handles = []

                else:

                    for message in messages:
                        yield message.get_body(), message.receipt_handle

                if loop_callback:
                    loop_callback(self)

        except Exception:
            self.stop_consuming()
            raise

        finally:
            if receipt_handles:
                self.ack(*receipt_handles)

    def declare(self):
        self._connection.create_queue(aws_queue_name=self._aws_queue_name)

    def get(self, no_ack=False):
        message = self._boto_queue.read(visibility_timeout=VISIBILITY_TIMEOUT)
        body = message and message.get_body()
        receipt_handle = message and message.receipt_handle
        if no_ack:
            if receipt_handle:
                self.ack(receipt_handle)
            return body
        else:
            return body, receipt_handle

    def nack(self, *message_identifiers):
        """
        If consuming messages, then they are being tracked using the timeout
        manager object. In that case, stop tracking these messages so they
        will get redelivered eventually.

        This will not trigger a message to be redelivered immediately.
        To achieve that, call redeliver with a time of 0, and then call nack.

        """
        if self.timeouts:
            self.timeouts.remove(*message_identifiers)

    def purge(self):
        """
        Tries to remove all messages on the queue. Unfortunately,
        some messages may not be removed due to their visibility timeouts.

        """

        remaining_attempts = 2
        while remaining_attempts:
            messages = list(self.all(no_ack=True))
            if not messages:
                remaining_attempts -= 1

    def put(self, *message_bodies):

        num_messages = len(message_bodies)

        if num_messages == 1:
            message = Message()
            message.set_body(message_bodies[0])
            status = self._boto_queue.write(message)
            assert status

        elif num_messages > WRITE_MESSAGE_BATCH_SIZE:
            for message_bodies in chunk_list(message_bodies, WRITE_MESSAGE_BATCH_SIZE):
                self.put(*message_bodies)

        elif num_messages:
            messages = []
            for num, message_body in enumerate(message_bodies, start=1):
                message_body = Message(body=message_body).get_body_encoded()
                message = (str(num), message_body, 0)
                messages.append(message)
            result = self._boto_queue.write_batch(messages)
            assert result, 'Result is %r' % result
            assert len(result.errors) == 0, 'Errors are %r' % result.errors
            assert len(result.results) == len(messages), 'Got %r expected roughly %r' % (result.results, message_bodies)

    def can_redeliver(self, num_seconds):
        return num_seconds <= SAFE_MAX_VISIBILITY_TIMEOUT

    def _clean_redeliver_items(self, *items):
        for (num_seconds, receipt_handle) in items:
            num_seconds = min(int(num_seconds), SAFE_MAX_VISIBILITY_TIMEOUT)
            if num_seconds and self.timeouts:
                remaining = self.timeouts.remaining(receipt_handle)
                if remaining:
                    if remaining >= num_seconds:
                        # No need to redeliver, the receipt will last long
                        # enough. Unfortunately, redelivery of low values
                        # might be ignored, except for the value 0.
                        continue
            yield (num_seconds, receipt_handle)

    def _redeliver_one(self, item):

        num_seconds, receipt_handle = item

        result = self._boto_connection.change_message_visibility(
            queue=self._boto_queue,
            receipt_handle=receipt_handle,
            visibility_timeout=num_seconds,
        )
        assert result

    def _redeliver_batch(self, *items):

        batch_messages = []

        for num, item in enumerate(items, start=1):

            num_seconds, receipt_handle = item

            message = MockMessage(receipt_handle, batch_id=num)
            message_tuple = (message, num_seconds)
            batch_messages.append(message_tuple)

        result = change_message_visibility_batch(
            connection=self._boto_connection,
            queue=self._boto_queue,
            messages=batch_messages,
        )
        assert result, 'Result is %r' % result
        assert len(result.errors) == 0, 'Errors are %r' % result.errors
        assert len(result.results) == len(batch_messages), 'Got %r expected roughly %r' % (result.results, batch_messages)

    def redeliver(self, *items):
        """
        Set the visibility timeout of messages so they will be redelivered at
        that point in time (unless they are updated or deleted before then).

        The items should be tuples of: (visibility_timeout, message_identifier)

        A message cannot be redelivered before its existing visibility
        timeout. Any attempt to do so will be ignored, except for the
        special case of 0, which will work. This is a limitation of
        this API, not SQS, to avoid making excessive API calls.

        """

        items = tuple(self._clean_redeliver_items(*items))
        num_items = len(items)

        if num_items > CHANGE_MESSAGE_VISIBILITY_BATCH_SIZE:
            for fewer_items in chunk_list(items, CHANGE_MESSAGE_VISIBILITY_BATCH_SIZE):

                num_items = len(fewer_items)

                if num_items == 1:
                    self._redeliver_one(*fewer_items)
                elif num_items:
                    self._redeliver_batch(*fewer_items)

        elif num_items:

            if num_items == 1:
                self._redeliver_one(*items)
            elif num_items:
                self._redeliver_batch(*items)

        if self.timeouts:
            for (num_seconds, receipt_handle) in items:
                self.timeouts.add(num_seconds, receipt_handle)

    def start_consuming(self, callback, no_ack=False, loop_callback=None):

        # A list of receipt handles to be used in batch calls.
        # This reduces the number of API calls and the total cost of SQS.
        receipt_handles = []

        self._consuming = True
        self.timeouts = VisibilityTimeoutManager(self)

        try:

            while self._consuming:

                messages = self._boto_queue.get_messages(
                    num_messages=GET_MESSAGE_BATCH_SIZE,
                    visibility_timeout=VISIBILITY_TIMEOUT,
                    wait_time_seconds=LONG_POLL_TIMEOUT,
                )

                if no_ack:

                    # Pass all messages to the callback function.
                    for message in messages:
                        receipt_handles.append(message.receipt_handle)
                        callback(message.get_body())

                    # And then acknowledge all messages with one call.
                    self.ack(*receipt_handles)
                    receipt_handles = []

                elif messages:

                    watched_receipt_handles = (message.receipt_handle for message in messages)
                    self.timeouts.add(VISIBILITY_TIMEOUT, *watched_receipt_handles)

                    for message in messages:
                        callback(message.get_body(), message.receipt_handle)

                if loop_callback:
                    loop_callback(self)

                self.timeouts.extend_timeouts()

                # Vary the delay between API calls, depending on the number
                # of received messages returned by the previous call.
                # Examples with max set to 2 seconds:
                # Got 10: 0.0s delay
                # Got  9: 0.2s delay
                # Got  5: 1s delay
                # Got  1: 1.8s delay
                # Got  0: 2.0s delay
                missed_messages = GET_MESSAGE_BATCH_SIZE - len(messages)
                if missed_messages:
                    delay = float(missed_messages) / GET_MESSAGE_BATCH_SIZE * POLL_MAX_DELAY_SECONDS
                    time.sleep(delay)

        except Exception:
            self.stop_consuming()
            raise

        finally:
            self.timeouts = None
            if receipt_handles:
                self.ack(*receipt_handles)

    def stop_consuming(self):
        self._consuming = False


class BufferedMessageQueue(MessageQueue):

    def put(self, *message_bodies):
        self.connection.backend._buffer_messages(
            queue_name=self.name,
            message_bodies=message_bodies,
        )


class Connection(ConnectionAPI):
    """
    A connection wrapper for the Boto SQS connection and queues. This has some
    locking around the initialization of some objects, but allows multiple
    threads to access them afterwards. They seem to be thread-safe.

    """

    closed = False

    def __init__(self, *args, **kwargs):
        super(Connection, self).__init__(*args, **kwargs)

        self._lock = threading.RLock()
        self._put_buffers = {}

    @property
    def boto_connection(self):
        with self._lock:
            if not hasattr(self, '_boto_connection'):
                self._boto_connection = sqs.connect_to_region(self.params['region'])
        return self._boto_connection

    def close(self):
        with self._lock:
            if hasattr(self, '_boto_connection'):
                self._boto_connection.close()

    def create_queue(self, aws_queue_name):
        """Create a queue in SQS and return a Boto Queue object for it."""

        # Note: you can't change the default visibility timeout of a queue.
        # It must deleted and created again to change it. This API will specify
        # the timeout value when fetching messages, so it doesn't really matter.

        queue = self.get_queue(aws_queue_name)
        if queue:
            return queue

        with self._lock:

            queue = self.boto_connection.create_queue(
                queue_name=aws_queue_name,
                visibility_timeout=VISIBILITY_TIMEOUT,
            )
            queue.set_message_class(Message)

            # Add to the local dictionary of queues.
            self._queues[aws_queue_name] = queue

        return queue

    def get_queue(self, aws_queue_name=None):
        """Get the queue from SQS and return a Boto Queue object for it."""

        with self._lock:

            if not hasattr(self, '_queues'):
                self._queues = {}
                for queue in self.boto_connection.get_all_queues():
                    queue.set_message_class(Message)
                    self._queues[queue.name] = queue

            if aws_queue_name:
                if aws_queue_name not in self._queues:
                    queue = self.boto_connection.get_queue(aws_queue_name)
                    if queue:
                        queue.set_message_class(Message)
                        self._queues[aws_queue_name] = queue
                return self._queues.get(aws_queue_name)


class SQSBackend(Backend):

    def __init__(self, *args, **kwargs):
        super(SQSBackend, self).__init__(*args, **kwargs)
        self._lock = threading.Lock()
        self._message_buffers = {}

    def _buffer_messages(self, queue_name, message_bodies):

        message_buffer = self._message_buffers.get(queue_name)
        if message_buffer is None:
            with self._lock:
                if queue_name in self._message_buffers:
                    message_buffer = self._message_buffers[queue_name]
                else:
                    message_buffer = Queue(maxsize=WRITE_MESSAGE_BATCH_SIZE)
                    self._message_buffers[queue_name] = message_buffer

        for message_body in message_bodies:
            while True:
                try:
                    message_buffer.put_nowait(message_body)
                except Full:
                    self.flush_buffer(queue_name)
                else:
                    break

    def enable_buffering(self):
        self.message_queue_class = BufferedMessageQueue

    def disable_buffering(self):
        self.message_queue_class = MessageQueue
        self.flush_buffers()

    def flush_buffer(self, queue_name):

        message_buffer = self._message_buffers.get(queue_name)
        if message_buffer is None:
            return

        message_bodies = []
        try:
            for num in xrange(WRITE_MESSAGE_BATCH_SIZE):
                message_bodies.append(message_buffer.get_nowait())
        except Empty:
            pass
        if not message_bodies:
            return

        batch_messages = []
        delay_time = 0
        for batch_id, message_body in enumerate(message_bodies, start=1):
            encoded_message = Message(body=message_body).get_body_encoded()
            batch_messages.append((
                batch_id,
                encoded_message,
                delay_time,
            ))

        with self.open(queue_name) as queue:
            result = queue._boto_queue.write_batch(batch_messages)

        assert result, 'Result is %r' % result
        assert len(result.errors) == 0, 'Errors are %r' % result.errors
        assert len(result.results) == len(batch_messages), 'Got %r expected roughly %r' % (result.results, message_bodies)

    def flush_buffers(self):
        for queue_name in self._message_buffers.keys():
            self.flush_buffer(queue_name)


def create_backend(region, queue_name_prefix):

    # Trigger the boto authentication checks.
    # This does not actually connect.
    try:
        sqs.connect_to_region(region)
    except NoAuthHandlerFound:
        logging.error('Boto is not configured to connect to SQS.')
        raise

    return SQSBackend(
        label='SQS',
        connection_class=Connection,
        connection_params={
            'region': region,
            'queue_name_prefix': queue_name_prefix,
        },
        message_queue_class=MessageQueue,
        connection_errors=(
            QueueNotFound,
        ),
    )
