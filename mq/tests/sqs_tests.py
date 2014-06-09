import boto
import datetime
import time
import unittest

from mq.backends import sqs_backend
from mq.tests import AMQPTestCase, ThreadingTestCase


class SQSTests(AMQPTestCase, ThreadingTestCase, unittest.RollbackTestCase):

    backend = sqs_backend.create_backend()

    def test_put(self):
        """Ensure that the put method works, even with connection failures."""

        with self.backend.open(self.test_queue) as queue:

            # Send some messages while messing with the connection.
            queue.put('hello1')
            queue.connection._boto_connection.close()
            queue.put('hello2')
            sent = set(['hello1', 'hello2'])

            # Confirm that the messages went through.
            received = set()
            for message in queue.all(no_ack=True):
                received.add(message)
            self.assertEquals(sent, received)

    def test_large_message(self):

        # This string will be AWS_MAX_MESSAGE_SIZE characters log after
        # it is base64 encoded, which is what happens to all SQS messages.
        longest_allowed_string = 'a' * 196608

        with self.backend.open(self.test_queue) as queue:

            queue.put(longest_allowed_string)

            try:
                try:
                    queue.put(longest_allowed_string + 'a')
                except sqs_backend.MessageLengthError:
                    pass
                else:
                    self.fail('This message was too large but no error was raised!')
            finally:
                # Consume messages put onto the queue.
                list(queue.all(no_ack=True))

    def MANUAL_test_redeliver_bad(self):

        sqs_backend.SAFE_MAX_VISIBILITY_TIMEOUT = sqs_backend.AWS_MAX_VISIBILITY_TIMEOUT - 1

        with self.backend.open(self.test_queue) as queue:

            print 'PURGE'
            queue.purge()

            boto.set_stream_logger('tellme')

            print '-' * 80
            print 'Add Message'
            print '-' * 80
            queue.put('hello9')

            time.sleep(1)

            print '-' * 80
            print 'Get Message'
            print '-' * 80
            message_body, message_id = queue.get()
            self.assertEqual(message_body, 'hello9')

            print '-' * 80
            print 'Redeliver Message %d seconds' % sqs_backend.SAFE_MAX_VISIBILITY_TIMEOUT
            print '-' * 80
            queue.redeliver((sqs_backend.SAFE_MAX_VISIBILITY_TIMEOUT, message_id))

            time.sleep(5)

            print '-' * 80
            print 'Redeliver Message %d seconds' % sqs_backend.SAFE_MAX_VISIBILITY_TIMEOUT
            print '-' * 80
            queue.redeliver((sqs_backend.SAFE_MAX_VISIBILITY_TIMEOUT, message_id))

    def MANUAL_test_redeliver_good(self):

        sqs_backend.SAFE_MAX_VISIBILITY_TIMEOUT = sqs_backend.AWS_MAX_VISIBILITY_TIMEOUT - 1

        with self.backend.open(self.test_queue) as queue:

            print 'PURGE'
            queue.purge()

            boto.set_stream_logger('tellme')

            print '-' * 80
            print 'Add Message'
            print '-' * 80
            queue.put('hello5')

            time.sleep(1)

            print '-' * 80
            print 'Get Message'
            print '-' * 80

            message_body, message_id = queue.get()
            self.assertEqual(message_body, 'hello5')

            print '-' * 80
            print 'Redeliver Message 5 seconds'
            print '-' * 80

            queue.redeliver((5, message_id))

            time.sleep(6)

            print '-' * 80
            print 'Get Message'
            print '-' * 80

            message_body, message_id = queue.get()
            self.assertEqual(message_body, 'hello5')

            print '-' * 80
            print 'Redeliver Message %d seconds' % sqs_backend.SAFE_MAX_VISIBILITY_TIMEOUT
            print '-' * 80

            queue.redeliver((sqs_backend.SAFE_MAX_VISIBILITY_TIMEOUT, message_id))

    def MANUAL_test_timeouts(self):

        sqs_backend.VISIBILITY_TIMEOUT = 10
        sqs_backend.VISIBILITY_TIMEOUT_PADDING = 5

        sqs_backend.LONG_POLL_TIMEOUT = 5

        with self.backend.open(self.test_queue) as queue:

            queue.purge()
            queue.put('hello1')

            def callback(message, receipt):
                print datetime.datetime.now().strftime('%H:%M:%S'), message, receipt
                queue.redeliver((30, receipt))

            queue.start_consuming(callback)


if __name__ == '__main__':
    unittest.main()
