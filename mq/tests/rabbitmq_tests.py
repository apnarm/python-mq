import unittest

from mq.backends import rabbitmq_backend
from mq.tests import AMQPTestCase, ThreadingTestCase


class RabbitTests(AMQPTestCase, ThreadingTestCase, unittest.TestCase):

    backend = rabbitmq_backend.create_backend()

    def test_put(self):
        """Ensure that the put method works, even with connection failures."""

        with self.backend.open(self.test_queue) as queue:

            # Send some messages while messing with the connection.
            queue.put('hello1')
            queue.connection.channel.close()
            queue.put('hello2')
            queue.connection.close()
            queue.put('hello3')

            # Confirm that the messages went through.
            received = []
            for message, ack in queue:
                received.append(message)
            self.assertEquals(received, ['hello1', 'hello2', 'hello3'])

    def test_put_mandatory(self):
        """
        When putting a message onto a non-existent queue, it should raise an
        exception every few attempts. It seems to almost always happen on the
        3rd attempt but I think I saw it on the 4th attempt this one time.

        """

        with self.backend.open(self.test_queue) as queue:

            # Sending 10 messages to a declared queue should work.
            for x in xrange(10):
                queue.put('hello')

            # But it will break when sending messages to a non-existent queue.
            queue.delete()
            self.__class__.test_queue_declared = False
            for x in xrange(5):
                try:
                    queue.put('hello')
                except self.backend.connection_errors:
                    break
            else:
                self.fail('It was meant to complain about the missing queue.')


if __name__ == '__main__':
    unittest.main()
