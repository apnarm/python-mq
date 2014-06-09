import unittest

from mq.backends.qpid_backend import create_backend
from mq.tests import AMQPTestCase, ThreadingTestCase


class QpidTests(AMQPTestCase, ThreadingTestCase, unittest.RollbackTestCase):

    backend = create_backend()

    def test_put(self):
        """Ensure that the put method works, even with connection failures."""

        with self.backend.open(self.test_queue) as queue:

            # Send some messages while messing with the connection.
            queue.put('hello1')
            queue.connection.session.close()
            queue.put('hello2')
            queue.connection.close()
            queue.put('hello3')

            # Confirm that the messages went through.
            received = []
            for message, message_id in queue:
                received.append(message)
                queue.ack(message_id)
            self.assertEquals(received, ['hello1', 'hello2', 'hello3'])


if __name__ == '__main__':
    unittest.main()
