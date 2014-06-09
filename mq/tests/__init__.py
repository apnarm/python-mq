import threading


class AMQPTestCase(object):

    backend = None

    test_queue = 'mq.tests'
    test_queue_declared = False

    def setUp(self):
        if not self.__class__.test_queue_declared:
            with self.backend.open(self.test_queue) as queue:
                queue.declare()
                queue.purge()
            self.__class__.test_queue_declared = True

    def _generate_messages(self, number):
        """Put a bunch of messages on the queue."""
        sent = set()
        with self.backend.open(self.test_queue) as queue:
            for num in xrange(number):
                message = 'hello%d' % num
                queue.put(message)
                sent.add(message)
        self.assertEquals(len(sent), number)
        return sent

    def _confirm_empty(self, queue):
        """Confirm that the queue is empty."""
        message = queue.get(no_ack=True)
        self.assertEquals(message, None)

    def test_close(self):
        """Confirm that closing a connection is safe and recoverable."""
        with self.backend.open(self.test_queue) as queue:
            queue.put('hello1')
            queue.connection.close()
            queue.connection.close()
            queue.connection.close()
            queue.put('hello2')
            queue.connection.close()
            queue.connection.close()
            queue.connection.close()
            received = set(message for (message, messageid) in queue)
            self.assertEquals(set(('hello1', 'hello2')), received)

    def test_consume(self):
        """Test the behavior of consuming with a callback."""

        sent, received = self._generate_messages(10), set()

        with self.backend.open(self.test_queue) as queue:

            def callback(message):
                received.add(message)
                # Stop the test when we have all of the messages.
                # Otherwise it will block/wait forever.
                if len(received) == len(sent):
                    queue.stop_consuming()

            # Starting the consumer will block here until the
            # callback function decides to call stop_consuming.
            queue.start_consuming(callback, no_ack=True)

            # Confirm that the queue is now empty.
            self._confirm_empty(queue)

        self.assertEquals(sent, received)

    def test_get(self):

        sent, received = self._generate_messages(10), set()

        with self.backend.open(self.test_queue) as queue:

            for x in xrange(len(sent)):
                received.add(queue.get(no_ack=True))

            # Confirm that the queue is now empty.
            self._confirm_empty(queue)

        self.assertEquals(sent, received)

    def test_iter(self):
        """Test iterating over a queue to consume messages."""

        sent, received = self._generate_messages(10), set()

        with self.backend.open(self.test_queue) as queue:

            for message, message_id in queue:
                received.add(message)
                queue.ack(message_id)

            self._confirm_empty(queue)

        self.assertEqual(sent, received)


class ThreadingTestCase(object):

    backend = None

    def test_threads(self):
        """
        Test the multi-threaded use of a sending and receiving from a queue."""

        num_threads = 5
        send_per_thread = 50
        total = num_threads * send_per_thread

        sent = set()
        received = set()

        # Prepare the queues.
        queue_names = []
        for num in xrange(num_threads):
            queue_name = 'mq.tests.thread%d' % num
            with self.backend.open(queue_name) as queue:
                queue.declare()
                queue.purge()
            queue_names.append(queue_name)

        def _send_messages(queue_name):
            """Send all of the messages using one queue context."""
            messages = []
            for num in xrange(send_per_thread):
                message = 'hello %s %d' % (queue_name, num)
                messages.append(message)
            with self.backend.open(queue_name) as queue:
                queue.put(*messages)
            sent.update(messages)

        # Create and run the send threads.
        threads = []
        for queue_name in queue_names:
            thread = threading.Thread(target=_send_messages, args=(queue_name,))
            threads.append(thread)

        [thread.start() for thread in threads]
        [thread.join() for thread in threads]

        def _consume_messages(queue_name):
            with self.backend.open(queue_name) as queue:
                count = 0
                for message in queue.all(no_ack=True):
                    count = count + 1
                    received.add(message)

        # Create and run the consumer threads.
        threads = []
        for queue_name in queue_names:
            thread = threading.Thread(target=_consume_messages, args=(queue_name,))
            threads.append(thread)

        [thread.start() for thread in threads]
        [thread.join() for thread in threads]

        self.assertEquals(len(sent), total)
        self.assertEquals(sent, received)

    def test_nested(self):
        """Test two connections simultaneously."""

        num_messages = 10
        sent = set()
        received = set()

        # Prepare the queues.
        name1 = 'mq.tests.1'
        name2 = 'mq.tests.2'
        for name in (name1, name2):
            with self.backend.open(name) as queue:
                queue.declare()
                queue.purge()

        # Add some messages to a queue.
        with self.backend.open(name1) as queue:
            for num in xrange(num_messages):
                message = 'hello %d' % num
                queue.put(message)
                sent.add(message)

        # Transfer messages from one queue to another.
        # This forces two simultaneous connections.
        with self.backend.open(name1) as queue1:
            with self.backend.open(name2) as queue2:
                message_ids = []
                for message, message_id in queue1:
                    queue2.put(message)
                    message_ids.append(message_id)
                queue1.ack(*message_ids)

        # Consume some messages from a queue.
        with self.backend.open(name2) as queue:

            def callback(message):
                received.add(message)
                if len(received) == num_messages:
                    queue.stop_consuming()

            queue.start_consuming(callback, no_ack=True)

        self.assertEqual(sent, received)
