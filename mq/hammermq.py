"""
This module has a Hammer class, which can be used to stress-test
a message queue backend. It also helps when looking for memory leaks.

This is only really useful for people working on mq backend classes.

"""

import itertools
import resource
import sys
import time
import threading

from boto import sqs
from boto.sqs.message import Message

from mq.utils import time_elapsed


class Hammer(object):

    def __init__(self, message_queue, aws_region=None):
        self.message_queue = message_queue
        self.aws_region = aws_region

    def main(self):
        """Handles the arguments and running of hammermq script."""

        if len(sys.argv) != 2:
            args = '|'.join(sorted(self))
            sys.stderr.write('Usage: %s <%s>\n' % (sys.argv[0], args))
            sys.exit(1)

        command = sys.argv[1]
        if not hasattr(self, command):
            sys.stderr.write('Unknown command: %s\n' % command)
            sys.exit(2)

        getattr(self, command)()

    def __iter__(self):
        for name in dir(self):
            if not name.startswith('_') and name != 'main':
                yield name

    def open(self):
        message_queue = self.message_queue
        for count in itertools.count(1):
            with message_queue.open('test'):
                print 'Loop %d' % count, memory_usage()

    def declare(self):
        message_queue = self.message_queue
        with message_queue.open('test') as queue:
            for count in itertools.count(1):
                queue.declare()
                print 'Loop %d' % count, memory_usage()

    def put(self):
        message_queue = self.message_queue
        with message_queue.open('test') as queue:
            queue.declare()
            for count in itertools.count(1):
                with time_elapsed('Loop %d' % count):
                    queue.put('hammermq')
                print 'Memory:', memory_usage()

    def put_buffered(self):
        message_queue = self.message_queue
        with message_queue.buffer_context():
            self.put()

    def put_threads(self, buffered=False, threads=2):

        # with SQS, threads=2:
        # 28mb at the beginning
        # 29mb quickly
        # 30mb at 200 messages
        # 30mb at 1300 messages
        # 30mb at 2000 messages

        message_queue = self.message_queue

        with message_queue.open('test') as queue:
            queue.declare()

        self.running = True

        counter = itertools.count()
        counter_lock = threading.Lock()

        def putter():
            while self.running:
                with counter_lock:
                    print 'Loop %d' % next(counter)
                with message_queue.open('test') as queue:
                    queue.put('hammermq')
                print 'Memory:', memory_usage()

        def putter_buffered():
            with message_queue.buffer_context():
                putter()

        if buffered:
            target = putter_buffered
        else:
            target = putter

        for x in range(threads):
            thread = threading.Thread(target=target)
            thread.daemon = True
            thread.start()

        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.running = False

    def put_buffered_threads(self):
        self.put_threads(buffered=True)

    def iter(self):
        message_queue = self.message_queue
        with message_queue.open('test') as queue:
            queue.declare()
            for count in itertools.count(1):
                with time_elapsed('Loop %d' % count):
                    for message in itertools.islice(queue, 1):
                        pass
                print 'Memory:', memory_usage()

    def ack(self):
        message_queue = self.message_queue
        with message_queue.open('test') as queue:
            for (count, (message, message_id)) in enumerate(queue):
                with time_elapsed('Loop %d' % count):
                    queue.ack(message_id)
                    print 'Memory:', memory_usage()

    def nack(self):
        message_queue = self.message_queue
        with message_queue.open('test') as queue:
            for count in itertools.count(1):
                with time_elapsed('Loop %d' % count):
                    for message, message_id in queue:
                        queue.nack(message_id)
                print 'Memory:', memory_usage()

    def boto_put(self):

        if not self.aws_region:
            raise ValueError('aws_region')

        queue_name = 'hammermq-test'

        connection = sqs.connect_to_region(self.aws_region)
        queue = connection.create_queue(queue_name)
        queue.set_message_class(Message)

        # stays around 28mb

        for count in itertools.count(1):
            with time_elapsed('Loop %d' % count):

                message = Message()
                message.set_body('test')

                result = queue.write(message)
                assert result
                print 'Memory:', memory_usage()

    def boto_put_threads(self, threads=2):

        # 31mb around the beginning
        # 32mb after roughly 1800 messages
        # 32mb after roughly 4500 messages

        if not self.aws_region:
            raise ValueError('aws_region')

        queue_name = 'hammermq-test'

        connection = sqs.connect_to_region(self.aws_region)
        queue = connection.create_queue(queue_name)
        queue.set_message_class(Message)

        message = Message()
        message.set_body('test')

        self.running = True

        counter = itertools.count()
        counter_lock = threading.Lock()

        def putter():
            while self.running:

                with counter_lock:
                    print 'Loop %d' % next(counter)

                result = queue.write(message)
                assert result

                print 'Memory:', memory_usage()

        for x in range(threads):
            thread = threading.Thread(target=putter)
            thread.daemon = True
            thread.start()

        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.running = False


def memory_usage():
    usage_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    usage_mb = usage_kb / 1024
    return '%dmb' % usage_mb
