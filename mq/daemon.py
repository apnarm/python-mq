# TODO: get this to work, since the parent Daemon class is being
# left out of the initial migration to GitHub.

import contextlib
import logging
import random
import signal
import sys
import threading
import time

from Queue import Queue


class ConsumerDaemon(object):

    def __init__(self, message_queue, queue_name, message_handler,
                 pid_file_name=None, status_server=None):

        raise NotImplementedError('the parent Daemon class has been left out')

        self.message_queue = message_queue
        self.queue_name = queue_name
        self.message_handler = message_handler
        self.trigger_reconnect = False

        signal_map = {
            signal.SIGINT: self.immediate_shutdown,
            signal.SIGTERM: self.graceful_shutdown,
        }

        super(ConsumerDaemon, self).__init__(
            signal_map=signal_map,
            pid_file_name=pid_file_name,
            status_server=status_server,
        )

    def graceful_shutdown(self, signum=None, frame=None):
        """Act upon a signal and arrange for the daemon to stop nicely."""
        self.keep_running_flag = False
        logging.info('Received a graceful shutdown request')

    def immediate_shutdown(self, signum=None, frame=None):
        """Act upon a signal and immediately exit."""
        logging.error('Received a terminate immediately request')
        sys.exit(1)

    def reconnect(self):
        logging.debug('Closing queue connection on purpose.')
        self.trigger_reconnect = True

    def on_error(self, error):
        # Subclasses can implement code to run here after an
        # exception gets caught. E.g. close database connections.
        logging.info('Sleeping for 15 seconds...')
        time.sleep(15)

    def run(self):

        # Run everything inside a loop, even if things break,
        # until specifically told to stop running.
        while self.keep_running_flag:
            try:
                self.declare_queue()
                self.consume_and_process_messages()
                time.sleep(1)
            except SystemExit:
                raise
            except self.message_queue.connection_errors as error:
                logging.error('%s.%s: %s' % (
                    error.__class__.__module__,
                    error.__class__.__name__,
                    error,
                ))
                self.on_error(error)
            except Exception as error:
                logging.exception()
                self.on_error(error)

        logging.info('ConsumerDaemon stopped.')

    def consume_and_process_messages(self):

        with self.message_queue.open(self.queue_name) as queue:

            # Keep trying to read messages and send them to the message handler.
            while self.keep_running_flag:

                for message_body, message_id in queue:
                    self.message_handler(message_body, message_id, queue)
                    if not self.keep_running_flag or self.trigger_reconnect:
                        break

                if not self.trigger_reconnect:
                    # The alive property calls "sync" when using the Qpid
                    # backend, which helps to keep the connection alive.
                    if queue.alive:
                        time.sleep(5)
                    else:
                        logging.warning('%s connection check failed, reconnecting.' % self.message_queue)
                        self.trigger_reconnect = True

                if self.trigger_reconnect:
                    self.trigger_reconnect = False
                    queue.connection.close()
                    break

    def declare_queue(self):
        try:
            with self.message_queue.open(self.queue_name) as queue:
                queue.declare()
        except Exception:
            logging.warning('Failed to declare queue %r' % self.queue_name)
            raise
        else:
            logging.info('Declared queue %r' % self.queue_name)


class WorkerScheduler(object):
    """
    Handle thread scheduling in a fair way. A worker thread uses the wait()
    context, while the main thread repeatedly calls next() to allow workers
    to continue.

    This lets all threads schedule their work in the order they requested,
    and limits threads to be scheduled only once at a time. So in theory,
    this should allow multiple streams of work to be processed without
    any one thread unfairly blocking the others.

    """

    def __init__(self, workers):
        self.worker_queue = Queue()
        self.worker_role = threading.Semaphore(workers)

    def next(self):
        work_request = self.worker_queue.get(block=True)
        with self.worker_role:
            work_request.set()
        self.worker_queue.task_done()

    @contextlib.contextmanager
    def wait(self):
        work_request = threading.Event()
        self.worker_queue.put(work_request)
        work_request.wait()
        with self.worker_role:
            yield


class MultiThreadedConsumerDaemon(ConsumerDaemon):
    """
    Uses 1 thread per queue. Each thread will always be consuming messages,
    but only the [worker_thread_count] number of workers will be processing
    messages at any one point in time.

    """

    def __init__(self, message_queue, queue_names, message_handler,
                 pid_file_name=None, status_server=None, worker_thread_count=3):

        self.message_queue = message_queue
        self.queue_names = queue_names
        self.message_handler = message_handler
        self.worker_thread_count = worker_thread_count
        self.trigger_reconnect = False

        # Map signals to these methods
        signal_map = {
            signal.SIGINT: self.immediate_shutdown,
            signal.SIGTERM: self.graceful_shutdown,
        }

        super(ConsumerDaemon, self).__init__(
            signal_map=signal_map,
            pid_file_name=pid_file_name,
            status_server=status_server,
        )

    @contextlib.contextmanager
    def exception_handling(self):
        try:
            yield
        except SystemExit:
            raise
        except self.message_queue.connection_errors as error:
            logging.error('%s.%s: %s' % (
                error.__class__.__module__,
                error.__class__.__name__,
                error,
            ))
            self.on_error(error)
        except Exception as error:
            logging.exception()
            self.on_error(error)

    def run(self):

        self.keep_running_flag = True

        self.worker_scheduler = WorkerScheduler(self.worker_thread_count)

        for queue_name in self.queue_names:
            thread = threading.Thread(
                target=self.consume_from_queue,
                args=(queue_name,),
            )
            thread.daemon = True
            thread.start()

        while self.keep_running_flag:
            self.worker_scheduler.next()

    def consume_from_queue(self, queue_name):

        # Clone the message queue object to allow
        # simultaneous use from multiple threads.
        message_queue = self.message_queue.clone()

        # Run everything inside a loop, even if things break,
        # until specifically told to stop running.
        while self.keep_running_flag:
            with self.exception_handling():
                with message_queue.open(queue_name) as queue:

                    # Declare the queue once.
                    try:
                        queue.declare()
                    except Exception:
                        logging.warning('Failed to declare queue %r' % queue_name)
                        raise
                    else:
                        logging.info('Declared queue %r' % queue_name)

                    # And then start consuming messages and processing them.
                    self.process_messages(queue)

            time.sleep(1)

    def process_messages(self, queue):
        """Keep consuming messages and send them to the message handler."""

        while self.keep_running_flag:

            message_item = None

            for message_body, message_id in queue:

                with self.worker_scheduler.wait():
                    self.message_handler(message_body, message_id, queue)

                if not self.keep_running_flag or self.trigger_reconnect:
                    break

            if not self.trigger_reconnect:
                # The alive property calls "sync" when using
                # qpid, which helps to ensure that we have a
                # working connection.
                if queue.alive:
                    if message_item is None:
                        # Sleep for a random amount of time, to avoid
                        # having every thread working at the same time.
                        sleep_time = 5 + (random.random() * 5)
                        time.sleep(sleep_time)
                else:
                    logging.warning('%s connection check failed, reconnecting.' % self.message_queue)
                    self.trigger_reconnect = True

            if self.trigger_reconnect:
                self.trigger_reconnect = False
                queue.connection.close()
                break
