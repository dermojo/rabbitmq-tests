#!/usr/bin/env python3

import subprocess
import sys
import time
import unittest
import uuid

import pika

CONTAINER_NAME = 'rabbitmq-test-server'
DOCKER_CLI = ('docker', 'run', '-it', '--rm', '--name', CONTAINER_NAME,
              '-p', '5672:5672', '-p', '15672:15672', 'rabbitmq')


class RabbitMQTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # start a rabbitmq container
        sys.stderr.write('Starting container\n')
        cls._log = open('server.log', 'wt')
        cls._proc = subprocess.Popen(
            DOCKER_CLI, stdout=cls._log, stderr=subprocess.STDOUT)

    @classmethod
    def tearDownClass(cls):
        # stop the container again
        sys.stderr.write('Stopping container\n')
        cls._proc.terminate()
        cls._proc.wait()
        cls._proc = None
        cls._log.close()
        cls._log = None

    def _receiveOnce(self, chan: pika.BlockingConnection, queue, auto_ack=True):
        for method, properties, body in chan.consume(queue=queue, auto_ack=auto_ack):
            chan.cancel()
            return method, properties, body

    def _conn(self, timeout=10, sleep=0.1):
        # loop until the container is up
        timeEnd = time.time() + timeout
        while time.time() <= timeEnd:
            try:
                conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
                chan = conn.channel()
                return conn, chan
            except (pika.exceptions.IncompatibleProtocolError, pika.exceptions.AMQPConnectionError):
                time.sleep(sleep)
        raise RuntimeError('connect failed')

    def _getQueueName(self):
        '''Generates a new random queue name'''
        return str(uuid.uuid4())

    def testHelloWorld(self):
        '''Send & receive a message'''
        queue = self._getQueueName()
        message = b'Hello World!'

        srvConn, srvChannel = self._conn()
        srvChannel.queue_declare(queue=queue)
        srvChannel.basic_publish(exchange='', routing_key=queue, body=message)

        clientConn, clientChannel = self._conn()
        clientChannel.queue_declare(queue=queue)
        method, properties, body = self._receiveOnce(clientChannel, queue=queue)

        self.assertEqual(body, message)
        self.assertNotEqual(method, None)
        self.assertNotEqual(properties, None)

    def testPubSub(self):
        '''Publish a message to a subscriber'''
        exchange = 'pubsub_test_exchange'
        message = b'This is a publication message.'

        srvConn, srvChannel = self._conn()
        srvChannel.exchange_declare(exchange=exchange, exchange_type='fanout')

        # client
        clientConn, clientChannel = self._conn()
        clientChannel.exchange_declare(exchange=exchange, exchange_type='fanout')
        # temporary queue, automatically deleted
        result = clientChannel.queue_declare(queue='', exclusive=True)
        queue = result.method.queue
        # subscribe by binding the queue to the exchange
        clientChannel.queue_bind(exchange=exchange, queue=queue)

        # now publish
        srvChannel.basic_publish(exchange=exchange, routing_key='', body=message)

        method, properties, body = self._receiveOnce(clientChannel, queue=queue)
        self.assertEqual(method.exchange, exchange)
        self.assertEqual(body, message)

# TODO: (see https://www.rabbitmq.com/queues.html)
# - PUB/SUB & filter
# - request/response (correlation IDs)
# - max. message/queue lifetime -> TTL
#   - queue length limit / policy: https://www.rabbitmq.com/maxlength.html
# - durability
#   - note: also affects publishing! (https://www.rabbitmq.com/publishers.html#message-properties)
# - cluster
#   - https://www.rabbitmq.com/quorum-queues.html
# - custom headers (tags)
# - C/C++ API

if __name__ == '__main__':
    unittest.main()
