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

    def setUp(self):
        self._connections = []

    def tearDown(self):
        for conn in self._connections:
            conn.close()
        self._connections = None

    def _receiveOnce(self, chan: pika.BlockingConnection, queue, auto_ack=True):
        for method, properties, body in chan.consume(queue=queue, auto_ack=auto_ack,
                                                     inactivity_timeout=3.0):
            chan.cancel()
            return method, properties, body

    def _conn(self, timeout=10, sleep=0.1):
        # loop until the container is up
        timeEnd = time.time() + timeout
        while time.time() <= timeEnd:
            try:
                conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
                chan = conn.channel()
                self._connections.append(conn)
                return chan
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

        srvChannel = self._conn()
        srvChannel.queue_declare(queue=queue)
        srvChannel.basic_publish(exchange='', routing_key=queue, body=message)

        clientChannel = self._conn()
        clientChannel.queue_declare(queue=queue)
        method, properties, body = self._receiveOnce(clientChannel, queue=queue)

        self.assertEqual(body, message)
        self.assertNotEqual(method, None)
        self.assertNotEqual(properties, None)

    def testPubSub(self):
        '''Publish a message to a subscriber'''
        exchange = 'pubsub_test_exchange'
        message = b'This is a publication message.'

        srvChannel = self._conn()
        srvChannel.exchange_declare(exchange=exchange, exchange_type='fanout')

        # client
        clientChannel = self._conn()
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

    def testPubSubWithTopics(self):
        '''Publish a message to multiple subscribers, using different topics'''
        exchange = 'pubsub_test_exchange_topics'
        message = b'This is a publication message.'
        # note: 'fanout' is for simple broadcasts, no topics/routing_keys supported
        # 'direct' supports topics, but no wildcards
        exchangeType = 'topic'

        srvChannel = self._conn()
        srvChannel.exchange_declare(exchange=exchange, exchange_type=exchangeType)

        # clients
        clients = []
        topics = []
        for i in range(3):
            clientChannel = self._conn()
            clientChannel.exchange_declare(exchange=exchange, exchange_type=exchangeType)
            # temporary queue, automatically deleted
            result = clientChannel.queue_declare(queue='', exclusive=True)
            queue = result.method.queue
            # subscribe by binding the queue to the exchange
            topic = 'topic.%d' % i
            clientChannel.queue_bind(exchange=exchange, queue=queue, routing_key=topic)
            clients.append((clientChannel, queue, topic))
            topics.append(topic)
        # another client that gets everything
        clientChanAllTopics = self._conn()
        clientChanAllTopics.basic_qos(prefetch_count=100)
        clientChanAllTopics.exchange_declare(exchange=exchange, exchange_type=exchangeType)
        result = clientChanAllTopics.queue_declare(queue='', exclusive=True)
        queueAllTopics = result.method.queue
        clientChanAllTopics.queue_bind(exchange=exchange, queue=queueAllTopics, routing_key='topic.#')

        # now publish multiple messages
        for topic in ['x', 'y'] + topics:
            topic = bytes(topic, 'utf-8')
            srvChannel.basic_publish(exchange=exchange, routing_key=topic, body=message + topic)

        # receive them
        for clientChannel, queue, topic in clients:
            method, properties, body = self._receiveOnce(clientChannel, queue=queue)
            self.assertEqual(method.exchange, exchange)
            topic = bytes(topic, 'utf-8')
            self.assertEqual(body, message + topic)

        expected = topics[:]
        for method, _, body in clientChanAllTopics.consume(queue=queueAllTopics, auto_ack=False,
                                                           inactivity_timeout=3.0):
            topic = expected.pop(0)
            self.assertEqual(method.exchange, exchange)
            topic = bytes(topic, 'utf-8')
            self.assertEqual(body, message + topic)
            clientChanAllTopics.basic_ack(delivery_tag=method.delivery_tag)

            if not expected:
                clientChanAllTopics.cancel()
                break

# TODO: (see https://www.rabbitmq.com/queues.html)
# - request/response (correlation IDs)
# - max. message/queue lifetime -> TTL
#   - queue length limit / policy: https://www.rabbitmq.com/maxlength.html
# - durability
#   - note: also affects publishing! (https://www.rabbitmq.com/publishers.html#message-properties)
# - cluster
#   - https://www.rabbitmq.com/quorum-queues.html
# - custom headers (tags)
# - C/C++ API (with ASIO - check https://github.com/CopernicaMarketingSoftware/AMQP-CPP/issues)
# - list queues / bindings / exchanges using a client
# - https://www.rabbitmq.com/heartbeats.html


if __name__ == '__main__':
    unittest.main()
