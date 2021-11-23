#!/usr/bin/env python3

import subprocess
import time
import unittest

import pika

CONTAINER_NAME = 'rabbitmq-test-server'
DOCKER_CLI = ('docker', 'run', '-it', '--rm', '--name', CONTAINER_NAME,
              '-p', '5672:5672', '-p', '15672:15672', 'rabbitmq')


class RabbitMQTests(unittest.TestCase):

    def setUp(self):
        # start a rabbitmq container
        self._log = open('server.log', 'wt')
        self._proc = subprocess.Popen(DOCKER_CLI, stdout=self._log, stderr=subprocess.STDOUT)

    def tearDown(self):
        # stop the container again
        self._proc.terminate()
        self._proc.wait()
        self._proc = None
        self._log.close()
        self._log = None
    
    def _receiveOnce(self, chan : pika.BlockingConnection, queue, auto_ack=True):
        for method, properties, body in chan.consume(queue=queue, auto_ack=auto_ack):
            chan.cancel()
            return method, properties, body

    def _conn(self):
        # loop until the container is up
        for _ in range(100):
            try:
                conn = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
                chan = conn.channel()
                return conn, chan
            except (pika.exceptions.IncompatibleProtocolError, pika.exceptions.AMQPConnectionError):
                time.sleep(0.1)
        raise RuntimeError('connect failed')

    def testHelloWorld(self):
        '''Send & receive a message'''
        queue = 'hello'
        message = b'Hello World!'

        srvConn, srvChannel = self._conn()
        srvChannel.queue_declare(queue=queue)
        srvChannel.basic_publish(exchange='', routing_key=queue, body=message)

        clientConn, clientChannel = self._conn()
        clientChannel.queue_declare(queue=queue)
        method, properties, body = self._receiveOnce(clientChannel, queue=queue)
        srvConn.close()
        clientConn.close()
        
        self.assertEqual(body, message)
        self.assertNotEqual(method, None)
        self.assertNotEqual(properties, None)


if __name__ == '__main__':
    unittest.main()
