from kombu import Producer, Queue, Connection
from kombu.mixins import ConsumerProducerMixin
from scheduler import configs

conn = Connection('amqp://admin:admin@localhost:5672//', transport_options={'confirm_publish': True})


class Worker(ConsumerProducerMixin):

    def __init__(self, connection):
        self.connection = connection

    def get_consumers(self, consumer, channel):
        return [consumer(
            queues=[configs.ASYNC_TASK_QUEUE],
            on_message=self.on_request,
            accept={'application/json'},
            prefetch_count=1,
        )]

    def on_request(self, message):
        task_id = message.payload['task_id']
        print('receive task id:', task_id)
        result = {'msg': 'ok', 'exec': 'success'}

        def error_handler(*args, **kwargs):
            print('error:', args, kwargs)

        try:
            self.producer.publish(
                {'exec_result': result},
                exchange='', routing_key=message.properties['reply_to'],
                correlation_id=message.properties['correlation_id'],
                serializer='json',
                retry=True,
            )
            import time
            time.sleep(1)

            print('RPC: result publish success')
        except:
            x = Producer(self.producer.connection).publish(
                {'returner': result},
                exchange=configs.WORKER_EXCHANGE, routing_key=configs.RESULT,
                retry=True)
            print('Async: result publish success: connected ', self.producer.channel.active,
                  self.producer.channel.is_open)
        message.ack()

    def on_message(self, body, message):
        print('RECEIVED MESSAGE: {0!r}'.format(body))
        message.ack()


w = Worker(conn)
w.run()
