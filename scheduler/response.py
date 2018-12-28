from kombu.mixins import ConsumerMixin, Consumer


class BaseResponse:
    def __init__(self):
        self.result = None


class ResponsePool:
    pool = {}

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            cls._instance = super().__new__(cls)
        return cls._instance

    def get(self, correlation_id):
        return self.pool.get(correlation_id)

    def add(self, correlation_id, response):
        self.pool[correlation_id] = response

    def remove(self, correlation_id):
        del self.pool[correlation_id]

    def pop(self, correlation_id):
        return self.pool.pop(correlation_id)


response_pool = ResponsePool()


class ResponseHandler(ConsumerMixin):

    def __init__(self, connection, queues):
        print(connection.heartbeat)
        self.connection = connection
        self.queues = queues

    def get_consumers(self, Consumer, channel):
        return [Consumer(
            on_message=self.on_message,
            queues=self.queues,
            no_ack=True), ]

    @staticmethod
    def on_message(msg):
        print('on_message: ',msg.payload)
        correlation_id = msg.properties.get('correlation_id')
        if correlation_id:
            response = response_pool.pool.get(correlation_id)
            if isinstance(response, BaseResponse):
                response.result = msg.payload

