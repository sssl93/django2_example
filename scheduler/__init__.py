from kombu import Connection
import timeit
import amqp.exceptions
from kombu import Exchange, Queue, uuid, pools
from scheduler import configs
from scheduler.response import ResponseHandler, response_pool, BaseResponse
import asyncio, threading, time


class Scheduler:

    def __init__(self):
        self._connection = Connection('amqp://admin:admin@localhost:5672//',
                                      transport_options={'confirm_publish': True})
        # TODO make sure connection is available
        self.connections = pools.Connections(limit=100)

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            cls._instance = super().__new__(cls)
        return cls._instance

    @staticmethod
    def generate_message(task_id, params):
        return {'task_id': task_id, 'params': params}

    def send_task(self, task_id, params):
        message = self.generate_message(task_id, params)

        with self.connections[self._connection].acquire() as conn:
            with conn.Producer() as producer:
                producer.publish(
                    body=message,
                    exchange=configs.WORKER_EXCHANGE,
                    routing_key=configs.ASYNC_TASK,
                    declare=[configs.ASYNC_TASK_QUEUE, configs.RESULT_QUEUE])

    async def execute_task(self, task_id, params, timeout=10):
        message = self.generate_message(task_id, params)
        correlation_id = uuid()
        response = BaseResponse()
        response_pool.add(correlation_id, response)
        wait_time = time.time() + timeout

        with self.connections[self._connection].acquire() as conn:
            with conn.Producer() as producer:
                print('publish task: ', task_id)
                producer.publish(
                    body=message,
                    exchange=configs.WORKER_EXCHANGE,
                    routing_key=configs.ASYNC_TASK,
                    declare=[configs.RPC_QUEUE],
                    reply_to=configs.RPC_QUEUE.name,
                    correlation_id=correlation_id)

            while response.result is None:
                if time.time() < wait_time:
                    await asyncio.sleep(0)
                else:
                    response_pool.remove(correlation_id)
                    # TODO 检查异常是否能被捕获
                    print('execute task timeout:ID  ', task_id)
                    raise Exception('execute task timeout:ID  ', task_id)

        print('execute successfully.', task_id, response.result)
        response_pool.remove(correlation_id)
        return response.result

    async def handle_task_result(self):

        while True:

            try:
                with self.connections[self._connection].acquire() as conn:

                    # conn.register_with_event_loop(asyncio.get_event_loop())
                    handler = ResponseHandler(conn, [configs.RESULT_QUEUE, configs.RPC_QUEUE])
                    handler.run()

            except Exception as e:
                print('error', e)
                await asyncio.sleep(10)

    async def handle_corn_task(self):
        # TODO corn task scheduling
        import time
        while 1:
            time.sleep(1000)
            print('scheduler corn task')

    def run_forever(self):

        def start_loop(loop):
            asyncio.set_event_loop(loop)
            loop.run_forever()

        for func in [self.handle_corn_task, self.handle_task_result]:
            new_loop = asyncio.new_event_loop()
            threading.Thread(target=start_loop, args=(new_loop,)).start()
            asyncio.run_coroutine_threadsafe(func(), new_loop)


if __name__ == '__main__':
    s = Scheduler()
    s.run_forever()
    loop2 = asyncio.get_event_loop()
    for i in range(5):
        asyncio.run_coroutine_threadsafe(s.execute_task(i, {'mmp': i}), loop2)

    loop2.run_forever()
