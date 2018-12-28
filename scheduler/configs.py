from kombu import Exchange, Queue
import socket, uuid


def generate_rpc_key():
    ip = ''
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
        s.close()
    finally:
        return f'{ip}({uuid.uuid1().hex[:8]})'


ASYNC_TASK = 'ops.async_task'
RPC = f'ops.rpc.{generate_rpc_key()}'
RESULT = 'ops.result'

WORKER_EXCHANGE = Exchange('ops.worker', 'topic')
ASYNC_TASK_QUEUE = Queue(ASYNC_TASK, exchange=WORKER_EXCHANGE, routing_key=ASYNC_TASK)
RPC_QUEUE = Queue(RPC, exchange=WORKER_EXCHANGE, routing_key=RPC, auto_delete=True)
RESULT_QUEUE = Queue(RESULT, exchange=WORKER_EXCHANGE, routing_key=RESULT)
