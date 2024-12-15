import fastapi
import uvicorn

import time
import typing

from logger import logger
import node
import general

router = fastapi.APIRouter()

@router.get('/ping')
async def ping():
    logger.info('Received ping request')
    return {'status': 'ok'}

@router.post('/heartbeat')
async def heartbeat(body = fastapi.Body()):
    node_id = body['node_id']
    logger.info(f'Got heartbeat from {node_id}')
    return {'status': 'ok'}

@router.post('/upsert')
def upsert(key: str, value: typing.Any, response: fastapi.Response):
    is_leader, leader_id = node.node.is_leader()
    if not is_leader:
        response.status_code = fastapi.status.HTTP_302_FOUND
        response.headers.append('Location', node.node.get_node_external_address(leader_id))
        return response
    
    op = general.Operation(type=general.OpType.UPSERT, key=key, value=value)
    result_queue = node.node.add_new_operation(op)
    result = result_queue.get().serialize()
    logger.info(f'Upsert result: {result}')
    return result

class Server:
    _app: fastapi.FastAPI
    _host: str
    _port: int

    def __init__(self, host: str, port: int):
        self._app = fastapi.FastAPI()
        self._app.include_router(router)
        self._host = host
        self._port = port

    def run_forever(self):
        while True:
            try:
                uvicorn.run(app=self._app, host=self._host, port=self._port)
            except Exception as e:
                logger.error(f'Error during server running: {e}')
            time.sleep(1)
