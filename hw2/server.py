import fastapi
import uvicorn
import time

from logger import logger

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
