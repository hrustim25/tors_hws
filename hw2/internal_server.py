import fastapi
import uvicorn
import time

from logger import logger
import node

internal_router = fastapi.APIRouter()

@internal_router.get('/ping')
def ping():
    logger.debug('Received ping request')
    return {'status': 'ok'}

@internal_router.post('/vote')
def vote(vote_request = fastapi.Body()):
    response = node.node.handle_vote_request(vote_request)
    logger.debug(f'Vote response: {response}')
    return response

@internal_router.post('/log/apply')
def log(log_request = fastapi.Body()):
    response = node.node.handle_log_request(log_request)
    logger.debug(f'Log response: {response}')
    return response

class InternalServer:
    _app: fastapi.FastAPI
    _host: str
    _port: int

    def __init__(self, host: str, port: int):
        self._app = fastapi.FastAPI()
        self._app.include_router(internal_router)
        self._host = host
        self._port = port

    def run_forever(self):
        while True:
            try:
                uvicorn.run(app=self._app, host=self._host, port=self._port)
            except Exception as e:
                logger.error(f'Error during server running: {e}')
            time.sleep(1)
