import fastapi
import uvicorn
import logging
import time

router = fastapi.APIRouter()

@router.get("/ping")
async def ping():
    logging.info('Received ping request')
    return {"status": "ok"}

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
                logging.error(f'Error during server running: {e}')
            time.sleep(1)
