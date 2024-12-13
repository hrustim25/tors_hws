import concurrent.futures
import httpx
import threading
import concurrent

from logger import logger

class RequestsSender:
    def __init__(self, retry_count: int = 3):
        self.retry_count = retry_count

    def send_request(self, address: str, json_data, timeout: float):
        for _ in range(3):
            logger.debug(f'Sent request to {address}, data: {json_data} with timeout {timeout}')
            try:
                resp = httpx.post(url=address, json=json_data, timeout=timeout)
                if resp.status_code != 200:
                    return (resp, False)
                return (resp, True)
            except:
                # bad luck
                pass
        logger.debug(f'Failed 3 times to send heartbeat to {address}')
        return (None, False)
