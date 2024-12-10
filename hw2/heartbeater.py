import concurrent.futures
import typing
import threading
import concurrent

import httpx

from logger import logger

class Heartbeater:
    current_node_id: str
    hosts: typing.Dict[str, str] # node_id, address
    heartbeat_time: float
    heartbeat_timeout: float
    heartbeat_timer: threading.Timer
    lock: threading.Lock

    def __init__(self, current_node_id: str, hosts: typing.Dict[str, str], heartbeat_time: float, heartbeat_timeout: float):
        self.current_node_id = current_node_id
        self.hosts = hosts
        self.heartbeat_time = heartbeat_time
        self.heartbeat_timeout = heartbeat_timeout
        self.heartbeat_timer = threading.Timer(self.heartbeat_time, self._send_heartbeat_task)
        self.lock = threading.Lock()

    def _send_heartbeat(self, host):
        for _ in range(3):
            logger.info(f'Sent heartbeat to {host}')
            try:
                resp = httpx.post(url=f'{host}/heartbeat', json={'node_id': self.current_node_id}, timeout=self.heartbeat_timeout)
                if resp.status_code == 200:
                    return (host, True)
            except:
                # bad luck
                pass
        logger.warning(f'Failed 3 times to send heartbeat to {host}')
        return (host, False)


    def _send_heartbeat_task(self):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for host in self.hosts.values():
                futures.append(executor.submit(self._send_heartbeat, host))
            alive_count = 1
            for f in concurrent.futures.as_completed(futures):
                host, is_alive = f.result()
                if is_alive:
                    alive_count += 1
            logger.info(f'Alive nodes {alive_count}/{len(self.hosts) + 1}')
        # restart
        self.start_heartbeat_timer()

    def start_heartbeat_timer(self):
        with self.lock:
            self.heartbeat_timer.cancel()
            self.heartbeat_timer = threading.Timer(self.heartbeat_time, self._send_heartbeat_task)
            self.heartbeat_timer.start()

    def stop_heartbeat_timer(self):
        self.heartbeat_timer.cancel()
