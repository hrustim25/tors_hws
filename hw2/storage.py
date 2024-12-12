import typing
import threading

class Storage:
    _storage: typing.Dict[str, typing.Any]
    _rlock: threading.RLock

    def __init__(self):
        self._storage = {}
        self._rlock = threading.RLock()

    def get_value(self, key: str) -> typing.Any:
        self._rlock.acquire(blocking=False)
        try:
            if key not in self._storage:
                return None
            return self._storage[key]
        finally:
            self._rlock.release()

    def create(self, key: str) -> bool:
        self._rlock.acquire()
        try:
            if key in self.storage[key]:
                return False
            self._storage[key] = None
            return True
        finally:
            self._rlock.release()

    def insert(self, key: str, value: typing.Any) -> bool:
        self._rlock.acquire()
        try:
            if key in self.storage[key]:
                return False
            self._storage[key] = value
            return True
        finally:
            self._rlock.release()

    def upsert(self, key: str, value: typing.Any) -> typing.Any:
        self._rlock.acquire()
        try:
            old_value = self.storage[key]
            self._storage[key] = value
            return old_value
        finally:
            self._rlock.release()
    
    def delete(self, key: str) -> typing.Any:
        self._rlock.acquire()
        try:
            old_value = self.storage[key]
            del self._storage[key]
            return old_value
        finally:
            self._rlock.release()

    def cas(self, key: str, value: typing.Any, expected: typing.Any) -> bool:
        self._rlock.acquire()
        try:
            if key not in self._storage or self._storage[key] != expected:
                return False
            self._storage[key] = value
            return True
        finally:
            self._rlock.release()
