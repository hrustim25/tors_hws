import typing
import threading

import general
from logger import logger

class Storage:
    _storage: typing.Dict[str, typing.Any]
    _lock: threading.Lock

    def __init__(self):
        self._storage = {}
        self._lock = threading.RLock()
    
    def apply_operation(self, op: general.Operation) -> general.OperationResult:
        if op.type == general.OpType.CREATE:
            success = self.create(op.key)
            return general.OperationResult(success=success, value=None)
        elif op.type == general.OpType.READ:
            success, value = self.get_value(op.key)
            return general.OperationResult(success=success, value=value)
        elif op.type == general.OpType.UPDATE:
            success, value = self.update(op.key, op.value)
            return general.OperationResult(success=success, value=value)
        elif op.type == general.OpType.DELETE:
            success, value = self.delete(op.key)
            return general.OperationResult(success=success, value=value)
        elif op.type == general.OpType.UPSERT:
            value = self.upsert(op.key, op.value)
            return general.OperationResult(success=True, value=value)
        elif op.type == general.OpType.CAS:
            success, value = self.cas(op.key, op.value, op.expected)
            return general.OperationResult(success=success, value=value)
        else:
            logger.warning(f'Unknown storage operation: {op.type}')
            return general.OperationResult(success=False, value=None)

    def get_value(self, key: str) -> typing.Tuple[bool, typing.Any]:
        self._lock.acquire()
        try:
            if key not in self._storage:
                return False, None
            return True, self._storage[key]
        finally:
            self._lock.release()

    def create(self, key: str) -> bool:
        self._lock.acquire()
        try:
            if key in self._storage:
                return False
            self._storage[key] = None
            return True
        finally:
            self._lock.release()

    def update(self, key: str, value: typing.Any) -> typing.Tuple[bool, typing.Any]:
        self._lock.acquire()
        try:
            if key not in self._storage:
                return False, None
            old_value = self._storage[key]
            self._storage[key] = value
            return True, old_value
        finally:
            self._lock.release()

    def upsert(self, key: str, value: typing.Any) -> typing.Any:
        self._lock.acquire()
        try:
            old_value = None
            if key in self._storage:
                old_value = self._storage[key]
            self._storage[key] = value
            return old_value
        finally:
            self._lock.release()

    def delete(self, key: str) -> typing.Tuple[bool, typing.Any]:
        self._lock.acquire()
        try:
            if key not in self._storage:
                return False, None
            old_value = self._storage[key]
            del self._storage[key]
            return True, old_value
        finally:
            self._lock.release()

    def cas(self, key: str, value: typing.Any, expected: typing.Any) -> typing.Tuple[bool, typing.Any]:
        self._lock.acquire()
        try:
            if key not in self._storage:
                return False, None
            old_value = self._storage[key]
            if old_value != expected:
                return False, self._storage[key]
            self._storage[key] = value
            return True, old_value
        finally:
            self._lock.release()
