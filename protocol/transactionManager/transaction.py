from typing import List
from ..lockManager.lock import Lock
from ..operationManager.operation import Operation


class Transaction:
    def __init__(self, id: int):
        self.id = id
        self.operations: List[Operation] = []
        self.locks: List[Lock] = []


    def get_id(self) -> int:
        return self.id


    def get_operations(self) -> List[Operation]:
        return self.operations


    def get_locks(self) -> List[Lock]:
        return self.locks


    def set_id(self, id: int):
        self.id = id


    def set_operations(self, operations: List[Operation]):
        self.operations = operations
        

    def set_locks(self, locks: List[Lock]):
        self.locks = locks


    def add_operation(self, operation: Operation):
        self.operations.append(operation)


    def add_lock(self, lock: Lock):
        self.locks.append(lock)


    def get_locks_by_obj(self, obj_name: str) -> List[Lock]:
        obj_locks = []
        for lock in self.locks:
            operation = lock.get_operation()
            if obj_name == operation.get_object():
                obj_locks.append(lock)
        return obj_locks
