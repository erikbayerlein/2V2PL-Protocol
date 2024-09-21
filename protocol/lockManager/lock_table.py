# protocol/lockManager/lock_table.py
from typing import List
from protocol.lockManager.lock_types import LockTypes
from protocol.lockManager.lock import Lock
from protocol.lockManager.lock_status import LockStatus
from protocol.operationManager.operation import Operation
from protocol.graph.graph import Graph

class LockTable:
    def __init__(self):
        self.locks: List[Lock] = []
        self.dependency_graph = Graph()
        self.obj_locks = {}

    @staticmethod
    def _lock_conflict(lock_on_wait: Lock, lock_granted: Lock) -> bool:
        if lock_on_wait.get_object() != lock_granted.get_object():
            return False

        new_type = lock_on_wait.get_type()
        granted_type = lock_granted.get_type()

        conflict_matrix = {
            LockTypes.READ_LOCK: {LockTypes.READ_LOCK: False, LockTypes.WRITE_LOCK: False, LockTypes.CERTIFY_LOCK: True},
            LockTypes.WRITE_LOCK: {LockTypes.READ_LOCK: False, LockTypes.WRITE_LOCK: True, LockTypes.CERTIFY_LOCK: True},
            LockTypes.CERTIFY_LOCK: {LockTypes.READ_LOCK: True, LockTypes.WRITE_LOCK: True, LockTypes.CERTIFY_LOCK: True},
        }

        return conflict_matrix[new_type][granted_type]

    def grant_lock(self, operation: Operation) -> bool:
        lock = Lock(operation)
        transaction_id = operation.get_transaction_id()
        obj = operation.get_object()

        existing_lock = self.get_lock(transaction_id, obj)
        if existing_lock:
            return True

        conflict_found = False
        if obj in self.obj_locks:
            for l in self.obj_locks[obj]:
                if self._lock_conflict(lock, l):
                    conflict_found = True
                    lock.set_status(LockStatus.WAITING)
                    self.locks.append(lock)
                    self.dependency_graph.add_dependency_edge(transaction_id, l.get_transaction_id())
                    break

        if not conflict_found:
            lock.set_status(LockStatus.GRANTED)
            self.locks.append(lock)
            self.obj_locks.setdefault(obj, []).append(lock)
            return True

        else:
            # Deadlock detection can be implemented here
            return False

    def release_lock(self, lock: Lock):
        self.locks.remove(lock)
        lock.set_status(LockStatus.RELEASED)
        # After releasing the lock, try to grant it to waiting transactions
        self.try_grant_waiting_locks(lock.get_object())

    def try_grant_waiting_locks(self, obj):
        waiting_locks = []
        for l in self.locks:
            if l.get_object() == obj and l.get_status() == LockStatus.WAITING:
                waiting_locks.append(l)

        for waiting_lock in waiting_locks:
            conflict = False
            for granted_lock in self.locks:
                if granted_lock.get_object() == obj and granted_lock.get_status() == LockStatus.GRANTED:
                    if self._lock_conflict(waiting_lock, granted_lock):
                        conflict = True
                        break
            if not conflict:
                waiting_lock.set_status(LockStatus.GRANTED)
                # Remove dependency edges since lock is now granted
                self.dependency_graph.remove_dependency_edges(waiting_lock.get_transaction_id())
                # You should also inform the transaction that the lock is now granted

    def get_lock(self, transaction_id, obj):
        for lock in self.locks:
            if lock.get_transaction_id() == transaction_id and lock.get_object() == obj:
                return lock
        return None