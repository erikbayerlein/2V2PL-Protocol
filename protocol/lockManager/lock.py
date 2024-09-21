# protocol/lockManager/lock.py
from protocol.lockManager.lock_types import LockTypes
from protocol.lockManager.lock_status import LockStatus
from protocol.operationManager.operation import Operation
from protocol.operationManager.operation_types import OperationTypes


class Lock:
    def __init__(self, operation: Operation):
        self.operation = operation
        self.type = self._determine_lock_type(operation.get_type())
        self.status = LockStatus.NOT_GRANTED
        self.transaction_id = operation.get_transaction_id()
        self.object = operation.get_object()

    def _determine_lock_type(self, operation_type: OperationTypes) -> LockTypes:
        if operation_type == OperationTypes.COMMIT:
            return LockTypes.CERTIFY_LOCK
        elif operation_type == OperationTypes.READ:
            return LockTypes.READ_LOCK
        elif operation_type == OperationTypes.WRITE:
            return LockTypes.WRITE_LOCK

    def get_status(self) -> LockStatus:
        return self.status

    def get_type(self) -> LockTypes:
        return self.type

    def get_operation(self) -> Operation:
        return self.operation

    def get_transaction_id(self):
        return self.transaction_id

    def get_object(self):
        return self.object

    def set_type(self, type):
        self.type = type

    def set_status(self, status: LockStatus):
        self.status = status

    def certify_lock(self):
        self.type = LockTypes.CERTIFY_LOCK
