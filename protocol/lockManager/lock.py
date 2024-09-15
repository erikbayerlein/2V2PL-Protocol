from lock_types import LockTypes
from lock_status import LockStatus
from ..operationManager.operation_types import OperationTypes
from ..operationManager.operation import Operation


class Lock:
    def __init__(self, operation: Operation):
        self.operation = operation
        self.type = self._determine_lock_type(operation.get_type())
        self.status = LockStatus.NOT_GRANTED
        self.transaction_id = operation.get_transaction_id()
        self.object = operation.get_object()


    def _determine_lock_type(self, operation_type: OperationTypes) -> LockTypes:
        if operation_type == OperationTypes.COMMIT:
            return LockTypes.CERTIFY
        elif operation_type == OperationTypes.READ:
            return LockTypes.READ
        elif operation_type == OperationTypes.WRITE:
            return LockTypes.WRITE


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
        self.type = LockTypes.CERTIFY
