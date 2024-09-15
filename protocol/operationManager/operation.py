import uuid
from datetime import datetime
from .operation_types import OperationTypes


class Operation:
    def __init__(self, operation_type: OperationTypes, transaction_id: int, obj: str = None):
        self.id = uuid.uuid4()
        self.timestamp = datetime.now()
        self.type = operation_type
        self.transaction_id = transaction_id
        self.object = obj


    def get_object(self) -> str:
        return self.object


    def get_transaction_id(self) -> int:
        return self.transaction_id


    def set_object(self, obj: str):
        self.object = obj


    def set_id(self, new_id: uuid.UUID):
        self.id = new_id


    def set_timestamp(self, new_timestamp: datetime):
        self.timestamp = new_timestamp


    def set_type(self, operation_type: OperationTypes):
        self.type = operation_type


    def get_id(self) -> uuid.UUID:
        return self.id


    def get_timestamp(self) -> datetime:
        return self.timestamp
    

    def get_type(self) -> OperationTypes:
        return self.type
