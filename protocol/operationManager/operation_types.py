from enum import Enum


class OperationTypes(Enum):
    READ = "READ"
    WRITE = "WRITE"
    COMMIT = "COMMIT"
