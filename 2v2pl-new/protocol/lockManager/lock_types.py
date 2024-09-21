from enum import Enum


class LockTypes(Enum):
    READ_LOCK = "READ_LOCK"
    WRITE_LOCK = "WRITE_LOCK"
    CERTIFY_LOCK = "CERTIFY_LOCK"
