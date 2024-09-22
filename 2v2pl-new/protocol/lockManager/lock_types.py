from enum import Enum


class LockTypes(Enum):
    READ_LOCK = "RL"
    WRITE_LOCK = "WL"
    CERTIFY_LOCK = "CL"
