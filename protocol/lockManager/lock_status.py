from enum import Enum


class LockStatus(Enum):
    GRANTED = "GRANTED"
    WAITING = "WAITING"
    NOT_GRANTED = "NOT_GRANTED"
