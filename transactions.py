class Transaction:
    def __init__(self, t: str):
        self.transaction = "T"+t

    def get_transaction(self) -> str:
        return self.transaction

    def __repr__(self) -> str:
        return self.transaction
