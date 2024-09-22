class Transaction:
    def __init__(self, t: str):
        self.transaction = f'T{t}'
        self.t = t

    def get_transaction(self) -> str:
        return self.transaction

    def __repr__(self) -> str:
        return f'{self.transaction}'
