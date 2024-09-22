class Operation:
    def __init__(self, op_type: str):
        if op_type == 'r':
            self.operation = 'Read'
        if op_type == 'w':
            self.operation = 'Write'
        if op_type == 'u':
            self.operation = 'Update'
        if op_type == 'c':
            self.operation = 'Commit'

    def get_operation(self) -> str:
        return self.operation
