from ..operationManager.operation import Operation
from ..operationManager.operation_types import OperationTypes


class InputReader:

    @staticmethod
    def read_input(input_str):
        operations_string = InputReader.parse_operations_string(input_str)
        operations_list = []

        for operation in operations_string:
            if operation[0] == 'r':
                current_operation = Operation(OperationTypes.READ, int(operation[1]), operation[3])
            elif operation[0] == 'w':
                current_operation = Operation(OperationTypes.WRITE, int(operation[1]), operation[3])
            elif operation[0] == 'c':
                current_operation = Operation(OperationTypes.COMMIT, int(operation[1]))

            operations_list.append(current_operation)

        return operations_list


    @staticmethod
    def parse_operations_string(input_str):
        operations_string = []
        current_operation_string = []
        is_certify_lock = False

        for s in input_str:
            current_operation_string.append(s)
            if is_certify_lock:
                operations_string.append(''.join(current_operation_string))
                current_operation_string.clear()
                is_certify_lock = False
            elif s == 'c':
                is_certify_lock = True
            elif s == ')':
                operations_string.append(''.join(current_operation_string))
                current_operation_string = []

        return operations_string
