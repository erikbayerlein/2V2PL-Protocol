class Graph:
    def __init__(self):
        self.graph = {}

    def create_nodes(self, transactions):
        created_nodes = []
        for transaction in transactions:
            if transaction[1] not in created_nodes:
                self.add_node(transaction[1].get_transaction())
                created_nodes.append(transaction[1])

    def add_node(self, node):
        if node not in self.graph:
            self.graph[node] = set()

    def has_cycle(self):
        visited_nodes = set()
        recursion_stack = set()

        for node in self.graph:
            if self._detect_cycle(node, visited_nodes, recursion_stack):
                return True
        return False

    def add_edge(self, source_node, target_node):
        self.graph[source_node].add(target_node)

    def remove_dependency_edges(self, node):
        if node in self.graph:
            del self.graph[node]
        for source_node in self.graph:
            if node in self.graph[source_node]:
                self.graph[source_node].remove(node)

    def _detect_cycle(self, node, visited_nodes, recursion_stack):
        if node in recursion_stack:
            return True
        if node in visited_nodes:
            return False

        visited_nodes.add(node)
        recursion_stack.add(node)

        for neighbor in self.graph[node]:
            if self._detect_cycle(neighbor, visited_nodes, recursion_stack):
                return True

        recursion_stack.remove(node)
        return False
