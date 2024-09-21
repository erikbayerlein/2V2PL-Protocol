class Graph:
    def __init__(self):
        self.node_connections = {}

    def add_node(self, node):
        if node not in self.node_connections:
            self.node_connections[node] = set()

    def add_edge(self, source_node, target_node):
        self.add_node(source_node)
        self.add_node(target_node)
        self.node_connections[source_node].add(target_node)

    def has_cycle(self):
        visited_nodes = set()
        recursion_stack = set()

        for node in self.node_connections:
            if self._detect_cycle(node, visited_nodes, recursion_stack):
                return True
        return False

    def _detect_cycle(self, node, visited_nodes, recursion_stack):
        if node in recursion_stack:
            return True
        if node in visited_nodes:
            return False

        visited_nodes.add(node)
        recursion_stack.add(node)

        for neighbor in self.node_connections[node]:
            if self._detect_cycle(neighbor, visited_nodes, recursion_stack):
                return True

        recursion_stack.remove(node)
        return False

    def add_dependency_edge(self, source_node, target_node):
        self._add_edge_with_message(source_node, target_node, "Adding dependency edge")

    def add_operation_edge(self, source_node, target_node, operation):
        self._add_edge_with_message(source_node, target_node, f"Adding edge due to operation: {operation}")

    def _add_edge_with_message(self, source_node, target_node, message):
        print(f"{message} between: {source_node} and {target_node}")
        self.add_edge(source_node, target_node)

    def remove_dependency_edges(self, node):
        if node in self.node_connections:
            del self.node_connections[node]
        for source_node in self.node_connections:
            if node in self.node_connections[source_node]:
                self.node_connections[source_node].remove(node)