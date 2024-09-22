import networkx as nx


class Graph:
    def __init__(self):
        self.graph = nx.DiGraph()

    def create_nodes(self, transactions):
        created_nodes = []
        for transaction in transactions:
            if transaction[1] not in created_nodes:
                self.graph.add_node(f"{transaction[1].get_transaction()}")
                created_nodes.append(transaction[1])

    def has_cycle(self):
        return nx.is_directed_acyclic_graph(self.graph)

    def add_edge(self, conflicting_transaction, transaction):
        self.graph.add_edge(conflicting_transaction, transaction)
        print(f"Dependency edge added from {conflicting_transaction} to {transaction}")

    def remove_node(self, aborted_transaction):
        self.graph.remove_node(aborted_transaction)
        print(f"Removing node {aborted_transaction}")

