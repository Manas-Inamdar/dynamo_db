# vector_clock.py

"""
A lightweight VectorClock class used by Dynamo.
Supports:
- increment(node_id)
- merge(vclock)
- compare(vclock)
- to_proto() / from_proto()
"""

from dynamo_pb2 import Context, VectorClockItem

class VectorClock:
    def __init__(self, clock=None):
        """
        clock: dict of node_id -> counter
        """
        self.clock = clock or {}

    # ------------------------------------------
    # Increment counter for a given node
    # ------------------------------------------
    def increment(self, node_id):
        self.clock[node_id] = self.clock.get(node_id, 0) + 1

    # ------------------------------------------
    # Merge two vector clocks
    # ------------------------------------------
    def merge(self, other):
        merged = {}
        for node, counter in self.clock.items():
            merged[node] = counter
        for node, counter in other.clock.items():
            merged[node] = max(merged.get(node, 0), counter)
        return VectorClock(merged)

    # ------------------------------------------
    # Comparison rules:
    # self < other  => returns -1
    # self == other => returns 0
    # self > other  => returns +1
    # concurrent    => returns None
    # ------------------------------------------
    def compare(self, other):
        self_dom = False
        other_dom = False

        all_nodes = set(self.clock.keys()) | set(other.clock.keys())

        for node in all_nodes:
            a = self.clock.get(node, 0)
            b = other.clock.get(node, 0)
            if a < b:
                other_dom = True
            elif a > b:
                self_dom = True

        if self_dom and not other_dom:
            return 1
        if other_dom and not self_dom:
            return -1
        if not self_dom and not other_dom:
            return 0
        return None  # concurrent

    # ------------------------------------------
    # Convert to protobuf Context
    # ------------------------------------------
    def to_proto(self):
        ctx = Context()
        for node, counter in self.clock.items():
            item = VectorClockItem(node_id=node, counter=counter)
            ctx.clock.append(item)
        return ctx

    # ------------------------------------------
    # Construct from protobuf
    # ------------------------------------------
    @staticmethod
    def from_proto(ctx):
        clock_dict = {item.node_id: item.counter for item in ctx.clock}
        return VectorClock(clock_dict)

    # ------------------------------------------
    # Pretty print
    # ------------------------------------------
    def __repr__(self):
        return f"VectorClock({self.clock})"
