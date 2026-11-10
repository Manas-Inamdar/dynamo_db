"""A Dynamo-style distributed key-value store.

Implements the core techniques of Amazon's Dynamo paper (DeCandia et al.,
SOSP 2007): consistent hashing with virtual nodes, quorum replication
(N, R, W), vector-clock versioning with sibling reconciliation, sloppy
quorums with hinted handoff, read repair, and gossip-based membership with
failure detection.
"""

__version__ = "1.0.0"
