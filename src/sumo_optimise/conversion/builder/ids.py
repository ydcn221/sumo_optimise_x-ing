"""Identifier helpers for nodes, edges, and crossings."""
from __future__ import annotations


def main_node_id(direction: str, pos: int) -> str:
    return f"Node.Main.{direction}.{pos}"


def main_edge_id(direction: str, west: int, east: int) -> str:
    return f"Edge.Main.{direction}.{west}-{east}"


def minor_end_node_id(pos: int, ns: str) -> str:
    return f"Node.Minor.{pos}.{ns}-End"


def minor_edge_id(pos: int, to_from: str, ns: str) -> str:
    return f"Edge.Minor.{pos}.{to_from}.{ns}"


def cluster_id(pos: int) -> str:
    return f"Cluster.Main.{pos}"


def crossing_id_minor(pos: int, ns: str) -> str:
    return f"Cross.Minor.{pos}.{ns}"


def crossing_id_main(pos: int, side: str) -> str:
    return f"Cross.Main.{pos}.{side}"


def crossing_id_main_split(pos: int, side: str, direction: str) -> str:
    return f"Cross.Main.{pos}.{side}.{direction}"


def crossing_id_midblock(pos: int) -> str:
    return f"Cross.Mid.{pos}"


def crossing_id_midblock_split(pos: int, direction: str) -> str:
    return f"Cross.Mid.{pos}.{direction}"
