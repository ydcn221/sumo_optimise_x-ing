"""Signal-related helper utilities shared across emitters."""
from __future__ import annotations

from typing import Optional

from ..domain.models import Cluster


def cluster_has_signal_reference(cluster: Optional[Cluster]) -> bool:
    """Return ``True`` when a cluster declares at least one signal reference."""

    if cluster is None:
        return False
    return any(event.signalized is True and event.signal is not None for event in cluster.events)


__all__ = ["cluster_has_signal_reference"]
