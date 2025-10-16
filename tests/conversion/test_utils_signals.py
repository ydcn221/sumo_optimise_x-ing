"""Tests for signal helper utilities."""
from sumo_optimise.conversion.domain.models import Cluster, EventKind, LayoutEvent, SignalRef
from sumo_optimise.conversion.utils.signals import cluster_has_signal_reference


def _make_event(
    *,
    event_type: EventKind,
    signalized: bool,
    with_signal: bool = True,
) -> LayoutEvent:
    return LayoutEvent(
        type=event_type,
        pos_m_raw=0.0,
        pos_m=0,
        signalized=signalized,
        signal=SignalRef(profile_id="p", offset_s=0) if with_signal else None,
    )


def test_cluster_has_signal_reference_returns_true_for_signalised_event() -> None:
    cluster = Cluster(pos_m=10, events=[_make_event(event_type=EventKind.CROSS, signalized=True)])

    assert cluster_has_signal_reference(cluster) is True


def test_cluster_has_signal_reference_returns_false_when_no_signal_reference() -> None:
    cluster = Cluster(pos_m=10, events=[_make_event(event_type=EventKind.CROSS, signalized=False)])

    assert cluster_has_signal_reference(cluster) is False


def test_cluster_has_signal_reference_requires_signal_object() -> None:
    cluster = Cluster(
        pos_m=10,
        events=[_make_event(event_type=EventKind.TEE, signalized=True, with_signal=False)],
    )

    assert cluster_has_signal_reference(cluster) is False


def test_cluster_has_signal_reference_handles_missing_cluster() -> None:
    assert cluster_has_signal_reference(None) is False
