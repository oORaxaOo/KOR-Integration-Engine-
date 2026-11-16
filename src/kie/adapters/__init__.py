"""Host-registered read-only adapters for KIE."""

from .evidence import EvidenceReadOnlyAdapter, EvidenceRpcTransport

__all__ = ["EvidenceReadOnlyAdapter", "EvidenceRpcTransport"]
