"""
Field-Level Conflict Detector for Concurrent Transactions

Implements optimistic concurrency control with field-level granularity:
1. Allows concurrent transactions if they don't have overlapping field accesses
2. Rejects transactions with field conflicts, signaling user to retry

Usage:
    detector = ConflictDetector()
    conflict_info = detector.check_conflict(
        read_fields={'age', 'phone'},
        write_fields={'age'},
        entity='main_records'
    )
    if conflict_info:
        # Conflict detected - tell user to retry
        raise ConflictException(conflict_info)
    
    # No conflict - proceed with transaction
    tx_id = detector.register_transaction(
        read_fields={'age', 'phone'},
        write_fields={'age'},
        entity='main_records'
    )
    
    try:
        # ... perform transaction ...
        detector.commit(tx_id)
    except:
        detector.abort(tx_id)
"""

import threading
import time
import uuid
from typing import Set, Dict, Optional, Tuple, Any
from dataclasses import dataclass, field
from collections import defaultdict


@dataclass
class TransactionInfo:
    """Metadata for an inflight transaction."""
    tx_id: str
    read_fields: Set[str]
    write_fields: Set[str]
    entity: str
    started_at: float
    
    def all_accessed_fields(self) -> Set[str]:
        """Return union of all fields accessed (read or written)."""
        return self.read_fields | self.write_fields


class ConflictDetector:
    """
    Field-level concurrency control with conflict detection.
    
    Detects conflicts based on field overlaps:
    - If transaction A reads field X and transaction B writes field X -> CONFLICT
    - If transaction A writes field X and transaction B writes field X -> CONFLICT
    - If transaction A writes field X and transaction B reads field X -> CONFLICT
    - If both only read field X -> NO CONFLICT
    """
    
    def __init__(self, timeout_seconds: int = 30):
        """
        Initialize the conflict detector.
        
        Args:
            timeout_seconds: Time after which inflight transactions are auto-cleaned.
        """
        self.timeout_seconds = timeout_seconds
        self._lock = threading.RLock()
        self._inflight: Dict[str, TransactionInfo] = {}
        self._entity_txs: Dict[str, Set[str]] = defaultdict(set)
        self._last_cleanup = time.time()
    
    def _cleanup_expired(self):
        """Remove transactions that exceeded timeout (called periodically)."""
        now = time.time()
        if now - self._last_cleanup < 5:  # Only cleanup every 5 seconds
            return
        
        self._last_cleanup = now
        expired = [
            tx_id for tx_id, tx in self._inflight.items()
            if (now - tx.started_at) > self.timeout_seconds
        ]
        
        for tx_id in expired:
            self.abort(tx_id)
    
    def check_conflict(
        self,
        read_fields: Set[str],
        write_fields: Set[str],
        entity: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Check if a new transaction would conflict with existing inflight transactions.
        
        Returns:
            None if no conflict exists
            Dict with conflict info if conflict detected:
                {
                    'conflict': True,
                    'conflicting_tx_id': str,
                    'field_overlap': Set[str],
                    'message': str
                }
        """
        with self._lock:
            self._cleanup_expired()
            
            new_read = read_fields or set()
            new_write = write_fields or set()
            
            # Check against each inflight transaction
            for tx_id, tx in self._inflight.items():
                if tx.entity != entity:
                    continue
                
                # Conflict conditions:
                # 1. New write overlaps with existing write
                write_conflict = bool(new_write & tx.write_fields)
                
                # 2. New write overlaps with existing read
                write_read_conflict = bool(new_write & tx.read_fields)
                
                # 3. New read overlaps with existing write
                read_write_conflict = bool(new_read & tx.write_fields)
                
                if write_conflict or write_read_conflict or read_write_conflict:
                    overlap_fields = (
                        (new_write & tx.write_fields) |
                        (new_write & tx.read_fields) |
                        (new_read & tx.write_fields)
                    )
                    
                    return {
                        'conflict': True,
                        'conflicting_tx_id': tx_id,
                        'conflicting_read_fields': tx.read_fields,
                        'conflicting_write_fields': tx.write_fields,
                        'field_overlap': overlap_fields,
                        'message': (
                            f"Conflict with transaction {tx_id[:8]}: "
                            f"overlapping fields {overlap_fields}. "
                            f"Please retry your query."
                        )
                    }
            
            return None
    
    def register_transaction(
        self,
        read_fields: Optional[Set[str]] = None,
        write_fields: Optional[Set[str]] = None,
        entity: str = 'main_records',
    ) -> str:
        """
        Register a new transaction for conflict tracking.
        
        Args:
            read_fields: Fields being read (from WHERE clause)
            write_fields: Fields being written (from SET clause)
            entity: Table/entity name
        
        Returns:
            Transaction ID for tracking
        """
        with self._lock:
            self._cleanup_expired()
            
            tx_id = str(uuid.uuid4())
            tx_info = TransactionInfo(
                tx_id=tx_id,
                read_fields=read_fields or set(),
                write_fields=write_fields or set(),
                entity=entity,
                started_at=time.time(),
            )
            
            self._inflight[tx_id] = tx_info
            self._entity_txs[entity].add(tx_id)
            
            return tx_id
    
    def commit(self, tx_id: str):
        """Mark transaction as completed and remove from tracking."""
        with self._lock:
            if tx_id in self._inflight:
                tx = self._inflight[tx_id]
                self._entity_txs[tx.entity].discard(tx_id)
                del self._inflight[tx_id]
    
    def abort(self, tx_id: str):
        """Abort transaction and remove from tracking."""
        with self._lock:
            if tx_id in self._inflight:
                tx = self._inflight[tx_id]
                self._entity_txs[tx.entity].discard(tx_id)
                del self._inflight[tx_id]
    
    def get_inflight_transactions(self, entity: str = None) -> Dict[str, TransactionInfo]:
        """Get all currently inflight transactions (for debugging)."""
        with self._lock:
            if entity:
                return {
                    tx_id: self._inflight[tx_id]
                    for tx_id in self._entity_txs.get(entity, set())
                    if tx_id in self._inflight
                }
            return dict(self._inflight)
    
    def clear(self):
        """Clear all tracked transactions (testing/reset)."""
        with self._lock:
            self._inflight.clear()
            self._entity_txs.clear()


# Global singleton detector
_detector = ConflictDetector()


def get_conflict_detector() -> ConflictDetector:
    """Get the global conflict detector instance."""
    return _detector


class ConflictException(Exception):
    """Raised when a field-level conflict is detected during transaction execution."""
    
    def __init__(self, conflict_info: Dict[str, Any]):
        self.conflict_info = conflict_info
        super().__init__(conflict_info.get('message', 'Field conflict detected'))
