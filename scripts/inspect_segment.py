#!/usr/bin/env python3
"""
Inspect aeon mmap log segment and index files.

Based on aeon's current write logic:
1) index file entry (8 bytes, little-endian):
   - u32 delta = batch_base_offset - segment_base_offset
   - u32 physical_position
2) log file batch layout (Kafka RecordBatch-like):
   - [0..8)   base_offset      (i64, big-endian)  (overwritten by storage)
   - [8..12)  batch_length     (i32, big-endian)
   - [12..]   rest of record batch bytes
   - total bytes = 12 + batch_length
"""

from __future__ import annotations

import argparse
import os
import struct
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


HEADER_MIN_LEN = 61  # enough to parse RecordBatch header fields


@dataclass
class IndexEntry:
    idx: int
    delta: int
    base_offset: int
    physical_position: int


@dataclass
class BatchInfo:
    idx: int
    position: int
    total_size: int
    base_offset: int
    batch_length: int
    leader_epoch: Optional[int]
    magic: Optional[int]
    crc: Optional[int]
    attributes: Optional[int]
    last_offset_delta: Optional[int]
    first_timestamp: Optional[int]
    max_timestamp: Optional[int]
    producer_id: Optional[int]
    producer_epoch: Optional[int]
    base_sequence: Optional[int]
    records_count: Optional[int]
    parse_error: Optional[str]


def infer_base_offset_from_filename(log_path: str) -> int:
    name = os.path.basename(log_path)
    stem, _ = os.path.splitext(name)
    try:
        return int(stem)
    except ValueError as exc:
        raise ValueError(
            f"Cannot infer base offset from log filename '{name}'. "
            "Use --base-offset explicitly."
        ) from exc


def read_index(index_path: str, base_offset: int) -> List[IndexEntry]:
    entries: List[IndexEntry] = []
    with open(index_path, "rb") as f:
        raw = f.read()

    usable = (len(raw) // 8) * 8
    if usable != len(raw):
        print(
            f"[WARN] index size {len(raw)} is not multiple of 8; "
            f"ignore trailing {len(raw) - usable} bytes."
        )
    raw = raw[:usable]

    for i in range(0, len(raw), 8):
        delta, pos = struct.unpack_from("<II", raw, i)
        entries.append(
            IndexEntry(
                idx=i // 8,
                delta=delta,
                base_offset=base_offset + delta,
                physical_position=pos,
            )
        )
    return entries


def parse_batch_header(buf: bytes, pos: int) -> Tuple[Optional[BatchInfo], int, Optional[str]]:
    # Need at least 12 bytes for [base_offset, batch_length]
    if pos + 12 > len(buf):
        return None, pos, "insufficient bytes for [base_offset,batch_length]"

    base_offset = struct.unpack_from(">q", buf, pos)[0]
    batch_length = struct.unpack_from(">i", buf, pos + 8)[0]
    if batch_length <= 0:
        return None, pos, f"invalid batch_length={batch_length}"

    total = 12 + batch_length
    end = pos + total
    if end > len(buf):
        return None, pos, f"incomplete batch: need end={end}, file_size={len(buf)}"

    body = buf[pos + 12 : end]
    parse_error: Optional[str] = None

    leader_epoch = None
    magic = None
    crc = None
    attributes = None
    last_offset_delta = None
    first_timestamp = None
    max_timestamp = None
    producer_id = None
    producer_epoch = None
    base_sequence = None
    records_count = None

    if len(body) >= (HEADER_MIN_LEN - 12):
        try:
            # Body starts from leader_epoch
            leader_epoch = struct.unpack_from(">i", body, 0)[0]
            magic = struct.unpack_from(">b", body, 4)[0]
            crc = struct.unpack_from(">I", body, 5)[0]
            attributes = struct.unpack_from(">h", body, 9)[0]
            last_offset_delta = struct.unpack_from(">i", body, 11)[0]
            first_timestamp = struct.unpack_from(">q", body, 15)[0]
            max_timestamp = struct.unpack_from(">q", body, 23)[0]
            producer_id = struct.unpack_from(">q", body, 31)[0]
            producer_epoch = struct.unpack_from(">h", body, 39)[0]
            base_sequence = struct.unpack_from(">i", body, 41)[0]
            records_count = struct.unpack_from(">i", body, 45)[0]
        except struct.error as exc:
            parse_error = f"header parse error: {exc}"
    else:
        parse_error = f"batch body too short for full header: {len(body)} bytes"

    info = BatchInfo(
        idx=-1,  # caller fills
        position=pos,
        total_size=total,
        base_offset=base_offset,
        batch_length=batch_length,
        leader_epoch=leader_epoch,
        magic=magic,
        crc=crc,
        attributes=attributes,
        last_offset_delta=last_offset_delta,
        first_timestamp=first_timestamp,
        max_timestamp=max_timestamp,
        producer_id=producer_id,
        producer_epoch=producer_epoch,
        base_sequence=base_sequence,
        records_count=records_count,
        parse_error=parse_error,
    )
    return info, end, None


def scan_log(log_path: str) -> Tuple[List[BatchInfo], Optional[str], int]:
    with open(log_path, "rb") as f:
        raw = f.read()

    batches: List[BatchInfo] = []
    pos = 0
    error: Optional[str] = None

    while pos < len(raw):
        info, next_pos, err = parse_batch_header(raw, pos)
        if err is not None:
            # trailing zeros from preallocation are expected; stop quietly if all-zero tail
            tail = raw[pos:]
            if all(b == 0 for b in tail):
                break
            error = f"scan stopped at pos={pos}: {err}"
            break
        assert info is not None
        info.idx = len(batches)
        batches.append(info)
        pos = next_pos

    return batches, error, len(raw)


def print_summary(
    base_offset: int,
    index_entries: List[IndexEntry],
    batches: List[BatchInfo],
    scan_error: Optional[str],
    log_size: int,
) -> None:
    print("=== Segment Inspect Summary ===")
    print(f"segment_base_offset: {base_offset}")
    print(f"log_file_size:        {log_size} bytes")
    print(f"index_entry_count:    {len(index_entries)}")
    print(f"batch_count_scanned:  {len(batches)}")
    if scan_error:
        print(f"[WARN] {scan_error}")

    if batches:
        first = batches[0].base_offset
        last = batches[-1].base_offset
        print(f"offset_range:         [{first} .. {last}]")
        print(f"next_offset_estimate: {last + 1}")
    else:
        print("offset_range:         <empty>")
        print(f"next_offset_estimate: {base_offset}")

    print()


def print_index(index_entries: List[IndexEntry]) -> None:
    print("=== Index Entries (from .index) ===")
    if not index_entries:
        print("<empty>")
        print()
        return

    print("idx  delta  base_offset  physical_position")
    for e in index_entries:
        print(
            f"{e.idx:>3}  {e.delta:>5}  {e.base_offset:>11}  {e.physical_position:>17}"
        )
    print()


def print_batches(index_entries: List[IndexEntry], batches: List[BatchInfo]) -> None:
    print("=== Batches (from .log scan) ===")
    if not batches:
        print("<empty>")
        print()
        return

    index_pos_to_base: Dict[int, int] = {
        e.physical_position: e.base_offset for e in index_entries
    }
    expected = batches[0].base_offset

    for b in batches:
        indexed_base = index_pos_to_base.get(b.position)
        indexed_mark = "Y" if indexed_base is not None else "N"
        continuous = "Y" if b.base_offset == expected else "N"

        print(
            f"[batch#{b.idx:03}] pos={b.position:>8} size={b.total_size:>6} "
            f"base={b.base_offset:>8} len={b.batch_length:>6} "
            f"records={str(b.records_count):>4} idx={indexed_mark} contiguous={continuous}"
        )
        if indexed_base is not None and indexed_base != b.base_offset:
            print(
                f"  [WARN] index base_offset={indexed_base}, but log base_offset={b.base_offset}"
            )

        print(
            f"        leader_epoch={b.leader_epoch} magic={b.magic} "
            f"last_offset_delta={b.last_offset_delta} attrs={b.attributes}"
        )
        if b.parse_error:
            print(f"        [WARN] {b.parse_error}")

        # estimate next expected base offset
        if b.records_count is not None and b.records_count > 0:
            expected = b.base_offset + b.records_count
        else:
            expected = b.base_offset + 1

    print()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Inspect aeon segment .log + .index and print readable structure."
    )
    parser.add_argument("--log", required=True, help="Path to segment .log file")
    parser.add_argument("--index", required=True, help="Path to segment .index file")
    parser.add_argument(
        "--base-offset",
        type=int,
        default=None,
        help="Segment base offset; default inferred from log filename.",
    )
    parser.add_argument(
        "--no-index",
        action="store_true",
        help="Skip printing index entries (still used for cross-check).",
    )
    args = parser.parse_args()

    if not os.path.exists(args.log):
        raise SystemExit(f"log file not found: {args.log}")
    if not os.path.exists(args.index):
        raise SystemExit(f"index file not found: {args.index}")

    base_offset = (
        args.base_offset
        if args.base_offset is not None
        else infer_base_offset_from_filename(args.log)
    )

    index_entries = read_index(args.index, base_offset)
    batches, scan_error, log_size = scan_log(args.log)

    print_summary(base_offset, index_entries, batches, scan_error, log_size)
    if not args.no_index:
        print_index(index_entries)
    print_batches(index_entries, batches)


if __name__ == "__main__":
    main()

