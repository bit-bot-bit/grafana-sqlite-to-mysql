#!/usr/bin/env python3
"""Infer dump dependency spread and a suggested apply order."""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from dataclasses import asdict, dataclass
from heapq import heappop, heappush
from typing import Iterable

from modules.parser import extract_insert_table, statement_splitter

_CREATE_TABLE_RE = re.compile(
    r"^\s*CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+(?P<table>[^\s(]+)\s*\((?P<body>.*)\)\s*;?\s*$",
    re.IGNORECASE | re.DOTALL,
)
_REFERENCES_RE = re.compile(
    r"REFERENCES\s+`?(?P<table>[\w$.]+)`?\s*\(",
    re.IGNORECASE,
)
_INSERT_RE = re.compile(
    r"^\s*(?:INSERT\s+OR\s+REPLACE\s+INTO|INSERT\s+INTO|REPLACE\s+INTO|REPLACE)\s+"
    r"(?P<table>[^\s(]+)\s*\((?P<columns>.*?)\)\s*VALUES\s*",
    re.IGNORECASE | re.DOTALL,
)


@dataclass(frozen=True)
class Edge:
    parent: str
    child: str
    source: str
    columns: tuple[str, ...]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Scan a SQL dump for FK-like dependencies and suggest apply order."
    )
    parser.add_argument("--dump-file", required=True, help="Path to .sql dump file")
    parser.add_argument(
        "--tables",
        default="",
        help="Comma-separated table names to include; defaults to all seen tables",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON output")
    return parser


def _normalize_identifier(value: str) -> str:
    value = value.strip()
    if value.startswith("`") and value.endswith("`"):
        value = value[1:-1]
    if value.startswith('"') and value.endswith('"'):
        value = value[1:-1]
    return value.split(".")[-1]


def _parse_csv(raw: str) -> list[str]:
    items: list[str] = []
    current: list[str] = []
    in_backtick = False
    for ch in raw:
        if ch == "`":
            in_backtick = not in_backtick
            current.append(ch)
            continue
        if ch == "," and not in_backtick:
            items.append(_normalize_identifier("".join(current)))
            current = []
            continue
        current.append(ch)
    if current:
        items.append(_normalize_identifier("".join(current)))
    return [item for item in items if item]


def _candidate_parent_names(column: str) -> tuple[str, ...]:
    name = column.lower().strip()
    if name == "id":
        return ()
    suffix = None
    if name.endswith("_id"):
        suffix = "_id"
    elif name.endswith("_uid"):
        suffix = "_uid"
    if suffix is None:
        return ()

    stem = name[: -len(suffix)]
    candidates = {stem}
    if stem.endswith("y"):
        candidates.add(stem[:-1] + "ies")
    candidates.add(stem + "s")
    if stem.endswith("_rule"):
        candidates.add(stem.replace("_rule", "_rule_version"))
    return tuple(candidate for candidate in candidates if candidate)


def _explicit_edges_from_create(statement: str) -> tuple[str | None, list[Edge]]:
    match = _CREATE_TABLE_RE.match(statement)
    if not match:
        return None, []
    child = _normalize_identifier(match.group("table"))
    edges: list[Edge] = []
    for ref_match in _REFERENCES_RE.finditer(match.group("body")):
        parent = _normalize_identifier(ref_match.group("table"))
        if parent == child:
            continue
        edges.append(Edge(parent=parent, child=child, source="explicit", columns=()))
    return child, edges


def _inferred_edges_from_insert(statement: str, known_tables: set[str]) -> tuple[str | None, list[Edge]]:
    match = _INSERT_RE.match(statement)
    if not match:
        return None, []
    child = _normalize_identifier(match.group("table"))
    columns = _parse_csv(match.group("columns"))
    by_parent: dict[str, set[str]] = defaultdict(set)
    for column in columns:
        for parent in _candidate_parent_names(column):
            if parent == child or parent not in known_tables:
                continue
            by_parent[parent].add(column)
    edges = [
        Edge(parent=parent, child=child, source="inferred", columns=tuple(sorted(cols)))
        for parent, cols in sorted(by_parent.items())
    ]
    return child, edges


def analyze_dump(dump_file: str, table_filter: tuple[str, ...]) -> dict[str, object]:
    statements: list[str] = []
    seen_tables: set[str] = set()
    explicit_edges: dict[tuple[str, str], Edge] = {}

    with open(dump_file, "r", encoding="utf-8", errors="replace") as fp:
        for statement, _start, _end in statement_splitter(fp):
            statements.append(statement)
            table = extract_insert_table(statement)
            if table:
                seen_tables.add(table.split(".")[-1])
            create_table, edges = _explicit_edges_from_create(statement)
            if create_table:
                seen_tables.add(create_table)
                for edge in edges:
                    explicit_edges[(edge.parent, edge.child)] = edge
                    seen_tables.add(edge.parent)

    selected_tables = set(table_filter) if table_filter else set(seen_tables)
    edges_by_key: dict[tuple[str, str], Edge] = {
        key: edge
        for key, edge in explicit_edges.items()
        if edge.parent in selected_tables and edge.child in selected_tables
    }

    for statement in statements:
        child, inferred = _inferred_edges_from_insert(statement, seen_tables)
        if not child or child not in selected_tables:
            continue
        for edge in inferred:
            if edge.parent not in selected_tables:
                continue
            key = (edge.parent, edge.child)
            if key in edges_by_key:
                continue
            edges_by_key[key] = edge

    edges = sorted(edges_by_key.values(), key=lambda edge: (edge.parent, edge.child, edge.source))
    parent_to_children: dict[str, set[str]] = defaultdict(set)
    child_to_parents: dict[str, set[str]] = defaultdict(set)
    for table in selected_tables:
        parent_to_children.setdefault(table, set())
        child_to_parents.setdefault(table, set())
    for edge in edges:
        parent_to_children[edge.parent].add(edge.child)
        child_to_parents[edge.child].add(edge.parent)

    order, cycles = _topological_order(selected_tables, parent_to_children, child_to_parents)
    spread = []
    for table in sorted(selected_tables):
        outgoing = sorted(parent_to_children[table])
        incoming = sorted(child_to_parents[table])
        spread.append(
            {
                "table": table,
                "depends_on": incoming,
                "referenced_by": outgoing,
                "depends_on_count": len(incoming),
                "referenced_by_count": len(outgoing),
            }
        )

    return {
        "tables": sorted(selected_tables),
        "edges": [asdict(edge) for edge in edges],
        "spread": spread,
        "suggested_order": order,
        "cycles": cycles,
    }


def _topological_order(
    tables: set[str],
    parent_to_children: dict[str, set[str]],
    child_to_parents: dict[str, set[str]],
) -> tuple[list[str], list[list[str]]]:
    indegree = {table: len(child_to_parents[table]) for table in tables}
    heap: list[str] = []
    for table, degree in indegree.items():
        if degree == 0:
            heappush(heap, table)

    order: list[str] = []
    while heap:
        table = heappop(heap)
        order.append(table)
        for child in sorted(parent_to_children[table]):
            indegree[child] -= 1
            if indegree[child] == 0:
                heappush(heap, child)

    unresolved = sorted(table for table in tables if table not in order)
    cycles: list[list[str]] = []
    if unresolved:
        cycles = _find_cycles(unresolved, parent_to_children)
        order.extend(unresolved)
    return order, cycles


def _find_cycles(nodes: list[str], graph: dict[str, set[str]]) -> list[list[str]]:
    unresolved = set(nodes)
    reverse_graph: dict[str, set[str]] = defaultdict(set)
    for parent in unresolved:
        for child in graph[parent]:
            if child in unresolved:
                reverse_graph[child].add(parent)

    seen: set[str] = set()
    components: list[list[str]] = []
    for start in nodes:
        if start in seen:
            continue
        stack = [start]
        component: set[str] = set()
        while stack:
            node = stack.pop()
            if node in component:
                continue
            component.add(node)
            seen.add(node)
            for nxt in graph[node]:
                if nxt in unresolved:
                    stack.append(nxt)
            for prev in reverse_graph[node]:
                stack.append(prev)
        if len(component) > 1:
            components.append(sorted(component))
    return sorted(components)


def _render_text(result: dict[str, object]) -> str:
    lines = []
    lines.append(f"Tables analyzed: {len(result['tables'])}")
    lines.append("Suggested apply order:")
    for index, table in enumerate(result["suggested_order"], start=1):
        lines.append(f"  {index}. {table}")
    if result["cycles"]:
        lines.append("Cycles detected:")
        for component in result["cycles"]:
            lines.append("  - " + ", ".join(component))
    lines.append("Dependency spread:")
    for item in result["spread"]:
        lines.append(
            "  "
            f"{item['table']}: depends_on={item['depends_on_count']} "
            f"referenced_by={item['referenced_by_count']}"
        )
    lines.append("Edges:")
    for edge in result["edges"]:
        column_text = ",".join(edge["columns"]) if edge["columns"] else "-"
        lines.append(
            f"  {edge['parent']} -> {edge['child']} "
            f"(source={edge['source']} columns={column_text})"
        )
    return "\n".join(lines)


def main(argv: Iterable[str]) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv))
    table_filter = tuple(
        table.strip() for table in str(args.tables or "").split(",") if table.strip()
    )
    result = analyze_dump(args.dump_file, table_filter)
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(_render_text(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
