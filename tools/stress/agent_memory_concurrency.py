#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import shutil
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

SCHEMA_STATEMENTS = [
    "CREATE NODE TABLE IF NOT EXISTS agent(id INT64 PRIMARY KEY, name STRING);",
    "CREATE NODE TABLE IF NOT EXISTS memory_session(id INT64 PRIMARY KEY, agentID INT64, startedAt INT64);",
    "CREATE NODE TABLE IF NOT EXISTS message(id INT64 PRIMARY KEY, sessionID INT64, role STRING, content STRING);",
    "CREATE NODE TABLE IF NOT EXISTS entity(id INT64 PRIMARY KEY, name STRING);",
    "CREATE NODE TABLE IF NOT EXISTS fact(id INT64 PRIMARY KEY, entityID INT64, confidence DOUBLE, body STRING);",
    "CREATE REL TABLE IF NOT EXISTS agent_has_session(FROM agent TO memory_session, MANY_MANY);",
    "CREATE REL TABLE IF NOT EXISTS session_has_message(FROM memory_session TO message, MANY_MANY);",
    "CREATE REL TABLE IF NOT EXISTS message_mentions_entity(FROM message TO entity, MANY_MANY);",
    "CREATE REL TABLE IF NOT EXISTS fact_supported_by_message(FROM fact TO message, MANY_MANY);",
]


@dataclass
class StressState:
    lock: threading.Lock = field(default_factory=threading.Lock)
    allocated_sessions: int = 0
    committed_sessions: int = 0
    writer_attempts: int = 0
    writer_retries: int = 0
    reader_queries: int = 0
    checkpoints: int = 0
    checkpoint_errors: int = 0
    invariant_failures: int = 0
    write_latencies_ms: list[float] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stress concurrent Kuzu writes with an AI-agent memory graph workload."
    )
    parser.add_argument("--db-path", type=Path, default=None)
    parser.add_argument("--keep-db", action="store_true")
    parser.add_argument("--agents", type=int, default=4)
    parser.add_argument("--entities", type=int, default=64)
    parser.add_argument("--writers", type=int, default=4)
    parser.add_argument("--readers", type=int, default=2)
    parser.add_argument("--messages-per-session", type=int, default=4)
    parser.add_argument("--duration-seconds", type=float, default=60.0)
    parser.add_argument("--ops-per-writer", type=int, default=0)
    parser.add_argument("--id-base", type=int, default=0)
    parser.add_argument("--auto-checkpoint", dest="auto_checkpoint", action="store_true", default=False)
    parser.add_argument("--no-auto-checkpoint", dest="auto_checkpoint", action="store_false")
    parser.add_argument("--checkpoint-threshold", type=int, default=16 * 1024)
    parser.add_argument("--manual-checkpoint-interval", type=float, default=0.0)
    parser.add_argument("--reopen-verify-interval", type=float, default=0.0)
    parser.add_argument("--retry-limit", type=int, default=5)
    parser.add_argument("--retry-backoff-ms", type=float, default=25.0)
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def import_kuzu() -> Any:
    try:
        import kuzu
    except ImportError as exc:
        raise SystemExit(
            "Unable to import kuzu. Build/install the Python package or set PYTHONPATH to the "
            "built package before running this stress tool."
        ) from exc
    return kuzu


def close_result(result: Any) -> None:
    if isinstance(result, list):
        for item in result:
            item.close()
    else:
        result.close()


def execute(conn: Any, query: str, parameters: dict[str, Any] | None = None) -> None:
    result = conn.execute(query, parameters or {})
    close_result(result)


def scalar(conn: Any, query: str, parameters: dict[str, Any] | None = None) -> Any:
    result = conn.execute(query, parameters or {})
    try:
        row = result.get_next()
        return row[0]
    finally:
        result.close()


def maybe_row(conn: Any, query: str) -> list[Any] | None:
    result = conn.execute(query)
    try:
        if not result.has_next():
            return None
        return result.get_next()
    finally:
        result.close()


def open_db(kuzu: Any, args: argparse.Namespace) -> Any:
    return kuzu.Database(
        database_path=args.db_path,
        auto_checkpoint=args.auto_checkpoint,
        checkpoint_threshold=args.checkpoint_threshold,
    )


def configure_runtime(conn: Any) -> None:
    execute(conn, "CALL force_checkpoint_on_close=false;")


def setup_database(kuzu: Any, args: argparse.Namespace) -> Any:
    db = open_db(kuzu, args)
    conn = kuzu.Connection(db)
    try:
        configure_runtime(conn)
        for statement in SCHEMA_STATEMENTS:
            execute(conn, statement)
        current = scalar(conn, "CALL current_setting('concurrent_writes') RETURN *;")
        if current != "True":
            raise RuntimeError(f"concurrent_writes default is {current!r}, expected 'True'")
        ensure_seed_rows(conn, "agent", args.agents, "CREATE (:agent {id: $id, name: $name});", "agent")
        ensure_seed_rows(
            conn,
            "entity",
            args.entities,
            "CREATE (:entity {id: $id, name: $name});",
            "entity",
        )
    finally:
        conn.close()
    return db


def reopen_and_verify(kuzu: Any, db: Any, args: argparse.Namespace, state: StressState) -> Any:
    db.close()
    db = open_db(kuzu, args)
    conn = kuzu.Connection(db)
    try:
        configure_runtime(conn)
        verify_exact_counts(conn, state.committed_sessions, args.messages_per_session)
    finally:
        conn.close()
    return db


def checkpoint_and_verify(kuzu: Any, db: Any, args: argparse.Namespace, state: StressState) -> Any:
    conn = kuzu.Connection(db)
    try:
        execute(conn, "CHECKPOINT;")
        with state.lock:
            state.checkpoints += 1
        verify_exact_counts(conn, state.committed_sessions, args.messages_per_session)
    finally:
        conn.close()
    return reopen_and_verify(kuzu, db, args, state)


def ensure_seed_rows(conn: Any, label: str, expected: int, create_query: str, name_prefix: str) -> None:
    existing = scalar(conn, f"MATCH (n:{label}) RETURN COUNT(n);")
    if existing == expected:
        return
    if existing != 0:
        raise RuntimeError(f"Existing {label} count is {existing}, expected 0 or {expected}")
    for row_id in range(expected):
        execute(conn, create_query, {"id": row_id, "name": f"{name_prefix}_{row_id}"})


def allocate_session(
    args: argparse.Namespace, state: StressState, stop_event: threading.Event
) -> int | None:
    with state.lock:
        if stop_event.is_set():
            return None
        target = args.ops_per_writer * args.writers if args.ops_per_writer > 0 else None
        if target is not None and state.allocated_sessions >= target:
            return None
        session_id = args.id_base + state.allocated_sessions
        state.allocated_sessions += 1
        return session_id


def run_writer(
    kuzu: Any,
    db: Any,
    args: argparse.Namespace,
    state: StressState,
    stop_event: threading.Event,
    deadline: float,
) -> None:
    conn = kuzu.Connection(db)
    try:
        while not stop_event.is_set() and time.monotonic() < deadline:
            session_id = allocate_session(args, state, stop_event)
            if session_id is None:
                return
            for attempt in range(args.retry_limit + 1):
                with state.lock:
                    state.writer_attempts += 1
                started = time.perf_counter()
                try:
                    write_session(conn, args, session_id)
                    elapsed_ms = (time.perf_counter() - started) * 1000.0
                    with state.lock:
                        state.committed_sessions += 1
                        state.write_latencies_ms.append(elapsed_ms)
                    break
                except Exception as exc:
                    try:
                        execute(conn, "ROLLBACK;")
                    except Exception:
                        pass
                    if attempt >= args.retry_limit:
                        with state.lock:
                            state.errors.append(f"writer failed for session {session_id}: {exc}")
                        stop_event.set()
                        return
                    with state.lock:
                        state.writer_retries += 1
                    time.sleep((args.retry_backoff_ms / 1000.0) * (attempt + 1))
    finally:
        conn.close()


def write_session(conn: Any, args: argparse.Namespace, session_id: int) -> None:
    agent_id = session_id % args.agents
    session_ordinal = session_id - args.id_base
    transient_message_id = -(session_id * 100 + 99)
    execute(conn, "BEGIN TRANSACTION;")
    execute(
        conn,
        "CREATE (:memory_session {id: $id, agentID: $agent_id, startedAt: $started_at});",
        {"id": session_id, "agent_id": agent_id, "started_at": session_id},
    )
    execute(
        conn,
        "MATCH (s:memory_session) WHERE s.id = $session_id SET s.startedAt = $started_at;",
        {"session_id": session_id, "started_at": session_id + 1},
    )
    execute(
        conn,
        "MATCH (a:agent), (s:memory_session) WHERE a.id = $agent_id AND s.id = $session_id "
        "CREATE (a)-[:agent_has_session]->(s);",
        {"agent_id": agent_id, "session_id": session_id},
    )
    for message_idx in range(args.messages_per_session):
        message_id = session_id * 100 + message_idx
        entity_id = (agent_id * 17 + session_ordinal * 7 + message_idx) % args.entities
        execute(
            conn,
            "CREATE (:message {id: $id, sessionID: $session_id, role: 'assistant', content: $content});",
            {
                "id": message_id,
                "session_id": session_id,
                "content": f"agent{agent_id}_session{session_id}_message{message_idx}",
            },
        )
        execute(
            conn,
            "CREATE (:fact {id: $id, entityID: $entity_id, confidence: 0.9, body: $body});",
            {"id": message_id, "entity_id": entity_id, "body": f"fact_{agent_id}_{session_id}_{message_idx}"},
        )
        execute(
            conn,
            "MATCH (s:memory_session), (m:message) WHERE s.id = $session_id AND m.id = $message_id "
            "CREATE (s)-[:session_has_message]->(m);",
            {"session_id": session_id, "message_id": message_id},
        )
        execute(
            conn,
            "MATCH (m:message), (e:entity) WHERE m.id = $message_id AND e.id = $entity_id "
            "CREATE (m)-[:message_mentions_entity]->(e);",
            {"message_id": message_id, "entity_id": entity_id},
        )
        execute(
            conn,
            "MATCH (f:fact), (m:message) WHERE f.id = $message_id AND m.id = $message_id "
            "CREATE (f)-[:fact_supported_by_message]->(m);",
            {"message_id": message_id},
        )
    execute(
        conn,
        "CREATE (:message {id: $id, sessionID: $session_id, role: 'discarded', content: 'discarded'});",
        {"id": transient_message_id, "session_id": session_id},
    )
    execute(
        conn,
        "CREATE (:fact {id: $id, entityID: 0, confidence: 0.1, body: 'discarded'});",
        {"id": transient_message_id},
    )
    execute(
        conn,
        "MATCH (s:memory_session), (m:message) WHERE s.id = $session_id AND m.id = $message_id "
        "CREATE (s)-[:session_has_message]->(m);",
        {"session_id": session_id, "message_id": transient_message_id},
    )
    execute(
        conn,
        "MATCH (m:message), (e:entity) WHERE m.id = $message_id AND e.id = 0 "
        "CREATE (m)-[:message_mentions_entity]->(e);",
        {"message_id": transient_message_id},
    )
    execute(
        conn,
        "MATCH (f:fact), (m:message) WHERE f.id = $message_id AND m.id = $message_id "
        "CREATE (f)-[:fact_supported_by_message]->(m);",
        {"message_id": transient_message_id},
    )
    execute(
        conn,
        "MATCH (s:memory_session)-[r:session_has_message]->(m:message) "
        "WHERE s.id = $session_id AND m.id = $message_id DELETE r;",
        {"session_id": session_id, "message_id": transient_message_id},
    )
    execute(
        conn,
        "MATCH (m:message)-[r:message_mentions_entity]->(:entity) "
        "WHERE m.id = $message_id DELETE r;",
        {"message_id": transient_message_id},
    )
    execute(
        conn,
        "MATCH (f:fact)-[r:fact_supported_by_message]->(:message) "
        "WHERE f.id = $message_id DELETE r;",
        {"message_id": transient_message_id},
    )
    execute(conn, "MATCH (m:message) WHERE m.id = $message_id DELETE m;", {"message_id": transient_message_id})
    execute(conn, "MATCH (f:fact) WHERE f.id = $message_id DELETE f;", {"message_id": transient_message_id})
    execute(conn, "COMMIT;")


def run_reader(
    kuzu: Any,
    db: Any,
    state: StressState,
    stop_event: threading.Event,
    deadline: float,
) -> None:
    conn = kuzu.Connection(db)
    try:
        while not stop_event.is_set() and time.monotonic() < deadline:
            try:
                verify_traversal_invariants(conn)
                with state.lock:
                    state.reader_queries += 1
            except Exception as exc:
                with state.lock:
                    state.invariant_failures += 1
                    state.errors.append(f"reader invariant failed: {exc}")
                stop_event.set()
                return
    finally:
        conn.close()


def run_checkpointer(
    kuzu: Any,
    db: Any,
    args: argparse.Namespace,
    state: StressState,
    stop_event: threading.Event,
    deadline: float,
) -> None:
    if args.manual_checkpoint_interval <= 0:
        return
    conn = kuzu.Connection(db)
    try:
        while not stop_event.wait(args.manual_checkpoint_interval):
            if time.monotonic() >= deadline:
                return
            try:
                execute(conn, "CHECKPOINT;")
                with state.lock:
                    state.checkpoints += 1
            except Exception as exc:
                with state.lock:
                    state.checkpoint_errors += 1
                    state.errors.append(f"manual checkpoint failed: {exc}")
                stop_event.set()
                return
    finally:
        conn.close()


def verify_traversal_invariants(conn: Any) -> None:
    counts = maybe_row(
        conn,
        "OPTIONAL MATCH (s:memory_session) "
        "WITH COUNT(s) AS sessions "
        "OPTIONAL MATCH (m:message) "
        "WITH sessions, COUNT(m) AS messages "
        "OPTIONAL MATCH (f:fact) "
        "WITH sessions, messages, COUNT(f) AS facts "
        "OPTIONAL MATCH (a:agent)-[:agent_has_session]->(s2:memory_session) "
        "WITH sessions, messages, facts, COUNT(DISTINCT s2.id) AS agentSessions "
        "OPTIONAL MATCH (s3:memory_session)-[:session_has_message]->(m2:message) "
        "WITH sessions, messages, facts, agentSessions, COUNT(DISTINCT m2.id) AS sessionMessages "
        "OPTIONAL MATCH (a2:agent)-[:agent_has_session]->(s4:memory_session)-[:session_has_message]->(m3:message) "
        "RETURN sessions, messages, facts, agentSessions, sessionMessages, COUNT(DISTINCT m3.id);",
    )
    if counts is None:
        raise RuntimeError("traversal query returned no rows")
    sessions, messages, facts, agent_sessions, session_messages, agent_messages = counts
    if sessions != agent_sessions:
        raise RuntimeError(f"agent->session traversal count {agent_sessions} != node count {sessions}")
    if messages != session_messages:
        raise RuntimeError(f"session->message traversal count {session_messages} != node count {messages}")
    if messages != agent_messages:
        raise RuntimeError(f"agent->session->message traversal count {agent_messages} != node count {messages}")
    if messages != facts:
        raise RuntimeError(f"message count {messages} != fact count {facts}")
    endpoint_checks = [
        (
            "MATCH (a:agent)-[:agent_has_session]->(s:memory_session) "
            "WHERE s.agentID <> a.id RETURN COUNT(s);",
            "agent/session endpoint mismatch",
        ),
        (
            "MATCH (s:memory_session)-[:session_has_message]->(m:message) "
            "WHERE m.sessionID <> s.id RETURN COUNT(m);",
            "session/message endpoint mismatch",
        ),
        (
            "MATCH (m:message)-[:message_mentions_entity]->(e:entity), (f:fact) "
            "WHERE f.id = m.id AND f.entityID <> e.id RETURN COUNT(m);",
            "message/entity endpoint mismatch",
        ),
        (
            "MATCH (f:fact)-[:fact_supported_by_message]->(m:message) "
            "WHERE f.id <> m.id RETURN COUNT(f);",
            "fact/message endpoint mismatch",
        ),
    ]
    for query, label in endpoint_checks:
        mismatches = scalar(conn, query)
        if mismatches != 0:
            raise RuntimeError(f"{label}: {mismatches}")


def verify_exact_counts(conn: Any, committed_sessions: int, messages_per_session: int) -> None:
    expected_messages = committed_sessions * messages_per_session
    checks = [
        ("MATCH (s:memory_session) RETURN COUNT(s);", committed_sessions, "sessions"),
        ("MATCH (m:message) RETURN COUNT(m);", expected_messages, "messages"),
        ("MATCH (f:fact) RETURN COUNT(f);", expected_messages, "facts"),
        (
            "MATCH (:agent)-[r:agent_has_session]->(:memory_session) RETURN COUNT(r);",
            committed_sessions,
            "agent_has_session rels",
        ),
        (
            "MATCH (:memory_session)-[r:session_has_message]->(:message) RETURN COUNT(r);",
            expected_messages,
            "session_has_message rels",
        ),
        (
            "MATCH (:message)-[r:message_mentions_entity]->(:entity) RETURN COUNT(r);",
            expected_messages,
            "message_mentions_entity rels",
        ),
        (
            "MATCH (:fact)-[r:fact_supported_by_message]->(:message) RETURN COUNT(r);",
            expected_messages,
            "fact_supported_by_message rels",
        ),
    ]
    for query, expected, label in checks:
        actual = scalar(conn, query)
        if actual != expected:
            raise RuntimeError(f"{label}: expected {expected}, got {actual}")
    verify_traversal_invariants(conn)


def run_epoch(
    kuzu: Any,
    db: Any,
    args: argparse.Namespace,
    state: StressState,
    deadline: float,
) -> None:
    stop_event = threading.Event()
    workers = [
        threading.Thread(target=run_writer, args=(kuzu, db, args, state, stop_event, deadline))
        for _ in range(args.writers)
    ]
    workers.extend(
        threading.Thread(target=run_reader, args=(kuzu, db, state, stop_event, deadline))
        for _ in range(args.readers)
    )
    workers.append(
        threading.Thread(target=run_checkpointer, args=(kuzu, db, args, state, stop_event, deadline))
    )
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join()


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, math.ceil((pct / 100.0) * len(ordered)) - 1))
    return ordered[index]


def build_report(args: argparse.Namespace, state: StressState, elapsed_seconds: float) -> dict[str, Any]:
    with state.lock:
        latencies = list(state.write_latencies_ms)
        committed_sessions = state.committed_sessions
        return {
            "db_path": str(args.db_path),
            "elapsed_seconds": round(elapsed_seconds, 3),
            "committed_sessions": committed_sessions,
            "committed_messages": committed_sessions * args.messages_per_session,
            "writer_attempts": state.writer_attempts,
            "writer_retries": state.writer_retries,
            "reader_queries": state.reader_queries,
            "manual_checkpoints": state.checkpoints,
            "checkpoint_errors": state.checkpoint_errors,
            "invariant_failures": state.invariant_failures,
            "throughput_sessions_per_second": committed_sessions / elapsed_seconds
            if elapsed_seconds > 0
            else 0.0,
            "write_latency_ms_p50": round(percentile(latencies, 50), 3),
            "write_latency_ms_p95": round(percentile(latencies, 95), 3),
            "write_latency_ms_p99": round(percentile(latencies, 99), 3),
            "errors": list(state.errors),
        }


def print_report(report: dict[str, Any], as_json: bool) -> None:
    if as_json:
        print(json.dumps(report, indent=2, sort_keys=True))
        return
    for key, value in report.items():
        if key == "errors":
            continue
        print(f"{key}: {value}")
    if report["errors"]:
        print("errors:")
        for error in report["errors"]:
            print(f"  - {error}")


def main() -> int:
    args = parse_args()
    temp_dir: Path | None = None
    if args.db_path is None:
        temp_dir = Path(tempfile.mkdtemp(prefix="kuzu-agent-memory-stress-"))
        args.db_path = temp_dir / "db"
    if args.id_base == 0:
        args.id_base = time.time_ns() // 1000
    kuzu = import_kuzu()
    state = StressState()
    started = time.monotonic()
    deadline = started + args.duration_seconds
    db = setup_database(kuzu, args)
    try:
        while time.monotonic() < deadline:
            epoch_deadline = deadline
            if args.reopen_verify_interval > 0:
                epoch_deadline = min(epoch_deadline, time.monotonic() + args.reopen_verify_interval)
            run_epoch(kuzu, db, args, state, epoch_deadline)
            conn = kuzu.Connection(db)
            try:
                verify_exact_counts(conn, state.committed_sessions, args.messages_per_session)
            finally:
                conn.close()
            if args.reopen_verify_interval <= 0 or time.monotonic() >= deadline:
                break
            db = reopen_and_verify(kuzu, db, args, state)
            db = checkpoint_and_verify(kuzu, db, args, state)
            if state.errors:
                break
        db = reopen_and_verify(kuzu, db, args, state)
        db = checkpoint_and_verify(kuzu, db, args, state)
    finally:
        db.close()
        if temp_dir is not None and not args.keep_db:
            shutil.rmtree(temp_dir, ignore_errors=True)
    elapsed = time.monotonic() - started
    report = build_report(args, state, elapsed)
    print_report(report, args.json)
    return 1 if report["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
