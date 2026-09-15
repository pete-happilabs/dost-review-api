"""Persistence for the risk service. RiskStore is the seam tests substitute."""
from __future__ import annotations

import contextlib
import json
from collections.abc import AsyncIterator
from contextvars import ContextVar
from typing import Any

from app.database import get_pool  # the review API's asyncpg pool accessor

# The connection whose transaction the current task is inside, if any. Every store call on
# this task routes through it, so the reads, the engine's fold and the writes of one ingest
# share one transaction (and one advisory lock). Each asyncio task sees its own value.
_conn: ContextVar[Any] = ContextVar("risk_store_conn", default=None)

# hashtext folds the key to int4; a collision between two keys only serializes two ingests
# that did not need it, never lets two that did overlap. The lock releases with the commit.
_LOCK = "SELECT pg_advisory_xact_lock(hashtext($1))"


def _jsonb(value: Any) -> Any:
    """asyncpg hands JSONB back as text here (no codec on the pool) — decode it."""
    return json.loads(value) if isinstance(value, str) else value


class RiskStore:
    """Interface; PostgresRiskStore below is the real one, tests substitute a fake."""

    @contextlib.asynccontextmanager
    async def transaction(self, lock_key: str) -> AsyncIterator[None]:
        """Run the block atomically, serialized with every other block on the same key.

        The base version is a no-op so fakes inherit the interface; the Postgres one takes
        an advisory lock inside a real transaction.
        """
        yield

    async def get_conversation_state(self, session_id: str) -> dict | None: ...
    async def put_conversation_state(self, session_id: str, state: dict) -> None: ...
    async def get_profile(self, profile_id: str) -> dict | None: ...
    async def put_profile(self, profile_id: str, **row: Any) -> None: ...
    async def insert_signal_event(self, record: dict) -> bool: ...
    async def insert_outcome(self, **row: Any) -> None: ...
    async def enqueue_review(self, **row: Any) -> None: ...
    async def participants_of(self, session_id: str, fallback: set[str]) -> set[str]: ...


class PostgresRiskStore(RiskStore):
    @staticmethod
    def _db() -> Any:
        """The bound transaction connection when inside `transaction`, else the pool."""
        conn = _conn.get()
        return get_pool() if conn is None else conn

    @contextlib.asynccontextmanager
    async def transaction(self, lock_key: str) -> AsyncIterator[None]:
        conn = _conn.get()
        if conn is not None:
            # Nested (the profile lock inside a session's transaction): stay on the same
            # connection and transaction, just take the further lock. Re-locking a key this
            # transaction already holds always succeeds, so a fold can never self-deadlock.
            await conn.execute(_LOCK, lock_key)
            yield
            return
        async with get_pool().acquire() as conn, conn.transaction():
            await conn.execute(_LOCK, lock_key)
            token = _conn.set(conn)
            try:
                yield
            finally:
                _conn.reset(token)

    async def get_conversation_state(self, session_id):
        row = await self._db().fetchrow(
            "SELECT state FROM conversation_risk WHERE session_id=$1", session_id)
        return _jsonb(row["state"]) if row else None

    async def put_conversation_state(self, session_id, state):
        await self._db().execute(
            "INSERT INTO conversation_risk (session_id, state) VALUES ($1, $2::jsonb) "
            "ON CONFLICT (session_id) DO UPDATE SET state=EXCLUDED.state, updated_at=now()",
            session_id, json.dumps(state))

    async def get_profile(self, profile_id):
        row = await self._db().fetchrow(
            "SELECT account_id, state, risk_score, risk_tier, repeated_across "
            "FROM profile_risk WHERE profile_id=$1", profile_id)
        if not row:
            return None
        out = dict(row)
        out["state"] = _jsonb(out["state"])
        out["repeated_across"] = _jsonb(out["repeated_across"])
        return out

    async def put_profile(self, profile_id, **row):
        await self._db().execute(
            "INSERT INTO profile_risk (profile_id, account_id, state, risk_score, risk_tier, "
            "repeated_across) VALUES ($1,$2,$3::jsonb,$4,$5,$6::jsonb) "
            "ON CONFLICT (profile_id) DO UPDATE SET "
            "account_id=COALESCE(EXCLUDED.account_id, profile_risk.account_id), "
            "state=EXCLUDED.state, risk_score=EXCLUDED.risk_score, risk_tier=EXCLUDED.risk_tier, "
            "repeated_across=EXCLUDED.repeated_across, updated_at=now()",
            profile_id, row.get("account_id"), json.dumps(row["state"]),
            row["risk_score"], row["risk_tier"], json.dumps(row.get("repeated_across", [])))

    async def insert_signal_event(self, record):
        res = await self._db().execute(
            "INSERT INTO signal_event (session_id, event_id, sender_profile_id, "
            "receiver_profile_id, model_version, verdict, record) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb) ON CONFLICT (session_id, event_id) DO NOTHING",
            record["sessionId"], record["eventId"], record["srcProfileId"],
            record.get("dstProfileId"), record["modelVersion"], record["verdict"],
            json.dumps(record))
        return res.endswith(" 1")   # "INSERT 0 1" inserted; "INSERT 0 0" duplicate

    async def insert_outcome(self, **row):
        await self._db().execute(
            "INSERT INTO outcome_event (profile_id, reporter_id, session_id, tag, evidence) "
            "VALUES ($1,$2,$3,$4,$5::jsonb)",
            row["profile_id"], row.get("reporter_id"), row.get("session_id"), row["tag"],
            json.dumps(row.get("evidence") or {}))

    async def enqueue_review(self, **row):
        await self._db().execute(
            "INSERT INTO review_queue (profile_id, session_id, reason, risk_score, evidence) "
            "VALUES ($1,$2,$3,$4,$5::jsonb)",
            row["profile_id"], row.get("session_id"), row["reason"], row["risk_score"],
            json.dumps(row.get("evidence") or {}))

    async def participants_of(self, session_id, fallback):
        # dost-talk owns the participant list; until an internal read exists, the pair on
        # the record (sender, receiver) is the best available truth.
        return fallback
