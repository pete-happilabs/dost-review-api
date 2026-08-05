"""Onboard pool lifecycle: startup degradation, read-only enforcement, timeout.

These cover the parts of profile validation that live outside gate_and_store().
"""
import asyncpg
import pytest

from app import main as main_mod
from app.database import get_onboard_pool

UNREACHABLE_DSN = "postgresql://nobody:nobody@127.0.0.1:1/does_not_exist"


@pytest.mark.asyncio
async def test_startup_survives_unreachable_onboard_db(monkeypatch):
    """An unreachable Onboard DB must degrade to disabled validation, not stop the
    service booting. Profile validation fails open at query time by design, so a
    hard boot dependency on the same DB would take the whole API down — including
    /health, /ready and reputation reads — over an optional read-only lookup.

    Only the onboard branch of lifespan is exercised; the primary-pool setup is
    stubbed so this test cannot disturb the session-scoped `pool` fixture.
    """

    async def _noop(*args, **kwargs):
        return None

    monkeypatch.setattr(main_mod, "create_pool", _noop)
    monkeypatch.setattr(main_mod, "run_migrations", _noop)
    monkeypatch.setattr(main_mod, "sweep_stale_batches", _noop)
    monkeypatch.setattr(main_mod, "close_pool", _noop)
    monkeypatch.setattr(main_mod, "start_scheduler", lambda: None)
    monkeypatch.setattr(main_mod, "stop_scheduler", lambda: None)
    monkeypatch.setattr(main_mod.settings, "onboard_database_url", UNREACHABLE_DSN)

    async with main_mod.lifespan(main_mod.app):
        # Booted. Validation is off rather than the process being dead.
        assert get_onboard_pool() is None


@pytest.mark.asyncio
async def test_onboard_pool_rejects_writes(onboard):
    """The 'read-only pool' design decision has to be enforced by the connection,
    not by convention: this pool points at another service's production database.
    """
    with pytest.raises(asyncpg.exceptions.ReadOnlySQLTransactionError):
        await onboard.execute("CREATE TABLE onboard_write_probe (x int)")


@pytest.mark.asyncio
async def test_validation_fails_open_when_lookup_times_out(
    monkeypatch, pool, onboard, seed_profile
):
    """A wedged (rather than erroring) Onboard DB must not hang review submission.
    The bound has to cover the wait for a free connection too — Pool.fetch's own
    timeout does not, and with max_size=3 that queue is the likelier place to stall.
    """
    from app.gate import _validate_profiles
    from uuid import uuid4

    target, rater = uuid4(), uuid4()
    await seed_profile(target, status="RETIRED")  # would otherwise be TARGET_INACTIVE
    await seed_profile(rater)

    monkeypatch.setattr(main_mod.settings, "onboard_query_timeout", 0)
    assert await _validate_profiles(target, rater) is None
