from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql://dost:dost@localhost:5433/dost_reviews"
    anthropic_api_key: str = ""
    batch_cadence_cron: str = "0 6 * * *"
    max_reviews_per_batch: int = 10_000
    rate_limit_per_rater_per_target: int = 5
    engine_path: str = ""
    # C1: Guard access key for API authentication
    guard_access_key: str = ""
    # Onboard DB for profile validation (read-only). Empty = skip validation.
    onboard_database_url: str = ""
    # Hard bound on a single Onboard lookup. Onboard is an optional dependency on
    # the request path: without this, a wedged (as opposed to erroring) Onboard DB
    # hangs review submission forever — the fail-open handler only catches raises.
    onboard_query_timeout: float = 2.0
    # H3: Max retries before dead-lettering a review
    max_review_retries: int = 3
    # H3: RUNNING batches older than this are considered dead (process crashed)
    # and get swept — their PROCESSING reviews are requeued as GATED.
    stale_batch_after_minutes: int = 60
    # M8: Batch scheduler timezone
    batch_timezone: str = "UTC"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
