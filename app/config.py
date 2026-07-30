from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql://dost:dost@localhost:5433/dost_reviews"
    anthropic_api_key: str = ""
    batch_cadence_cron: str = "0 6 * * *"
    max_reviews_per_batch: int = 10_000
    rate_limit_per_rater_per_target: int = 5
    engine_path: str = ""

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
