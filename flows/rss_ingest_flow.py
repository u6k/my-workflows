from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from prefect import flow, get_run_logger, task
from prefect.blocks.core import Block
from prefect.blocks.system import Secret


REQUIRED_PREFECT_BLOCK_KEYS = (
    "s3_credentials_block",
    "ollama_base_url_block",
    "ollama_model_block",
)


def _load_yaml_config(config_path: str) -> dict[str, Any]:
    path = Path(config_path)
    if not path.exists():
        raise ValueError(f"config file is not found: {config_path}")

    with path.open("r", encoding="utf-8") as fp:
        config = yaml.safe_load(fp)

    if not isinstance(config, dict):
        raise ValueError("config must be a YAML object")

    return config


def _validate_config(config: dict[str, Any]) -> None:
    rss_urls = config.get("rss_urls")
    if not isinstance(rss_urls, list) or len(rss_urls) == 0:
        raise ValueError("config.rss_urls must be a non-empty list")

    invalid_urls = [url for url in rss_urls if not isinstance(url, str) or not url.startswith(("http://", "https://"))]
    if invalid_urls:
        raise ValueError("all config.rss_urls values must start with http:// or https://")

    retry = config.get("retry")
    if not isinstance(retry, dict):
        raise ValueError("config.retry must be an object")

    max_retries = retry.get("max_retries")
    if not isinstance(max_retries, int):
        raise ValueError("config.retry.max_retries must be an integer")

    if "initial_delay_sec" in retry and not isinstance(retry["initial_delay_sec"], int):
        raise ValueError("config.retry.initial_delay_sec must be an integer")

    if "backoff_multiplier" in retry and not isinstance(retry["backoff_multiplier"], (int, float)):
        raise ValueError("config.retry.backoff_multiplier must be a number")

    storage = config.get("storage")
    if not isinstance(storage, dict):
        raise ValueError("config.storage must be an object")

    s3_prefix = storage.get("s3_prefix")
    if not isinstance(s3_prefix, str) or not s3_prefix:
        raise ValueError("config.storage.s3_prefix must be a non-empty string")

    prefect_blocks = config.get("prefect_blocks")
    if not isinstance(prefect_blocks, dict):
        raise ValueError("config.prefect_blocks must be an object")

    missing_block_keys = [key for key in REQUIRED_PREFECT_BLOCK_KEYS if key not in prefect_blocks]
    if missing_block_keys:
        raise ValueError(f"config.prefect_blocks is missing required keys: {', '.join(missing_block_keys)}")

    invalid_block_names = [
        key
        for key in REQUIRED_PREFECT_BLOCK_KEYS
        if not isinstance(prefect_blocks.get(key), str) or not prefect_blocks[key]
    ]
    if invalid_block_names:
        raise ValueError(f"config.prefect_blocks values must be non-empty strings: {', '.join(invalid_block_names)}")


@task(name="load_config_task")
def load_config_task(config_path: str = "config.yaml") -> dict[str, Any]:
    config = _load_yaml_config(config_path)
    _validate_config(config)
    return config


@task(name="validate_prerequisites_task")
def validate_prerequisites_task(config: dict[str, Any]) -> None:
    logger = get_run_logger()

    rss_urls = config["rss_urls"]
    unique_count = len(set(rss_urls))
    logger.info("rss_urls validation passed: total=%d unique=%d", len(rss_urls), unique_count)

    block_config = config["prefect_blocks"]

    s3_block_name = block_config["s3_credentials_block"]
    Block.load(s3_block_name)
    logger.info("Prefect block loaded: key=s3_credentials_block name=%s", s3_block_name)

    ollama_base_url_block = block_config["ollama_base_url_block"]
    Secret.load(ollama_base_url_block).get()
    logger.info("Prefect secret loaded: key=ollama_base_url_block name=%s", ollama_base_url_block)

    ollama_model_block = block_config["ollama_model_block"]
    Secret.load(ollama_model_block).get()
    logger.info("Prefect secret loaded: key=ollama_model_block name=%s", ollama_model_block)


@flow(name="rss_ingest_flow")
def rss_ingest_flow(config_path: str = "config.yaml") -> None:
    config = load_config_task(config_path)
    validate_prerequisites_task(config)


if __name__ == "__main__":
    rss_ingest_flow()
