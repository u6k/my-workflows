from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml
from botocore.config import Config as BotocoreConfig
from prefect import flow, get_run_logger, task
from prefect.exceptions import MissingContextError
from prefect_aws.credentials import AwsCredentials


def _get_task_logger() -> logging.Logger:
    try:
        return get_run_logger()
    except MissingContextError:
        return logging.getLogger(__name__)


def _load_yaml_config(config_path: str) -> dict[str, Any]:
    path = Path(config_path)
    if not path.exists():
        raise ValueError(f"config file is not found: {config_path}")

    with path.open("r", encoding="utf-8") as fp:
        config = yaml.safe_load(fp)

    if not isinstance(config, dict):
        raise ValueError("config must be a YAML object")

    return config


def _validate_daily_digest_config(config: dict[str, Any]) -> None:
    storage = config.get("storage")
    if not isinstance(storage, dict):
        raise ValueError("config.storage must be an object")

    s3_bucket = storage.get("s3_bucket")
    if not isinstance(s3_bucket, str) or not s3_bucket:
        raise ValueError("config.storage.s3_bucket must be a non-empty string")

    s3_prefix = storage.get("s3_prefix")
    if not isinstance(s3_prefix, str):
        raise ValueError("config.storage.s3_prefix must be a string")

    prefect_blocks = config.get("prefect_blocks")
    if not isinstance(prefect_blocks, dict):
        raise ValueError("config.prefect_blocks must be an object")

    aws_credentials_block = prefect_blocks.get("aws_credentials_block")
    if not isinstance(aws_credentials_block, str) or not aws_credentials_block:
        raise ValueError("config.prefect_blocks.aws_credentials_block must be a non-empty string")


def _parse_target_date(target_date: str) -> date:
    try:
        return datetime.strptime(target_date, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError("target_date must be in YYYY-MM-DD format") from exc


def _normalize_botocore_config(raw_config: Any) -> BotocoreConfig | None:
    if raw_config is None:
        return None

    if isinstance(raw_config, BotocoreConfig):
        return raw_config

    normalized_config = raw_config
    if isinstance(raw_config, str):
        normalized_config = raw_config.strip()
        if normalized_config == "":
            return None
        try:
            normalized_config = json.loads(normalized_config)
        except json.JSONDecodeError as exc:
            raise ValueError("aws_client_parameters.config must be valid JSON when provided as a string") from exc

    if not isinstance(normalized_config, dict):
        raise ValueError("aws_client_parameters.config must be a dict, JSON string, or botocore Config")

    return BotocoreConfig(**normalized_config)


def _get_aws_client_parameters(aws_credentials: AwsCredentials) -> dict[str, Any]:
    aws_client_parameters = getattr(aws_credentials, "aws_client_parameters", None)
    if aws_client_parameters is None:
        return {}

    if hasattr(aws_client_parameters, "get_params_override"):
        params_override = aws_client_parameters.get_params_override()
        if isinstance(params_override, dict):
            return params_override

    if isinstance(aws_client_parameters, dict):
        return aws_client_parameters

    extracted: dict[str, Any] = {}
    for key in ("endpoint_url", "config"):
        value = getattr(aws_client_parameters, key, None)
        if value is not None:
            extracted[key] = value
    return extracted


def _create_s3_client(aws_credentials: AwsCredentials) -> Any:
    client_kwargs: dict[str, Any] = {}
    client_parameters = _get_aws_client_parameters(aws_credentials)

    endpoint_url = client_parameters.get("endpoint_url")
    if isinstance(endpoint_url, str) and endpoint_url:
        client_kwargs["endpoint_url"] = endpoint_url

    config = _normalize_botocore_config(client_parameters.get("config"))
    if config is not None:
        client_kwargs["config"] = config

    return aws_credentials.get_boto3_session().client("s3", **client_kwargs)


@task(name="load_daily_digest_config_task")
def load_daily_digest_config_task(config_path: str = "config.yaml") -> dict[str, Any]:
    config = _load_yaml_config(config_path)
    _validate_daily_digest_config(config)
    return config


@task(name="fetch_daily_articles_from_s3_task")
def fetch_daily_articles_from_s3_task(
    target_date: str,
    storage: dict[str, Any],
    aws_credentials_block_name: str,
) -> list[dict[str, Any]]:
    logger = _get_task_logger()
    target = _parse_target_date(target_date)
    day_start = datetime.combine(target, datetime.min.time(), tzinfo=timezone.utc)
    day_end = day_start + timedelta(days=1)

    aws_credentials = AwsCredentials.load(aws_credentials_block_name)
    s3_client = _create_s3_client(aws_credentials)

    matched_articles: list[dict[str, Any]] = []
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=storage["s3_bucket"], Prefix=storage["s3_prefix"].strip("/"))

    for page in pages:
        for obj in page.get("Contents", []):
            last_modified = obj["LastModified"]
            if last_modified.tzinfo is None:
                last_modified = last_modified.replace(tzinfo=timezone.utc)
            else:
                last_modified = last_modified.astimezone(timezone.utc)

            if not (day_start <= last_modified < day_end):
                continue

            object_key = obj["Key"]
            s3_path = f"s3://{storage['s3_bucket']}/{object_key}"
            logger.info("target S3 data fetch path: %s", s3_path)

            response = s3_client.get_object(Bucket=storage["s3_bucket"], Key=object_key)
            body = response["Body"].read().decode("utf-8")
            matched_articles.append(json.loads(body))

    return matched_articles


@flow(name="daily-news-blog-digest-flow")
def daily_news_blog_digest_flow(target_date: str, config_path: str = "config.yaml") -> list[dict[str, Any]]:
    logger = _get_task_logger()
    logger.info("daily-news-blog-digest-flow start: target_date=%s", target_date)

    config = load_daily_digest_config_task(config_path)
    return fetch_daily_articles_from_s3_task(
        target_date=target_date,
        storage=config["storage"],
        aws_credentials_block_name=config["prefect_blocks"]["aws_credentials_block"],
    )


if __name__ == "__main__":
    daily_news_blog_digest_flow(target_date="2026-04-02")
