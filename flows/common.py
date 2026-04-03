from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml
from botocore.config import Config as BotocoreConfig
from prefect_aws.credentials import AwsCredentials


def load_yaml_config(config_path: str) -> dict[str, Any]:
    path = Path(config_path)
    if not path.exists():
        raise ValueError(f"config file is not found: {config_path}")

    with path.open("r", encoding="utf-8") as fp:
        config = yaml.safe_load(fp)

    if not isinstance(config, dict):
        raise ValueError("config must be a YAML object")

    return config


def normalize_botocore_config(raw_config: Any) -> BotocoreConfig | None:
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


def get_aws_client_parameters(aws_credentials: AwsCredentials) -> dict[str, Any]:
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
            if key == "endpoint_url" and not isinstance(value, str):
                continue
            if key == "config" and not isinstance(value, (str, dict, BotocoreConfig)):
                continue
            extracted[key] = value
    return extracted


def create_s3_client(aws_credentials: AwsCredentials) -> Any:
    client_kwargs: dict[str, Any] = {}
    client_parameters = get_aws_client_parameters(aws_credentials)

    endpoint_url = client_parameters.get("endpoint_url")
    if isinstance(endpoint_url, str) and endpoint_url:
        client_kwargs["endpoint_url"] = endpoint_url

    config = normalize_botocore_config(client_parameters.get("config"))
    if config is not None:
        client_kwargs["config"] = config

    return aws_credentials.get_boto3_session().client("s3", **client_kwargs)
