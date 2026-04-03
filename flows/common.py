from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

import yaml
from botocore.config import Config as BotocoreConfig
from prefect.blocks.system import Secret
from prefect_aws.credentials import AwsCredentials

REQUIRED_OLLAMA_CONNECTION_KEYS = (
    "base_url",
    "model",
)


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


def validate_ollama_connection(ollama_connection: Any) -> dict[str, str]:
    if not isinstance(ollama_connection, dict):
        raise ValueError("Ollama Secret block value must be a dict")

    missing_ollama_keys = [
        key for key in REQUIRED_OLLAMA_CONNECTION_KEYS if not isinstance(ollama_connection.get(key), str) or not ollama_connection[key]
    ]
    if missing_ollama_keys:
        raise ValueError(f"Ollama Secret JSON is missing required keys: {', '.join(missing_ollama_keys)}")

    return {
        "base_url": ollama_connection["base_url"],
        "model": ollama_connection["model"],
    }


def load_ollama_connection_secret(secret_block_name: str, logger: logging.Logger | None = None) -> dict[str, str]:
    ollama_secret_value = Secret.load(secret_block_name).get()
    if logger is not None:
        logger.info("Prefect secret loaded: key=ollama_connection_secret_block name=%s", secret_block_name)
    return validate_ollama_connection(ollama_secret_value)


def invoke_ollama_generate(
    ollama_connection: dict[str, str],
    prompt: str,
    timeout_sec: int = 120,
    response_format: str | None = None,
) -> str:
    payload: dict[str, Any] = {
        "model": ollama_connection["model"],
        "prompt": prompt,
        "stream": False,
    }
    if response_format is not None:
        payload["format"] = response_format

    request = Request(
        f"{ollama_connection['base_url'].rstrip('/')}/api/generate",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=timeout_sec) as response:
        body = response.read().decode("utf-8")

    response_json = json.loads(body)
    return str(response_json.get("response", "")).strip()
