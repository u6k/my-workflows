from __future__ import annotations

import logging
from pathlib import Path
from typing import Any
from urllib.request import urlopen
import xml.etree.ElementTree as ET

import yaml
from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret
from prefect.exceptions import MissingContextError
from prefect_aws.credentials import AwsCredentials


REQUIRED_PREFECT_BLOCK_KEYS = (
    "aws_credentials_block",
    "ollama_connection_secret_block",
)
REQUIRED_OLLAMA_CONNECTION_KEYS = (
    "base_url",
    "model",
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






def _get_task_logger() -> logging.Logger:
    try:
        return get_run_logger()
    except MissingContextError:
        return logging.getLogger(__name__)

def _local_name(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[1]
    return tag


def _extract_links_from_feed_xml(feed_xml: bytes) -> list[str]:
    try:
        root = ET.fromstring(feed_xml)
    except ET.ParseError as exc:
        raise ValueError("failed to parse RSS/Atom XML") from exc

    links: list[str] = []

    for element in root.iter():
        name = _local_name(element.tag)

        if name == "item":
            for child in element:
                if _local_name(child.tag) == "link" and child.text:
                    links.append(child.text.strip())
        elif name == "entry":
            for child in element:
                if _local_name(child.tag) != "link":
                    continue

                href = child.attrib.get("href")
                rel = child.attrib.get("rel", "alternate")
                if href and rel in ("alternate", ""):
                    links.append(href.strip())

    unique_links: list[str] = []
    seen: set[str] = set()
    for link in links:
        if not link or not link.startswith(("http://", "https://")):
            continue
        if link in seen:
            continue
        seen.add(link)
        unique_links.append(link)

    return unique_links


@task(name="fetch_feed_task")
def fetch_feed_task(feed_url: str) -> list[str]:
    logger = _get_task_logger()

    with urlopen(feed_url, timeout=30) as response:
        feed_xml = response.read()

    links = _extract_links_from_feed_xml(feed_xml)
    logger.debug("extracted links: feed_url=%s links=%s", feed_url, links)
    if len(links) == 0:
        raise ValueError(f"feed has no entries: {feed_url}")

    return links

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

    aws_credentials_block = block_config["aws_credentials_block"]
    AwsCredentials.load(aws_credentials_block)
    logger.info("Prefect block loaded: key=aws_credentials_block name=%s", aws_credentials_block)

    ollama_secret_block = block_config["ollama_connection_secret_block"]
    ollama_secret_value = Secret.load(ollama_secret_block).get()
    logger.info("Prefect secret loaded: key=ollama_connection_secret_block name=%s", ollama_secret_block)

    if not isinstance(ollama_secret_value, dict):
        raise ValueError("Ollama Secret block value must be a dict")

    ollama_connection = ollama_secret_value

    missing_ollama_keys = [
        key for key in REQUIRED_OLLAMA_CONNECTION_KEYS if not isinstance(ollama_connection.get(key), str) or not ollama_connection[key]
    ]
    if missing_ollama_keys:
        raise ValueError(f"Ollama Secret JSON is missing required keys: {', '.join(missing_ollama_keys)}")


@flow(name="rss_ingest_flow")
def rss_ingest_flow(config_path: str = "config.yaml") -> None:
    logger = _get_task_logger()

    config = load_config_task(config_path)
    validate_prerequisites_task(config)

    all_links: list[str] = []
    for feed_url in list(dict.fromkeys(config["rss_urls"])):
        links = fetch_feed_task(feed_url)
        all_links.extend(links)

    for link in all_links:
        logger.debug("all_links record: %s", link)

    logger.info("feed links extracted: total=%d", len(all_links))


if __name__ == "__main__":
    rss_ingest_flow()
