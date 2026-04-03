from __future__ import annotations

import logging
import hashlib
import json
from html.parser import HTMLParser
from typing import Any
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET

from prefect import flow, get_run_logger, task
from prefect.blocks.system import Secret
from prefect.exceptions import MissingContextError
from prefect_aws.credentials import AwsCredentials
import trafilatura
from flows.common import create_s3_client, load_yaml_config


REQUIRED_PREFECT_BLOCK_KEYS = (
    "aws_credentials_block",
    "ollama_connection_secret_block",
)
REQUIRED_OLLAMA_CONNECTION_KEYS = (
    "base_url",
    "model",
)
BRIEFING_PROMPT_TEMPLATE = """以下のニュース記事から主要なテーマとアイデアを統合した包括的なブリーフィングドキュメントを作成してください。まずは、最も重要なポイントを簡潔にまとめたエグゼクティブサマリーから始めましょう。本文では、情報源に含まれる主要なテーマ、証拠、そして結論を​​詳細かつ徹底的に検証する必要があります。分析は、明瞭性を確保するために、見出しと箇条書きを用いて論理的に構成する必要があります。トーンは客観的かつ鋭いものでなければなりません。

# ニュース記事
```txt
{article_content}
```"""
ONE_SENTENCE_PROMPT_TEMPLATE = """以下のニュース記事を、内容が過不足なく伝わるように「一文」で要約してください。
余計な解説は不要です。

# ニュース記事
```txt
{article_content}
```"""


def _load_yaml_config(config_path: str) -> dict[str, Any]:
    return load_yaml_config(config_path)


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

    s3_bucket = storage.get("s3_bucket")
    if not isinstance(s3_bucket, str) or not s3_bucket:
        raise ValueError("config.storage.s3_bucket must be a non-empty string")

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

    ollama = config.get("ollama")
    if ollama is not None:
        if not isinstance(ollama, dict):
            raise ValueError("config.ollama must be an object")

        request_timeout_sec = ollama.get("request_timeout_sec")
        if request_timeout_sec is not None and (not isinstance(request_timeout_sec, int) or request_timeout_sec <= 0):
            raise ValueError("config.ollama.request_timeout_sec must be a positive integer")


def _get_task_logger() -> logging.Logger:
    try:
        return get_run_logger()
    except MissingContextError:
        return logging.getLogger(__name__)


def _local_name(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[1]
    return tag


def _normalize_extracted_link(link: str) -> str:
    if not link.startswith("https://www.google.com/url"):
        return link

    parsed = urlparse(link)
    query = parse_qs(parsed.query)
    return query.get("url", [link])[0]


def _url_to_md5(url: str) -> str:
    return hashlib.md5(url.encode("utf-8")).hexdigest()


def _build_s3_object_key(s3_prefix: str, article_id: str) -> str:
    normalized_prefix = s3_prefix.strip("/")
    return f"{normalized_prefix}/{article_id[:2]}/{article_id}.json"


def _create_s3_client(aws_credentials: AwsCredentials) -> Any:
    return create_s3_client(aws_credentials)


def _build_briefing_prompt(article_content: str) -> str:
    return BRIEFING_PROMPT_TEMPLATE.format(article_content=article_content)


def _build_one_sentence_prompt(article_content: str) -> str:
    return ONE_SENTENCE_PROMPT_TEMPLATE.format(article_content=article_content)


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
        normalized_link = _normalize_extracted_link(link)
        if not normalized_link or not normalized_link.startswith(("http://", "https://")):
            continue
        if normalized_link in seen:
            continue
        seen.add(normalized_link)
        unique_links.append(normalized_link)

    return unique_links


class _ArticleHTMLParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.in_title = False
        self.skip_depth = 0
        self.title: str | None = None
        self.visible_text_parts: list[str] = []
        self.metadata: dict[str, Any] = {"tags": []}

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_dict = {k.lower(): (v or "") for k, v in attrs}
        lower_tag = tag.lower()

        if lower_tag in {"script", "style", "noscript"}:
            self.skip_depth += 1
            return

        if lower_tag == "title":
            self.in_title = True
            return

        if lower_tag != "meta":
            return

        property_name = attrs_dict.get("property", "").lower()
        name = attrs_dict.get("name", "").lower()
        content = attrs_dict.get("content", "").strip()
        if not content:
            return

        if property_name == "og:title" and not self.title:
            self.title = content
        elif property_name == "og:site_name":
            self.metadata["site_name"] = content
        elif property_name == "og:image":
            self.metadata["image_url"] = content
        elif property_name == "article:published_time":
            self.metadata["published_timestamp"] = content
        elif property_name == "article:modified_time":
            self.metadata["updated_timestamp"] = content
        elif property_name == "article:tag":
            self.metadata["tags"].append(content)

        if name == "author":
            self.metadata["author"] = content
        elif name == "keywords":
            keywords = [tag.strip() for tag in content.split(",") if tag.strip()]
            self.metadata["tags"].extend(keywords)
        elif name in {"lang", "language"}:
            self.metadata["language"] = content

    def handle_endtag(self, tag: str) -> None:
        lower_tag = tag.lower()
        if lower_tag in {"script", "style", "noscript"} and self.skip_depth > 0:
            self.skip_depth -= 1
            return

        if lower_tag == "title":
            self.in_title = False

    def handle_data(self, data: str) -> None:
        if self.skip_depth > 0:
            return

        text = data.strip()
        if not text:
            return

        if self.in_title:
            if self.title:
                self.title = f"{self.title} {text}".strip()
            else:
                self.title = text
            return

        self.visible_text_parts.append(text)


def _extract_article_content_and_metadata(article_html: str) -> dict[str, Any]:
    parser = _ArticleHTMLParser()
    parser.feed(article_html)

    tags = sorted(set(parser.metadata.get("tags", [])))
    metadata = {key: value for key, value in parser.metadata.items() if key != "tags" and value}
    if tags:
        metadata["tags"] = tags

    extracted_content = trafilatura.extract(
        article_html,
        output_format="txt",
        include_comments=False,
        include_tables=False,
        deduplicate=True,
    )
    if isinstance(extracted_content, str):
        content = extracted_content.strip()
    else:
        content = ""

    if not content:
        content = "\n".join(parser.visible_text_parts).strip()

    return {
        "title": parser.title or "",
        "content": content,
        "metadata": metadata,
    }


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


@task(name="fetch_article_task")
def fetch_article_task(article_url: str) -> dict[str, Any]:
    request = Request(
        article_url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; rss-ingest-flow/1.0)",
        },
    )

    with urlopen(request, timeout=30) as response:
        status = getattr(response, "status", 200)
        if status != 200:
            raise ValueError(f"unexpected status code: {status}")

        raw_html = response.read()
        content_type = response.headers.get_content_charset() or "utf-8"

    article_html = raw_html.decode(content_type, errors="replace")
    extracted = _extract_article_content_and_metadata(article_html)
    extracted["url"] = article_url
    extracted["id"] = _url_to_md5(article_url)
    extracted["raw_html"] = article_html
    return extracted


@task(name="store_to_s3_task")
def store_to_s3_task(article: dict[str, Any], storage: dict[str, Any], aws_credentials_block_name: str) -> str:
    article_id = article["id"]
    object_key = _build_s3_object_key(storage["s3_prefix"], article_id)

    aws_credentials = AwsCredentials.load(aws_credentials_block_name)
    s3_client = _create_s3_client(aws_credentials)
    s3_client.put_object(
        Bucket=storage["s3_bucket"],
        Key=object_key,
        Body=json.dumps(article, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )
    return object_key


@task(name="check_s3_object_exists_task")
def check_s3_object_exists_task(article_url: str, storage: dict[str, Any], aws_credentials_block_name: str) -> dict[str, Any]:
    article_id = _url_to_md5(article_url)
    object_key = _build_s3_object_key(storage["s3_prefix"], article_id)

    aws_credentials = AwsCredentials.load(aws_credentials_block_name)
    s3_client = _create_s3_client(aws_credentials)
    try:
        s3_client.head_object(Bucket=storage["s3_bucket"], Key=object_key)
        return {"exists": True, "id": article_id, "object_key": object_key}
    except Exception as exc:
        response = getattr(exc, "response", {})
        error = response.get("Error", {})
        error_code = str(error.get("Code", ""))
        if error_code in {"404", "NoSuchKey", "NotFound"}:
            return {"exists": False, "id": article_id, "object_key": object_key}
        raise


@task(name="summarize_briefing_task")
def summarize_briefing_task(article_content: str, ollama_connection: dict[str, str], timeout_sec: int = 120) -> str:
    logger = _get_task_logger()
    prompt = _build_briefing_prompt(article_content)
    logger.debug("ollama briefing prompt: %s", prompt)

    payload = {
        "model": ollama_connection["model"],
        "prompt": prompt,
        "stream": False,
    }
    request = Request(
        f"{ollama_connection['base_url'].rstrip('/')}/api/generate",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=timeout_sec) as response:
        body = response.read().decode("utf-8")
    response_json = json.loads(body)
    summary = response_json.get("response", "").strip()
    logger.debug("ollama briefing response: %s", summary)
    if not summary:
        raise ValueError("Ollama response does not contain summary text")
    return summary


@task(name="summarize_one_sentence_task")
def summarize_one_sentence_task(article_content: str, ollama_connection: dict[str, str], timeout_sec: int = 120) -> str:
    logger = _get_task_logger()
    prompt = _build_one_sentence_prompt(article_content)
    logger.debug("ollama one-sentence prompt: %s", prompt)

    payload = {
        "model": ollama_connection["model"],
        "prompt": prompt,
        "stream": False,
    }
    request = Request(
        f"{ollama_connection['base_url'].rstrip('/')}/api/generate",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=timeout_sec) as response:
        body = response.read().decode("utf-8")
    response_json = json.loads(body)
    summary = response_json.get("response", "").strip()
    logger.debug("ollama one-sentence response: %s", summary)
    if not summary:
        raise ValueError("Ollama response does not contain one sentence summary text")
    return summary


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
    ollama_connection = Secret.load(config["prefect_blocks"]["ollama_connection_secret_block"]).get()
    ollama_timeout_sec = config.get("ollama", {}).get("request_timeout_sec", 120)

    all_links: list[str] = []
    for feed_url in list(dict.fromkeys(config["rss_urls"])):
        links = fetch_feed_task(feed_url)
        all_links.extend(links)

    for link in all_links:
        logger.debug("all_links record: %s", link)

    unique_links = list(dict.fromkeys(all_links))
    logger.info("feed links extracted: total=%d", len(all_links))
    logger.info("feed links extracted: total=%d unique=%d", len(all_links), len(unique_links))

    article_count = 0
    stored_count = 0
    skipped_existing_count = 0
    for link in unique_links:
        s3_lookup = check_s3_object_exists_task(
            article_url=link,
            storage=config["storage"],
            aws_credentials_block_name=config["prefect_blocks"]["aws_credentials_block"],
        )
        if s3_lookup["exists"]:
            logger.info(
                "article already exists in s3, skip fetch/summarize: id=%s url=%s s3://%s/%s",
                s3_lookup["id"],
                link,
                config["storage"]["s3_bucket"],
                s3_lookup["object_key"],
            )
            skipped_existing_count += 1
            continue

        try:
            article = fetch_article_task(link)
        except Exception as exc:
            logger.warning("article fetch skipped: url=%s reason=%s", link, exc)
            continue

        try:
            article["briefing_summary"] = summarize_briefing_task(
                article_content=article["content"],
                ollama_connection=ollama_connection,
                timeout_sec=ollama_timeout_sec,
            )
            article["one_sentence_summary"] = summarize_one_sentence_task(
                article_content=article["content"],
                ollama_connection=ollama_connection,
                timeout_sec=ollama_timeout_sec,
            )
        except Exception as exc:
            logger.warning("article summarize skipped: url=%s reason=%s", link, exc)
            continue

        object_key = store_to_s3_task(
            article=article,
            storage=config["storage"],
            aws_credentials_block_name=config["prefect_blocks"]["aws_credentials_block"],
        )

        logger.debug(
            "article fetched and stored: id=%s url=%s s3://%s/%s title=%s metadata=%s content_length=%d",
            article["id"],
            article["url"],
            config["storage"]["s3_bucket"],
            object_key,
            article["title"],
            article["metadata"],
            len(article["content"]),
        )
        article_count += 1
        stored_count += 1

    logger.info("article fetching completed: total=%d", article_count)
    logger.info("article storing completed: total=%d", stored_count)
    logger.info("article skipped by existing s3 object: total=%d", skipped_existing_count)


if __name__ == "__main__":
    rss_ingest_flow()
