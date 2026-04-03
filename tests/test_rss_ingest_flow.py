from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from flows import rss_ingest_flow


VALID_CONFIG_YAML = """
rss_urls:
  - https://example.com/rss.xml
retry:
  max_retries: 3
  initial_delay_sec: 2
  backoff_multiplier: 2
storage:
  s3_bucket: news-bucket
  s3_prefix: rss
prefect_blocks:
  aws_credentials_block: aws-credentials-prod
  ollama_connection_secret_block: ollama-connection
"""


INVALID_EMPTY_RSS_CONFIG_YAML = """
rss_urls: []
retry:
  max_retries: 3
storage:
  s3_bucket: news-bucket
  s3_prefix: rss
prefect_blocks:
  aws_credentials_block: aws-credentials-prod
  ollama_connection_secret_block: ollama-connection
"""


def test_load_config_task_returns_config_when_valid(tmp_path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(VALID_CONFIG_YAML, encoding="utf-8")

    config = rss_ingest_flow.load_config_task.fn(str(config_path))

    assert config["rss_urls"] == ["https://example.com/rss.xml"]
    assert config["retry"]["max_retries"] == 3
    assert config["storage"]["s3_prefix"] == "rss"
    assert config["prefect_blocks"]["aws_credentials_block"] == "aws-credentials-prod"


def test_load_config_task_raises_when_rss_urls_is_empty(tmp_path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(INVALID_EMPTY_RSS_CONFIG_YAML, encoding="utf-8")

    with pytest.raises(ValueError):
        rss_ingest_flow.load_config_task.fn(str(config_path))


@patch("flows.rss_ingest_flow.get_run_logger")
@patch("flows.rss_ingest_flow.Secret")
@patch("flows.rss_ingest_flow.AwsCredentials")
def test_validate_prerequisites_task_loads_blocks_and_json_secret(
    mock_aws_credentials: MagicMock,
    mock_secret: MagicMock,
    mock_get_run_logger: MagicMock,
) -> None:
    mock_get_run_logger.return_value = MagicMock()
    mock_secret.load.return_value.get.return_value = {"base_url": "http://localhost:11434", "model": "llama3.1:8b"}

    parsed_config = {
        "rss_urls": ["https://example.com/rss.xml", "https://example.com/rss.xml"],
        "retry": {"max_retries": 3},
        "storage": {"s3_bucket": "news-bucket", "s3_prefix": "rss"},
        "prefect_blocks": {
            "aws_credentials_block": "aws-credentials-prod",
            "ollama_connection_secret_block": "ollama-connection",
        },
    }

    rss_ingest_flow.validate_prerequisites_task.fn(parsed_config)

    mock_aws_credentials.load.assert_called_once_with("aws-credentials-prod")
    mock_secret.load.assert_called_once_with("ollama-connection")


@patch("flows.rss_ingest_flow.get_run_logger")
@patch("flows.rss_ingest_flow.Secret")
@patch("flows.rss_ingest_flow.AwsCredentials")
def test_validate_prerequisites_task_raises_when_secret_value_is_not_dict(
    mock_aws_credentials: MagicMock,
    mock_secret: MagicMock,
    mock_get_run_logger: MagicMock,
) -> None:
    mock_get_run_logger.return_value = MagicMock()
    mock_secret.load.return_value.get.return_value = "not-dict"

    parsed_config = {
        "rss_urls": ["https://example.com/rss.xml"],
        "retry": {"max_retries": 3},
        "storage": {"s3_bucket": "news-bucket", "s3_prefix": "rss"},
        "prefect_blocks": {
            "aws_credentials_block": "aws-credentials-prod",
            "ollama_connection_secret_block": "ollama-connection",
        },
    }

    with pytest.raises(ValueError):
        rss_ingest_flow.validate_prerequisites_task.fn(parsed_config)

    mock_aws_credentials.load.assert_called_once_with("aws-credentials-prod")


RSS_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Example RSS</title>
    <item><title>A</title><link>https://example.com/a</link></item>
    <item><title>B</title><link>https://example.com/b</link></item>
    <item><title>B duplicate</title><link>https://example.com/b</link></item>
    <item><title>C redirect</title><link>https://www.google.com/url?url=https%3A%2F%2Fexample.com%2Fc&amp;sa=D</link></item>
  </channel>
</rss>
"""


ATOM_XML = b"""<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Example Atom</title>
  <entry><title>A</title><link rel="alternate" href="https://example.com/atom-a" /></entry>
  <entry><title>B</title><link href="https://example.com/atom-b" /></entry>
</feed>
"""


ARTICLE_HTML = b"""<!doctype html>
<html>
  <head>
    <title>Example title</title>
    <meta property="og:site_name" content="Example Site">
    <meta property="og:image" content="https://example.com/image.jpg">
    <meta property="article:published_time" content="2026-04-01T12:34:56Z">
    <meta property="article:tag" content="ai">
    <meta name="author" content="Jane Doe">
    <meta name="keywords" content="tech,python">
    <meta name="language" content="ja">
  </head>
  <body>
    <article>
      <h1>Heading</h1>
      <p>Paragraph A.</p>
      <p>Paragraph B.</p>
    </article>
    <script>ignored()</script>
  </body>
</html>
"""


def test_extract_links_from_feed_xml_supports_rss_and_deduplicates() -> None:
    links = rss_ingest_flow._extract_links_from_feed_xml(RSS_XML)

    assert links == ["https://example.com/a", "https://example.com/b", "https://example.com/c"]


def test_extract_links_from_feed_xml_supports_atom() -> None:
    links = rss_ingest_flow._extract_links_from_feed_xml(ATOM_XML)

    assert links == ["https://example.com/atom-a", "https://example.com/atom-b"]


@patch("flows.rss_ingest_flow.get_run_logger")
@patch("flows.rss_ingest_flow.urlopen")
def test_fetch_feed_task_returns_links_from_feed(
    mock_urlopen: MagicMock,
    mock_get_run_logger: MagicMock,
) -> None:
    mock_response = MagicMock()
    mock_response.read.return_value = RSS_XML
    mock_urlopen.return_value.__enter__.return_value = mock_response
    mock_logger = MagicMock()
    mock_get_run_logger.return_value = mock_logger

    links = rss_ingest_flow.fetch_feed_task.fn("https://example.com/rss.xml")

    assert links == ["https://example.com/a", "https://example.com/b", "https://example.com/c"]
    mock_urlopen.assert_called_once_with("https://example.com/rss.xml", timeout=30)
    mock_logger.debug.assert_called_once_with(
        "extracted links: feed_url=%s links=%s",
        "https://example.com/rss.xml",
        ["https://example.com/a", "https://example.com/b", "https://example.com/c"],
    )


@patch("flows.rss_ingest_flow.get_run_logger")
@patch("flows.rss_ingest_flow.urlopen")
def test_fetch_feed_task_raises_when_no_entries(
    mock_urlopen: MagicMock,
    mock_get_run_logger: MagicMock,
) -> None:
    mock_response = MagicMock()
    mock_response.read.return_value = b"<rss><channel><title>empty</title></channel></rss>"
    mock_urlopen.return_value.__enter__.return_value = mock_response
    mock_logger = MagicMock()
    mock_get_run_logger.return_value = mock_logger

    with pytest.raises(ValueError):
        rss_ingest_flow.fetch_feed_task.fn("https://example.com/empty.xml")

    mock_logger.debug.assert_called_once_with(
        "extracted links: feed_url=%s links=%s",
        "https://example.com/empty.xml",
        [],
    )


@patch("flows.rss_ingest_flow.trafilatura.extract")
def test_extract_article_content_and_metadata(mock_trafilatura_extract: MagicMock) -> None:
    mock_trafilatura_extract.return_value = "Main content from trafilatura"

    extracted = rss_ingest_flow._extract_article_content_and_metadata(ARTICLE_HTML.decode("utf-8"))

    assert extracted["title"] == "Example title"
    assert extracted["content"] == "Main content from trafilatura"
    assert extracted["metadata"] == {
        "author": "Jane Doe",
        "image_url": "https://example.com/image.jpg",
        "language": "ja",
        "published_timestamp": "2026-04-01T12:34:56Z",
        "site_name": "Example Site",
        "tags": ["ai", "python", "tech"],
    }


@patch("flows.rss_ingest_flow.trafilatura.extract")
@patch("flows.rss_ingest_flow.urlopen")
def test_fetch_article_task_returns_content_and_metadata(
    mock_urlopen: MagicMock,
    mock_trafilatura_extract: MagicMock,
) -> None:
    mock_trafilatura_extract.return_value = "Main content from trafilatura"
    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.read.return_value = ARTICLE_HTML
    mock_response.headers.get_content_charset.return_value = "utf-8"
    mock_urlopen.return_value.__enter__.return_value = mock_response

    article = rss_ingest_flow.fetch_article_task.fn("https://example.com/posts/1")

    assert article["id"] == "de0617c481337158695d4e48d5c275d2"
    assert article["url"] == "https://example.com/posts/1"
    assert article["title"] == "Example title"
    assert article["content"] == "Main content from trafilatura"
    assert "<html>" in article["raw_html"]
    assert article["metadata"]["author"] == "Jane Doe"
    assert article["metadata"]["tags"] == ["ai", "python", "tech"]


@patch("flows.rss_ingest_flow.trafilatura.extract")
def test_extract_article_content_and_metadata_falls_back_when_trafilatura_returns_none(
    mock_trafilatura_extract: MagicMock,
) -> None:
    mock_trafilatura_extract.return_value = None

    extracted = rss_ingest_flow._extract_article_content_and_metadata(ARTICLE_HTML.decode("utf-8"))

    assert "Heading" in extracted["content"]
    assert "ignored()" not in extracted["content"]


@patch("flows.rss_ingest_flow.AwsCredentials")
def test_store_to_s3_task_stores_article_json_with_hashed_key(mock_aws_credentials: MagicMock) -> None:
    mock_s3_client = MagicMock()
    mock_session = MagicMock()
    mock_session.client.return_value = mock_s3_client
    mock_aws_credentials.load.return_value.get_boto3_session.return_value = mock_session

    key = rss_ingest_flow.store_to_s3_task.fn(
        article={"id": "ab1234567890abcdef", "url": "https://example.com/a", "title": "A"},
        storage={"s3_bucket": "news-bucket", "s3_prefix": "rss"},
        aws_credentials_block_name="aws-credentials-prod",
    )

    assert key == "rss/ab/ab1234567890abcdef.json"
    mock_aws_credentials.load.assert_called_once_with("aws-credentials-prod")
    mock_session.client.assert_called_once_with("s3")
    mock_s3_client.put_object.assert_called_once()
    put_object_kwargs = mock_s3_client.put_object.call_args.kwargs
    assert put_object_kwargs["Bucket"] == "news-bucket"
    assert put_object_kwargs["Key"] == "rss/ab/ab1234567890abcdef.json"


def test_create_s3_client_parses_json_string_botocore_config() -> None:
    mock_s3_client = MagicMock()
    mock_session = MagicMock()
    mock_session.client.return_value = mock_s3_client

    mock_aws_credentials = MagicMock()
    mock_aws_credentials.get_boto3_session.return_value = mock_session
    mock_aws_credentials.aws_client_parameters = {
        "endpoint_url": "https://s3.example.com",
        "config": '{"s3":{"addressing_style":"path"}}',
    }

    client = rss_ingest_flow._create_s3_client(mock_aws_credentials)

    assert client is mock_s3_client
    mock_session.client.assert_called_once()
    call_kwargs = mock_session.client.call_args.kwargs
    assert call_kwargs["endpoint_url"] == "https://s3.example.com"
    assert call_kwargs["config"].s3 == {"addressing_style": "path"}


def test_create_s3_client_raises_when_json_string_botocore_config_is_invalid() -> None:
    mock_session = MagicMock()
    mock_aws_credentials = MagicMock()
    mock_aws_credentials.get_boto3_session.return_value = mock_session
    mock_aws_credentials.aws_client_parameters = {
        "endpoint_url": "https://s3.example.com",
        "config": '{"s3":{"addressing_style":"path"',
    }

    with pytest.raises(ValueError):
        rss_ingest_flow._create_s3_client(mock_aws_credentials)


@patch("flows.rss_ingest_flow.AwsCredentials")
def test_check_s3_object_exists_task_returns_true_when_object_exists(mock_aws_credentials: MagicMock) -> None:
    mock_s3_client = MagicMock()
    mock_session = MagicMock()
    mock_session.client.return_value = mock_s3_client
    mock_aws_credentials.load.return_value.get_boto3_session.return_value = mock_session

    result = rss_ingest_flow.check_s3_object_exists_task.fn(
        article_url="https://example.com/posts/1",
        storage={"s3_bucket": "news-bucket", "s3_prefix": "rss"},
        aws_credentials_block_name="aws-credentials-prod",
    )

    assert result["exists"] is True
    assert result["id"] == "de0617c481337158695d4e48d5c275d2"
    assert result["object_key"] == "rss/de/de0617c481337158695d4e48d5c275d2.json"
    mock_s3_client.head_object.assert_called_once_with(
        Bucket="news-bucket",
        Key="rss/de/de0617c481337158695d4e48d5c275d2.json",
    )


@patch("flows.rss_ingest_flow.AwsCredentials")
def test_check_s3_object_exists_task_returns_false_when_object_not_found(mock_aws_credentials: MagicMock) -> None:
    mock_s3_client = MagicMock()
    mock_session = MagicMock()
    mock_session.client.return_value = mock_s3_client
    mock_aws_credentials.load.return_value.get_boto3_session.return_value = mock_session

    not_found_error = Exception("not found")
    not_found_error.response = {"Error": {"Code": "404"}}
    mock_s3_client.head_object.side_effect = not_found_error

    result = rss_ingest_flow.check_s3_object_exists_task.fn(
        article_url="https://example.com/posts/1",
        storage={"s3_bucket": "news-bucket", "s3_prefix": "rss"},
        aws_credentials_block_name="aws-credentials-prod",
    )

    assert result["exists"] is False
    assert result["id"] == "de0617c481337158695d4e48d5c275d2"
    assert result["object_key"] == "rss/de/de0617c481337158695d4e48d5c275d2.json"


@patch("flows.rss_ingest_flow.urlopen")
def test_fetch_article_task_raises_when_status_is_not_200(mock_urlopen: MagicMock) -> None:
    mock_response = MagicMock()
    mock_response.status = 500
    mock_urlopen.return_value.__enter__.return_value = mock_response

    with pytest.raises(ValueError):
        rss_ingest_flow.fetch_article_task.fn("https://example.com/posts/error")


@patch("flows.rss_ingest_flow.check_s3_object_exists_task")
@patch("flows.rss_ingest_flow.store_to_s3_task")
@patch("flows.rss_ingest_flow.fetch_article_task")
@patch("flows.rss_ingest_flow.fetch_feed_task")
@patch("flows.rss_ingest_flow.validate_prerequisites_task")
@patch("flows.rss_ingest_flow.load_config_task")
def test_rss_ingest_flow_continues_when_article_fetch_fails(
    mock_load_config_task: MagicMock,
    mock_validate_prerequisites_task: MagicMock,
    mock_fetch_feed_task: MagicMock,
    mock_fetch_article_task: MagicMock,
    mock_store_to_s3_task: MagicMock,
    mock_check_s3_object_exists_task: MagicMock,
) -> None:
    mock_load_config_task.return_value = {
        "rss_urls": ["https://example.com/rss.xml"],
        "retry": {"max_retries": 3},
        "storage": {"s3_bucket": "news-bucket", "s3_prefix": "rss"},
        "prefect_blocks": {
            "aws_credentials_block": "aws-credentials-prod",
            "ollama_connection_secret_block": "ollama-connection",
        },
    }
    mock_fetch_feed_task.return_value = ["https://example.com/a", "https://example.com/b"]
    mock_check_s3_object_exists_task.side_effect = [
        {"exists": False, "id": "aaaa", "object_key": "rss/aa/aaaa.json"},
        {"exists": False, "id": "bbbb", "object_key": "rss/bb/bbbb.json"},
    ]
    mock_fetch_article_task.side_effect = [
        ValueError("unexpected status code: 500"),
        {"id": "bbbb", "url": "https://example.com/b", "title": "B", "metadata": {}, "content": "ok"},
    ]
    mock_store_to_s3_task.return_value = "rss/bb/bbbb.json"

    with patch("flows.rss_ingest_flow._get_task_logger") as mock_get_task_logger:
        mock_logger = MagicMock()
        mock_get_task_logger.return_value = mock_logger
        rss_ingest_flow.rss_ingest_flow.fn("config.yaml")

    mock_logger.warning.assert_called_once()
    warning_args = mock_logger.warning.call_args[0]
    assert warning_args[0] == "article fetch skipped: url=%s reason=%s"
    assert warning_args[1] == "https://example.com/a"
    assert str(warning_args[2]) == "unexpected status code: 500"
    mock_logger.info.assert_any_call("feed links extracted: total=%d", 2)
    mock_logger.info.assert_any_call("feed links extracted: total=%d unique=%d", 2, 2)
    mock_logger.info.assert_any_call("article fetching completed: total=%d", 1)


@patch("flows.rss_ingest_flow.check_s3_object_exists_task")
@patch("flows.rss_ingest_flow.store_to_s3_task")
@patch("flows.rss_ingest_flow.fetch_article_task")
@patch("flows.rss_ingest_flow.fetch_feed_task")
@patch("flows.rss_ingest_flow.validate_prerequisites_task")
@patch("flows.rss_ingest_flow.load_config_task")
def test_rss_ingest_flow_continues_when_article_fetch_raises_unexpected_exception(
    mock_load_config_task: MagicMock,
    mock_validate_prerequisites_task: MagicMock,
    mock_fetch_feed_task: MagicMock,
    mock_fetch_article_task: MagicMock,
    mock_store_to_s3_task: MagicMock,
    mock_check_s3_object_exists_task: MagicMock,
) -> None:
    mock_load_config_task.return_value = {
        "rss_urls": ["https://example.com/rss.xml"],
        "retry": {"max_retries": 3},
        "storage": {"s3_bucket": "news-bucket", "s3_prefix": "rss"},
        "prefect_blocks": {
            "aws_credentials_block": "aws-credentials-prod",
            "ollama_connection_secret_block": "ollama-connection",
        },
    }
    mock_fetch_feed_task.return_value = ["https://example.com/a", "https://example.com/b"]
    mock_check_s3_object_exists_task.side_effect = [
        {"exists": False, "id": "aaaa", "object_key": "rss/aa/aaaa.json"},
        {"exists": False, "id": "bbbb", "object_key": "rss/bb/bbbb.json"},
    ]
    mock_fetch_article_task.side_effect = [
        RuntimeError("boom"),
        {"id": "bbbb", "url": "https://example.com/b", "title": "B", "metadata": {}, "content": "ok"},
    ]
    mock_store_to_s3_task.return_value = "rss/bb/bbbb.json"

    with patch("flows.rss_ingest_flow._get_task_logger") as mock_get_task_logger:
        mock_logger = MagicMock()
        mock_get_task_logger.return_value = mock_logger
        rss_ingest_flow.rss_ingest_flow.fn("config.yaml")

    mock_logger.warning.assert_called_once()
    warning_args = mock_logger.warning.call_args[0]
    assert warning_args[0] == "article fetch skipped: url=%s reason=%s"
    assert warning_args[1] == "https://example.com/a"
    assert str(warning_args[2]) == "boom"
    mock_logger.info.assert_any_call("article fetching completed: total=%d", 1)


@patch("flows.rss_ingest_flow.check_s3_object_exists_task")
@patch("flows.rss_ingest_flow.store_to_s3_task")
@patch("flows.rss_ingest_flow.fetch_article_task")
@patch("flows.rss_ingest_flow.fetch_feed_task")
@patch("flows.rss_ingest_flow.validate_prerequisites_task")
@patch("flows.rss_ingest_flow.load_config_task")
def test_rss_ingest_flow_skips_fetch_when_s3_object_exists(
    mock_load_config_task: MagicMock,
    mock_validate_prerequisites_task: MagicMock,
    mock_fetch_feed_task: MagicMock,
    mock_fetch_article_task: MagicMock,
    mock_store_to_s3_task: MagicMock,
    mock_check_s3_object_exists_task: MagicMock,
) -> None:
    mock_load_config_task.return_value = {
        "rss_urls": ["https://example.com/rss.xml"],
        "retry": {"max_retries": 3},
        "storage": {"s3_bucket": "news-bucket", "s3_prefix": "rss"},
        "prefect_blocks": {
            "aws_credentials_block": "aws-credentials-prod",
            "ollama_connection_secret_block": "ollama-connection",
        },
    }
    mock_fetch_feed_task.return_value = ["https://example.com/a"]
    mock_check_s3_object_exists_task.return_value = {
        "exists": True,
        "id": "aaaa",
        "object_key": "rss/aa/aaaa.json",
    }

    with patch("flows.rss_ingest_flow._get_task_logger") as mock_get_task_logger:
        mock_logger = MagicMock()
        mock_get_task_logger.return_value = mock_logger
        rss_ingest_flow.rss_ingest_flow.fn("config.yaml")

    mock_fetch_article_task.assert_not_called()
    mock_store_to_s3_task.assert_not_called()
    mock_logger.info.assert_any_call("article skipped by existing s3 object: total=%d", 1)


def test_get_task_logger_returns_standard_logger_when_prefect_context_missing() -> None:
    with patch("flows.rss_ingest_flow.get_run_logger", side_effect=rss_ingest_flow.MissingContextError("missing")):
        logger = rss_ingest_flow._get_task_logger()

    assert logger.name == "flows.rss_ingest_flow"


def test_normalize_extracted_link_extracts_google_redirect_url() -> None:
    link = "https://www.google.com/url?url=https%3A%2F%2Fexample.com%2Fdest&sa=D"

    normalized = rss_ingest_flow._normalize_extracted_link(link)

    assert normalized == "https://example.com/dest"
