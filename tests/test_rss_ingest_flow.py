from __future__ import annotations

import sqlite3
from pathlib import Path
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
ollama:
  models:
    rss_ingest_flow: qwen3.5:0.8b
    rss_ingest_flow_embedding: nomic-embed-text
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
ollama:
  models:
    rss_ingest_flow: qwen3.5:0.8b
    rss_ingest_flow_embedding: nomic-embed-text
"""


def test_load_config_task_returns_config_when_valid(tmp_path) -> None:
    """Test case: load config task returns config when valid."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(VALID_CONFIG_YAML, encoding="utf-8")

    config = rss_ingest_flow.load_config_task.fn(str(config_path))

    assert config["rss_urls"] == ["https://example.com/rss.xml"]
    assert config["retry"]["max_retries"] == 3
    assert config["storage"]["s3_prefix"] == "rss"
    assert config["prefect_blocks"]["aws_credentials_block"] == "aws-credentials-prod"


def test_load_config_task_raises_when_rss_urls_is_empty(tmp_path) -> None:
    """Test case: load config task raises when rss urls is empty."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(INVALID_EMPTY_RSS_CONFIG_YAML, encoding="utf-8")

    with pytest.raises(ValueError):
        rss_ingest_flow.load_config_task.fn(str(config_path))


def test_load_config_task_raises_when_ollama_timeout_is_invalid(tmp_path) -> None:
    """Test case: load config task raises when ollama timeout is invalid."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
rss_urls:
  - https://example.com/rss.xml
retry:
  max_retries: 3
storage:
  s3_bucket: news-bucket
  s3_prefix: rss
ollama:
  request_timeout_sec: 0
  models:
    rss_ingest_flow: qwen3.5:0.8b
    rss_ingest_flow_embedding: nomic-embed-text
prefect_blocks:
  aws_credentials_block: aws-credentials-prod
  ollama_connection_secret_block: ollama-connection
""",
        encoding="utf-8",
    )

    with pytest.raises(ValueError):
        rss_ingest_flow.load_config_task.fn(str(config_path))


@patch("flows.rss_ingest_flow.get_run_logger")
@patch("flows.rss_ingest_flow.load_ollama_connection_secret")
@patch("flows.rss_ingest_flow.AwsCredentials")
def test_validate_prerequisites_task_loads_blocks_and_json_secret(
    mock_aws_credentials: MagicMock,
    mock_load_ollama_connection_secret: MagicMock,
    mock_get_run_logger: MagicMock,
) -> None:
    mock_get_run_logger.return_value = MagicMock()
    mock_load_ollama_connection_secret.return_value = {"base_url": "http://localhost:11434", "model": "llama3.1:8b"}

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
    mock_load_ollama_connection_secret.assert_called_once()


@patch("flows.rss_ingest_flow.get_run_logger")
@patch("flows.rss_ingest_flow.load_ollama_connection_secret")
@patch("flows.rss_ingest_flow.AwsCredentials")
def test_validate_prerequisites_task_raises_when_secret_value_is_not_dict(
    mock_aws_credentials: MagicMock,
    mock_load_ollama_connection_secret: MagicMock,
    mock_get_run_logger: MagicMock,
) -> None:
    mock_get_run_logger.return_value = MagicMock()
    mock_load_ollama_connection_secret.side_effect = ValueError("Ollama Secret block value must be a dict")

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


OLLAMA_GENERATE_RESPONSE = b'{"response":"summary text"}'


def test_extract_links_from_feed_xml_supports_rss_and_deduplicates() -> None:
    """Test case: extract links from feed xml supports rss and deduplicates."""
    links = rss_ingest_flow._extract_links_from_feed_xml(RSS_XML)

    assert links == ["https://example.com/a", "https://example.com/b", "https://example.com/c"]


def test_extract_links_from_feed_xml_supports_atom() -> None:
    """Test case: extract links from feed xml supports atom."""
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
    """Test case: extract article content and metadata."""
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


def test_build_briefing_prompt_includes_article_content() -> None:
    """Test case: build briefing prompt includes article content."""
    prompt = rss_ingest_flow._build_briefing_prompt("本文です")
    assert "本文です" in prompt
    assert "# ニュース記事" in prompt


def test_build_one_sentence_prompt_includes_article_content() -> None:
    """Test case: build one sentence prompt includes article content."""
    prompt = rss_ingest_flow._build_one_sentence_prompt("本文です")
    assert "本文です" in prompt
    assert "一文" in prompt


def test_summarize_briefing_task_returns_ollama_response() -> None:
    """Test case: summarize briefing task returns ollama response."""
    mock_logger = MagicMock()

    with (
        patch("flows.rss_ingest_flow._get_task_logger", return_value=mock_logger),
        patch("flows.rss_ingest_flow.invoke_ollama_generate", return_value="summary text") as mock_invoke_ollama_generate,
    ):
        summary = rss_ingest_flow.summarize_briefing_task.fn(
            article_content="本文",
            ollama_connection={"base_url": "http://localhost:11434", "model": "llama3.1:8b"},
            timeout_sec=45,
        )

    assert summary == "summary text"
    assert mock_logger.debug.call_count == 2
    assert mock_invoke_ollama_generate.call_args.kwargs["timeout_sec"] == 45
    assert mock_invoke_ollama_generate.call_args.kwargs["ollama_connection"]["model"] == "llama3.1:8b"
    assert mock_invoke_ollama_generate.call_args.kwargs["logger"] is mock_logger


def test_summarize_one_sentence_task_returns_ollama_response() -> None:
    """Test case: summarize one sentence task returns ollama response."""
    mock_logger = MagicMock()

    with (
        patch("flows.rss_ingest_flow._get_task_logger", return_value=mock_logger),
        patch("flows.rss_ingest_flow.invoke_ollama_generate", return_value="summary text") as mock_invoke_ollama_generate,
    ):
        summary = rss_ingest_flow.summarize_one_sentence_task.fn(
            article_content="本文",
            ollama_connection={"base_url": "http://localhost:11434", "model": "llama3.1:8b"},
            timeout_sec=30,
        )

    assert summary == "summary text"
    assert mock_logger.debug.call_count == 2
    assert mock_invoke_ollama_generate.call_args.kwargs["timeout_sec"] == 30
    assert mock_invoke_ollama_generate.call_args.kwargs["ollama_connection"]["model"] == "llama3.1:8b"
    assert mock_invoke_ollama_generate.call_args.kwargs["logger"] is mock_logger


@patch("flows.rss_ingest_flow.invoke_ollama_embeddings")
def test_upsert_briefing_embedding_to_sqlite_task_stores_embedding(mock_invoke_ollama_embeddings: MagicMock, tmp_path) -> None:
    """Test case: upsert briefing embedding to sqlite task stores embedding."""
    sqlite_path = tmp_path / "rss_embeddings.sqlite3"
    mock_invoke_ollama_embeddings.return_value = [0.1, 0.2, 0.3]

    rss_ingest_flow.upsert_briefing_embedding_to_sqlite_task.fn(
        article={
            "id": "article-1",
            "url": "https://example.com/a",
            "title": "Article title",
            "briefing_summary": "summary text",
            "one_sentence_summary": "one sentence",
            "published_timestamp": "2026-04-04T00:00:00+00:00",
            "fetch_timestamp": "2026-04-04T01:00:00+00:00",
            "metadata": {"source_feed_url": "https://example.com/rss.xml"},
        },
        embedding_connection={"base_url": "http://localhost:11434", "model": "nomic-embed-text"},
        sqlite_path=str(sqlite_path),
        timeout_sec=30,
    )

    import sqlite3

    with sqlite3.connect(sqlite_path) as conn:
        row = conn.execute(
            """
            SELECT article_id, article_url, title, published_timestamp, fetch_timestamp, briefing_summary, one_sentence_summary, metadata_json, embedding_json, embedding_timestamp
            FROM article_embeddings
            WHERE article_id=?
            """,
            ("article-1",),
        ).fetchone()

    assert row is not None
    assert row[0] == "article-1"
    assert row[1] == "https://example.com/a"
    assert row[2] == "Article title"
    assert row[3] == "2026-04-04T00:00:00+00:00"
    assert row[4] == "2026-04-04T01:00:00+00:00"
    assert row[5] == "summary text"
    assert row[6] == "one sentence"
    assert row[7] == '{"source_feed_url": "https://example.com/rss.xml"}'
    assert row[8] == "[0.1, 0.2, 0.3]"
    assert isinstance(row[9], str) and row[9] != ""


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
    """Test case: store to s3 task stores article json with hashed key."""
    mock_s3_client = MagicMock()
    mock_session = MagicMock()
    mock_session.client.return_value = mock_s3_client
    mock_credentials = mock_aws_credentials.load.return_value
    mock_credentials.get_boto3_session.return_value = mock_session
    mock_credentials.aws_client_parameters = None

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
    """Test case: create s3 client parses json string botocore config."""
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
    """Test case: create s3 client raises when json string botocore config is invalid."""
    mock_session = MagicMock()
    mock_aws_credentials = MagicMock()
    mock_aws_credentials.get_boto3_session.return_value = mock_session
    mock_aws_credentials.aws_client_parameters = {
        "endpoint_url": "https://s3.example.com",
        "config": '{"s3":{"addressing_style":"path"',
    }

    with pytest.raises(ValueError):
        rss_ingest_flow._create_s3_client(mock_aws_credentials)


def test_check_s3_object_exists_task_returns_true_when_embedding_record_exists(tmp_path: Path) -> None:
    """Test case: check s3 object exists task returns true when embedding record exists."""
    sqlite_path = tmp_path / "rss_embeddings.sqlite3"
    rss_ingest_flow._initialize_embeddings_sqlite(str(sqlite_path))
    with sqlite3.connect(sqlite_path) as conn:
        conn.execute(
            "INSERT INTO article_embeddings (article_id, model, embedding_json) VALUES (?, ?, ?)",
            ("de0617c481337158695d4e48d5c275d2", "nomic-embed-text", "[0.1,0.2]"),
        )
        conn.commit()
    result = rss_ingest_flow.check_s3_object_exists_task.fn(
        article_url="https://example.com/posts/1",
        sqlite_path=str(sqlite_path),
    )

    assert result["exists"] is True
    assert result["id"] == "de0617c481337158695d4e48d5c275d2"


def test_check_s3_object_exists_task_returns_false_when_embedding_record_not_found(tmp_path: Path) -> None:
    """Test case: check s3 object exists task returns false when embedding record not found."""
    sqlite_path = tmp_path / "rss_embeddings.sqlite3"
    result = rss_ingest_flow.check_s3_object_exists_task.fn(
        article_url="https://example.com/posts/1",
        sqlite_path=str(sqlite_path),
    )

    assert result["exists"] is False
    assert result["id"] == "de0617c481337158695d4e48d5c275d2"


@patch("flows.rss_ingest_flow.urlopen")
def test_fetch_article_task_raises_when_status_is_not_200(mock_urlopen: MagicMock) -> None:
    """Test case: fetch article task raises when status is not 200."""
    mock_response = MagicMock()
    mock_response.status = 500
    mock_urlopen.return_value.__enter__.return_value = mock_response

    with pytest.raises(ValueError):
        rss_ingest_flow.fetch_article_task.fn("https://example.com/posts/error")


@patch("flows.rss_ingest_flow.check_embedding_record_exists_task")
@patch("flows.rss_ingest_flow.store_to_s3_task")
@patch("flows.rss_ingest_flow.fetch_and_summarize_article_flow")
@patch("flows.rss_ingest_flow.fetch_feed_task")
@patch("flows.rss_ingest_flow.validate_prerequisites_task")
@patch("flows.rss_ingest_flow.load_ollama_connection_secret")
@patch("flows.rss_ingest_flow.load_config_task")
def test_rss_ingest_flow_continues_when_article_fetch_fails(
    mock_load_config_task: MagicMock,
    mock_load_ollama_connection_secret: MagicMock,
    mock_validate_prerequisites_task: MagicMock,
    mock_fetch_feed_task: MagicMock,
    mock_fetch_and_summarize_article_flow: MagicMock,
    mock_store_to_s3_task: MagicMock,
    mock_check_embedding_record_exists_task: MagicMock,
) -> None:
    mock_load_config_task.return_value = {
        "rss_urls": ["https://example.com/rss.xml"],
        "retry": {"max_retries": 3},
        "storage": {"s3_bucket": "news-bucket", "s3_prefix": "rss"},
            "ollama": {
                "models": {
                    "rss_ingest_flow": "qwen3.5:0.8b",
                    "rss_ingest_flow_embedding": "nomic-embed-text",
                }
            },
        "prefect_blocks": {
            "aws_credentials_block": "aws-credentials-prod",
            "ollama_connection_secret_block": "ollama-connection",
        },
    }
    mock_load_ollama_connection_secret.return_value = {"base_url": "http://localhost:11434", "model": "llama3.1:8b"}
    mock_fetch_feed_task.return_value = ["https://example.com/a", "https://example.com/b"]
    mock_check_embedding_record_exists_task.side_effect = [
        {"exists": False, "id": "aaaa"},
        {"exists": False, "id": "bbbb"},
    ]
    mock_fetch_and_summarize_article_flow.side_effect = [
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
    assert warning_args[0] == "article fetch/summarize skipped: url=%s reason=%s"
    assert warning_args[1] == "https://example.com/a"
    assert str(warning_args[2]) == "unexpected status code: 500"
    mock_logger.info.assert_any_call("feed links extracted: total=%d", 2)
    mock_logger.info.assert_any_call("feed links extracted: total=%d unique=%d", 2, 2)
    mock_logger.info.assert_any_call("article fetching completed: total=%d", 1)


@patch("flows.rss_ingest_flow.check_embedding_record_exists_task")
@patch("flows.rss_ingest_flow.store_to_s3_task")
@patch("flows.rss_ingest_flow.fetch_and_summarize_article_flow")
@patch("flows.rss_ingest_flow.fetch_feed_task")
@patch("flows.rss_ingest_flow.validate_prerequisites_task")
@patch("flows.rss_ingest_flow.load_ollama_connection_secret")
@patch("flows.rss_ingest_flow.load_config_task")
def test_rss_ingest_flow_continues_when_article_fetch_raises_unexpected_exception(
    mock_load_config_task: MagicMock,
    mock_load_ollama_connection_secret: MagicMock,
    mock_validate_prerequisites_task: MagicMock,
    mock_fetch_feed_task: MagicMock,
    mock_fetch_and_summarize_article_flow: MagicMock,
    mock_store_to_s3_task: MagicMock,
    mock_check_embedding_record_exists_task: MagicMock,
) -> None:
    mock_load_config_task.return_value = {
        "rss_urls": ["https://example.com/rss.xml"],
        "retry": {"max_retries": 3},
        "storage": {"s3_bucket": "news-bucket", "s3_prefix": "rss"},
            "ollama": {
                "models": {
                    "rss_ingest_flow": "qwen3.5:0.8b",
                    "rss_ingest_flow_embedding": "nomic-embed-text",
                }
            },
        "prefect_blocks": {
            "aws_credentials_block": "aws-credentials-prod",
            "ollama_connection_secret_block": "ollama-connection",
        },
    }
    mock_load_ollama_connection_secret.return_value = {"base_url": "http://localhost:11434", "model": "llama3.1:8b"}
    mock_fetch_feed_task.return_value = ["https://example.com/a", "https://example.com/b"]
    mock_check_embedding_record_exists_task.side_effect = [
        {"exists": False, "id": "aaaa"},
        {"exists": False, "id": "bbbb"},
    ]
    mock_fetch_and_summarize_article_flow.side_effect = [
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
    assert warning_args[0] == "article fetch/summarize skipped: url=%s reason=%s"
    assert warning_args[1] == "https://example.com/a"
    assert str(warning_args[2]) == "boom"
    mock_logger.info.assert_any_call("article fetching completed: total=%d", 1)


@patch("flows.rss_ingest_flow.check_embedding_record_exists_task")
@patch("flows.rss_ingest_flow.store_to_s3_task")
@patch("flows.rss_ingest_flow.fetch_and_summarize_article_flow")
@patch("flows.rss_ingest_flow.fetch_feed_task")
@patch("flows.rss_ingest_flow.validate_prerequisites_task")
@patch("flows.rss_ingest_flow.load_ollama_connection_secret")
@patch("flows.rss_ingest_flow.load_config_task")
def test_rss_ingest_flow_skips_fetch_when_s3_object_exists(
    mock_load_config_task: MagicMock,
    mock_load_ollama_connection_secret: MagicMock,
    mock_validate_prerequisites_task: MagicMock,
    mock_fetch_feed_task: MagicMock,
    mock_fetch_and_summarize_article_flow: MagicMock,
    mock_store_to_s3_task: MagicMock,
    mock_check_embedding_record_exists_task: MagicMock,
) -> None:
    mock_load_config_task.return_value = {
        "rss_urls": ["https://example.com/rss.xml"],
        "retry": {"max_retries": 3},
        "storage": {"s3_bucket": "news-bucket", "s3_prefix": "rss"},
            "ollama": {
                "models": {
                    "rss_ingest_flow": "qwen3.5:0.8b",
                    "rss_ingest_flow_embedding": "nomic-embed-text",
                }
            },
        "prefect_blocks": {
            "aws_credentials_block": "aws-credentials-prod",
            "ollama_connection_secret_block": "ollama-connection",
        },
    }
    mock_load_ollama_connection_secret.return_value = {"base_url": "http://localhost:11434", "model": "llama3.1:8b"}
    mock_fetch_feed_task.return_value = ["https://example.com/a"]
    mock_check_embedding_record_exists_task.return_value = {"exists": True, "id": "aaaa"}

    with patch("flows.rss_ingest_flow._get_task_logger") as mock_get_task_logger:
        mock_logger = MagicMock()
        mock_get_task_logger.return_value = mock_logger
        rss_ingest_flow.rss_ingest_flow.fn("config.yaml")

    mock_fetch_and_summarize_article_flow.assert_not_called()
    mock_store_to_s3_task.assert_not_called()
    mock_logger.info.assert_any_call("article skipped by existing sqlite embedding record: total=%d", 1)


@patch("flows.rss_ingest_flow.check_embedding_record_exists_task")
@patch("flows.rss_ingest_flow.store_to_s3_task")
@patch("flows.rss_ingest_flow.fetch_and_summarize_article_flow")
@patch("flows.rss_ingest_flow.fetch_feed_task")
@patch("flows.rss_ingest_flow.validate_prerequisites_task")
@patch("flows.rss_ingest_flow.load_ollama_connection_secret")
@patch("flows.rss_ingest_flow.load_config_task")
def test_rss_ingest_flow_skips_article_when_summarization_fails(
    mock_load_config_task: MagicMock,
    mock_load_ollama_connection_secret: MagicMock,
    mock_validate_prerequisites_task: MagicMock,
    mock_fetch_feed_task: MagicMock,
    mock_fetch_and_summarize_article_flow: MagicMock,
    mock_store_to_s3_task: MagicMock,
    mock_check_embedding_record_exists_task: MagicMock,
) -> None:
    mock_load_config_task.return_value = {
        "rss_urls": ["https://example.com/rss.xml"],
        "retry": {"max_retries": 3},
        "storage": {"s3_bucket": "news-bucket", "s3_prefix": "rss"},
            "ollama": {
                "models": {
                    "rss_ingest_flow": "qwen3.5:0.8b",
                    "rss_ingest_flow_embedding": "nomic-embed-text",
                }
            },
        "prefect_blocks": {
            "aws_credentials_block": "aws-credentials-prod",
            "ollama_connection_secret_block": "ollama-connection",
        },
    }
    mock_load_ollama_connection_secret.return_value = {"base_url": "http://localhost:11434", "model": "llama3.1:8b"}
    mock_fetch_feed_task.return_value = ["https://example.com/a"]
    mock_check_embedding_record_exists_task.return_value = {"exists": False, "id": "aaaa"}
    mock_fetch_and_summarize_article_flow.side_effect = RuntimeError("ollama error")

    with patch("flows.rss_ingest_flow._get_task_logger") as mock_get_task_logger:
        mock_logger = MagicMock()
        mock_get_task_logger.return_value = mock_logger
        rss_ingest_flow.rss_ingest_flow.fn("config.yaml")

    mock_store_to_s3_task.assert_not_called()
    mock_logger.warning.assert_called_once()
    warning_args = mock_logger.warning.call_args[0]
    assert warning_args[0] == "article fetch/summarize skipped: url=%s reason=%s"
    assert warning_args[1] == "https://example.com/a"
    assert str(warning_args[2]) == "ollama error"


def test_get_task_logger_returns_standard_logger_when_prefect_context_missing() -> None:
    """Test case: get task logger returns standard logger when prefect context missing."""
    with patch("flows.rss_ingest_flow.get_run_logger", side_effect=rss_ingest_flow.MissingContextError("missing")):
        logger = rss_ingest_flow._get_task_logger()

    assert logger.name == "flows.rss_ingest_flow"


def test_normalize_extracted_link_extracts_google_redirect_url() -> None:
    """Test case: normalize extracted link extracts google redirect url."""
    link = "https://www.google.com/url?url=https%3A%2F%2Fexample.com%2Fdest&sa=D"

    normalized = rss_ingest_flow._normalize_extracted_link(link)

    assert normalized == "https://example.com/dest"


@patch("flows.rss_ingest_flow.fetch_and_summarize_article_flow")
@patch("flows.rss_ingest_flow.validate_prerequisites_task")
@patch("flows.rss_ingest_flow.load_ollama_connection_secret")
@patch("flows.rss_ingest_flow.load_config_task")
def test_fetch_summarize_url_flow_returns_article_with_summaries(
    mock_load_config_task: MagicMock,
    mock_load_ollama_connection_secret: MagicMock,
    mock_validate_prerequisites_task: MagicMock,
    mock_fetch_and_summarize_article_flow: MagicMock,
) -> None:
    mock_load_config_task.return_value = {
        "rss_urls": ["https://example.com/rss.xml"],
        "retry": {"max_retries": 3},
        "storage": {"s3_bucket": "news-bucket", "s3_prefix": "rss"},
        "ollama": {"request_timeout_sec": 45, "models": {"rss_ingest_flow": "qwen3.5:0.8b"}},
        "prefect_blocks": {
            "aws_credentials_block": "aws-credentials-prod",
            "ollama_connection_secret_block": "ollama-connection",
        },
    }
    mock_load_ollama_connection_secret.return_value = {"base_url": "http://localhost:11434"}
    mock_fetch_and_summarize_article_flow.return_value = {
        "id": "aaaa",
        "url": "https://example.com/a",
        "title": "A",
        "metadata": {},
        "content": "本文",
        "briefing_summary": "briefing summary",
        "one_sentence_summary": "one sentence",
        "raw_html": "<html></html>",
    }

    article = rss_ingest_flow.fetch_summarize_url_flow.fn("https://example.com/a", "config.yaml")

    assert article["briefing_summary"] == "briefing summary"
    assert article["one_sentence_summary"] == "one sentence"
    mock_validate_prerequisites_task.assert_called_once()
    mock_fetch_and_summarize_article_flow.assert_called_once()
    assert mock_fetch_and_summarize_article_flow.call_args.kwargs["article_url"] == "https://example.com/a"
    assert mock_fetch_and_summarize_article_flow.call_args.kwargs["timeout_sec"] == 45


def test_fetch_summarize_url_flow_raises_when_url_is_invalid() -> None:
    with pytest.raises(ValueError):
        rss_ingest_flow.fetch_summarize_url_flow.fn("ftp://example.com/a", "config.yaml")


@patch("flows.rss_ingest_flow.summarize_one_sentence_task")
@patch("flows.rss_ingest_flow.summarize_briefing_task")
@patch("flows.rss_ingest_flow.fetch_article_task")
def test_fetch_and_summarize_article_flow_calls_tasks_in_order(
    mock_fetch_article_task: MagicMock,
    mock_summarize_briefing_task: MagicMock,
    mock_summarize_one_sentence_task: MagicMock,
) -> None:
    mock_fetch_article_task.return_value = {
        "id": "aaaa",
        "url": "https://example.com/a",
        "title": "A",
        "metadata": {},
        "content": "本文",
        "raw_html": "<html></html>",
    }
    mock_summarize_briefing_task.return_value = "briefing summary"
    mock_summarize_one_sentence_task.return_value = "one sentence"

    article = rss_ingest_flow.fetch_and_summarize_article_flow.fn(
        article_url="https://example.com/a",
        ollama_connection={"base_url": "http://localhost:11434", "model": "qwen3.5:0.8b"},
        timeout_sec=20,
    )

    assert article["briefing_summary"] == "briefing summary"
    assert article["one_sentence_summary"] == "one sentence"
    mock_fetch_article_task.assert_called_once_with("https://example.com/a")
    assert mock_summarize_briefing_task.call_args.kwargs["timeout_sec"] == 20
    assert mock_summarize_one_sentence_task.call_args.kwargs["timeout_sec"] == 20
