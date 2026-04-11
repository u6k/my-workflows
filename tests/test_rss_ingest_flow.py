from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from flows import rss_ingest_flow


VALID_CONFIG_YAML = """
retry:
  max_retries: 3
  initial_delay_sec: 2
  backoff_multiplier: 2
prefect_blocks:
  aws_credentials_block: aws-credentials-prod
  llm_connection_block: llm-connection
chroma:
  host: 127.0.0.1
  port: 8000
  ssl: false
  collection_name: rss-articles
ollama:
  request_timeout_sec: 120
rss_ingest:
  rss_urls:
    - https://example.com/rss.xml
  s3_bucket: news-bucket
  s3_prefix: rss
  llm_model: qwen3.5:0.8b
"""


INVALID_EMPTY_RSS_CONFIG_YAML = """
retry:
  max_retries: 3
prefect_blocks:
  aws_credentials_block: aws-credentials-prod
  llm_connection_block: llm-connection
chroma:
  host: 127.0.0.1
  port: 8000
  ssl: false
  collection_name: rss-articles
ollama:
  request_timeout_sec: 120
rss_ingest:
  rss_urls: []
  s3_bucket: news-bucket
  s3_prefix: rss
  llm_model: qwen3.5:0.8b
"""


class FakeCollection:
    def __init__(self) -> None:
        self.records: dict[str, dict[str, object]] = {}

    def upsert(self, ids, embeddings=None, documents=None, metadatas=None):
        for index, item_id in enumerate(ids):
            self.records[item_id] = {
                "embedding": embeddings[index] if embeddings is not None else None,
                "document": documents[index] if documents is not None else None,
                "metadata": metadatas[index] if metadatas is not None else None,
            }

    def get(self, ids=None, where=None, include=None):
        if ids is not None:
            found_ids = [item_id for item_id in ids if item_id in self.records]
            return {"ids": found_ids}

        matched = []
        for item_id, payload in self.records.items():
            metadata = payload["metadata"] if isinstance(payload, dict) else None
            if not isinstance(metadata, dict):
                continue
            epoch = metadata.get("fetch_timestamp_epoch")
            if isinstance(where, dict):
                predicates = where.get("$and", [])
                okay = True
                for predicate in predicates:
                    condition = predicate.get("fetch_timestamp_epoch")
                    if not isinstance(condition, dict):
                        continue
                    if "$gte" in condition and not (epoch >= condition["$gte"]):
                        okay = False
                    if "$lt" in condition and not (epoch < condition["$lt"]):
                        okay = False
                if not okay:
                    continue
            matched.append((item_id, payload))
        return {
            "ids": [item_id for item_id, _ in matched],
            "metadatas": [payload["metadata"] for _, payload in matched],
            "embeddings": [payload["embedding"] for _, payload in matched],
            "documents": [payload["document"] for _, payload in matched],
        }


def test_load_config_task_returns_config_when_valid(tmp_path) -> None:
    """Test case: load config task returns config when valid."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(VALID_CONFIG_YAML, encoding="utf-8")

    config = rss_ingest_flow.load_config_task.fn(str(config_path))

    assert config["rss_ingest"]["rss_urls"] == ["https://example.com/rss.xml"]
    assert config["retry"]["max_retries"] == 3
    assert config["rss_ingest"]["s3_prefix"] == "rss"
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
retry:
  max_retries: 3
ollama:
  request_timeout_sec: 0
prefect_blocks:
  aws_credentials_block: aws-credentials-prod
  llm_connection_block: llm-connection
chroma:
  host: 127.0.0.1
  port: 8000
  ssl: false
  collection_name: rss-articles
rss_ingest:
  rss_urls:
    - https://example.com/rss.xml
  s3_bucket: news-bucket
  s3_prefix: rss
  llm_model: qwen3.5:0.8b
""",
        encoding="utf-8",
    )

    with pytest.raises(ValueError):
        rss_ingest_flow.load_config_task.fn(str(config_path))


def test_load_config_task_raises_when_chroma_host_is_missing(tmp_path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        VALID_CONFIG_YAML.replace("host: 127.0.0.1", "host: ''"),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="config.chroma.host"):
        rss_ingest_flow.load_config_task.fn(str(config_path))


def test_get_embeddings_collection_uses_http_client_configuration() -> None:
    mock_collection = MagicMock()
    mock_client = MagicMock()
    mock_client.get_or_create_collection.return_value = mock_collection

    with patch("chromadb.HttpClient", return_value=mock_client) as mock_http_client:
        result = rss_ingest_flow._get_embeddings_collection(
            {"host": "127.0.0.1", "port": 8000, "ssl": False, "collection_name": "rss-articles"}
        )

    assert result is mock_collection
    mock_http_client.assert_called_once_with(host="127.0.0.1", port=8000, ssl=False)
    mock_client.get_or_create_collection.assert_called_once_with(name="rss-articles")


@patch("flows.rss_ingest_flow.get_run_logger")
@patch("flows.rss_ingest_flow.load_llm_connection_secret")
@patch("flows.rss_ingest_flow.AwsCredentials")
def test_validate_prerequisites_task_loads_blocks_and_json_secret(
    mock_aws_credentials: MagicMock,
    mock_load_llm_connection_secret: MagicMock,
    mock_get_run_logger: MagicMock,
) -> None:
    mock_get_run_logger.return_value = MagicMock()
    mock_load_llm_connection_secret.return_value = {"base_url": "http://localhost:11434", "model": "llama3.1:8b"}

    parsed_config = {
        "rss_ingest": {"rss_urls": ["https://example.com/rss.xml", "https://example.com/rss.xml"]},
        "retry": {"max_retries": 3},
        "prefect_blocks": {
            "aws_credentials_block": "aws-credentials-prod",
            "llm_connection_block": "llm-connection",
        },
    }

    rss_ingest_flow.validate_prerequisites_task.fn(parsed_config)

    mock_aws_credentials.load.assert_called_once_with("aws-credentials-prod")
    mock_load_llm_connection_secret.assert_called_once()


@patch("flows.rss_ingest_flow.get_run_logger")
@patch("flows.rss_ingest_flow.load_llm_connection_secret")
@patch("flows.rss_ingest_flow.AwsCredentials")
def test_validate_prerequisites_task_raises_when_secret_value_is_not_dict(
    mock_aws_credentials: MagicMock,
    mock_load_llm_connection_secret: MagicMock,
    mock_get_run_logger: MagicMock,
) -> None:
    mock_get_run_logger.return_value = MagicMock()
    mock_load_llm_connection_secret.side_effect = ValueError("Ollama Secret block value must be a dict")

    parsed_config = {
        "rss_ingest": {"rss_urls": ["https://example.com/rss.xml"]},
        "retry": {"max_retries": 3},
        "prefect_blocks": {
            "aws_credentials_block": "aws-credentials-prod",
            "llm_connection_block": "llm-connection",
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
        "rss feed fetched: feed_url=%s link_count=%d",
        "https://example.com/rss.xml",
        3,
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
        "rss feed fetched: feed_url=%s link_count=%d",
        "https://example.com/empty.xml",
        0,
    )


@patch("flows.rss_ingest_flow.trafilatura.extract")
def test_extract_article_content_and_metadata(mock_trafilatura_extract: MagicMock) -> None:
    """Test case: extract article content and metadata."""
    mock_trafilatura_extract.return_value = "# Heading\n\nParagraph A."

    extracted = rss_ingest_flow._extract_article_content_and_metadata(ARTICLE_HTML.decode("utf-8"))

    assert extracted["title"] == "Example title"
    assert extracted["content"] == "# Heading\n\nParagraph A."
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


def test_summarize_briefing_task_returns_llm_response() -> None:
    """Test case: summarize briefing task returns llm response."""
    mock_logger = MagicMock()

    with (
        patch("flows.rss_ingest_flow._get_task_logger", return_value=mock_logger),
        patch("flows.rss_ingest_flow.invoke_llm_generate", return_value="summary text") as mock_invoke_llm_generate,
    ):
        summary = rss_ingest_flow.summarize_briefing_task.fn(
            article_content="本文",
            llm_connection={"base_url": "http://localhost:11434", "model": "llama3.1:8b"},
            timeout_sec=45,
        )

    assert summary == "summary text"
    assert mock_logger.debug.call_count == 2
    assert mock_invoke_llm_generate.call_args.kwargs["timeout_sec"] == 45
    assert mock_invoke_llm_generate.call_args.kwargs["llm_connection"]["model"] == "llama3.1:8b"
    assert mock_invoke_llm_generate.call_args.kwargs["logger"] is mock_logger


def test_summarize_one_sentence_task_returns_llm_response() -> None:
    """Test case: summarize one sentence task returns llm response."""
    mock_logger = MagicMock()

    with (
        patch("flows.rss_ingest_flow._get_task_logger", return_value=mock_logger),
        patch("flows.rss_ingest_flow.invoke_llm_generate", return_value="summary text") as mock_invoke_llm_generate,
    ):
        summary = rss_ingest_flow.summarize_one_sentence_task.fn(
            article_content="本文",
            llm_connection={"base_url": "http://localhost:11434", "model": "llama3.1:8b"},
            timeout_sec=30,
        )

    assert summary == "summary text"
    assert mock_logger.debug.call_count == 2
    assert mock_invoke_llm_generate.call_args.kwargs["timeout_sec"] == 30
    assert mock_invoke_llm_generate.call_args.kwargs["llm_connection"]["model"] == "llama3.1:8b"
    assert mock_invoke_llm_generate.call_args.kwargs["logger"] is mock_logger


def test_upsert_briefing_embedding_to_chroma_task_stores_document_and_metadata() -> None:
    """Test case: chroma upsert stores document and metadata and lets Chroma build embeddings."""
    collection = FakeCollection()

    with patch("flows.rss_ingest_flow._get_embeddings_collection", return_value=collection):
        rss_ingest_flow.upsert_briefing_embedding_to_chroma_task.fn(
            article={
                "id": "article-1",
                "url": "https://example.com/a",
                "title": "Article title",
                "briefing_summary": "summary text",
                "one_sentence_summary": "one sentence",
                "fetch_timestamp": "2026-04-04T01:00:00+00:00",
                "metadata": {"source_feed_url": "https://example.com/rss.xml"},
            },
            chroma_config={"host": "127.0.0.1", "port": 8000, "ssl": False, "collection_name": "rss-articles"},
        )
    stored = collection.records["article-1"]
    assert stored["embedding"] is None
    assert stored["document"] == "summary text"
    assert stored["metadata"]["article_url"] == "https://example.com/a"
    assert stored["metadata"]["title"] == "Article title"
    assert stored["metadata"]["fetch_timestamp"] == "2026-04-04T01:00:00+00:00"
    assert stored["metadata"]["one_sentence_summary"] == "one sentence"
    assert stored["metadata"]["metadata_json"] == '{"source_feed_url": "https://example.com/rss.xml"}'
    assert isinstance(stored["metadata"]["embedding_timestamp"], str) and stored["metadata"]["embedding_timestamp"] != ""


@patch("flows.rss_ingest_flow.trafilatura.extract")
@patch("flows.rss_ingest_flow.urlopen")
def test_fetch_article_task_returns_content_and_metadata(
    mock_urlopen: MagicMock,
    mock_trafilatura_extract: MagicMock,
) -> None:
    mock_trafilatura_extract.return_value = "# Heading\n\nParagraph A."
    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.read.return_value = ARTICLE_HTML
    mock_response.headers.get_content_charset.return_value = "utf-8"
    mock_urlopen.return_value.__enter__.return_value = mock_response

    article = rss_ingest_flow.fetch_article_task.fn("https://example.com/posts/1")

    assert article["id"] == "de0617c481337158695d4e48d5c275d2"
    assert article["url"] == "https://example.com/posts/1"
    assert article["title"] == "Example title"
    assert article["content"] == "# Heading\n\nParagraph A."
    assert "<html>" in article["raw_html"]
    assert article["metadata"]["author"] == "Jane Doe"
    assert article["metadata"]["tags"] == ["ai", "python", "tech"]


def test_is_youtube_url_supports_common_hosts() -> None:
    assert rss_ingest_flow._is_youtube_url("https://www.youtube.com/watch?v=abc123") is True
    assert rss_ingest_flow._is_youtube_url("https://youtu.be/abc123") is True
    assert rss_ingest_flow._is_youtube_url("https://example.com/watch?v=abc123") is False


def test_extract_title_from_markdown_text_prefers_heading() -> None:
    title = rss_ingest_flow._extract_title_from_markdown_text("\n# Video title\n\n本文")
    assert title == "Video title"


def test_extract_transcript_section_returns_transcript_body() -> None:
    transcript = rss_ingest_flow._extract_transcript_section(
        "# YouTube\n\n## Video title\n\n### Description\n概要\n\n### Transcript\n字幕本文です\n\n### Video Metadata\nx"
    )
    assert transcript == "字幕本文です"


@patch("flows.rss_ingest_flow._fetch_youtube_transcript_with_markitdown")
def test_fetch_article_task_uses_markitdown_for_youtube_urls(
    mock_fetch_youtube_transcript_with_markitdown: MagicMock,
) -> None:
    mock_fetch_youtube_transcript_with_markitdown.return_value = {
        "title": "Video title",
        "content": "# YouTube\n\n## Video title\n\n### Transcript\nTranscript body",
        "metadata": {
            "source_type": "youtube_transcript",
            "transcript_provider": "markitdown",
            "transcript_language_priority": ["ja", "en"],
        },
        "raw_html": "",
    }

    article = rss_ingest_flow.fetch_article_task.fn("https://www.youtube.com/watch?v=abc123")

    assert article["url"] == "https://www.youtube.com/watch?v=abc123"
    assert article["title"] == "Video title"
    assert "### Transcript\nTranscript body" in article["content"]
    assert article["raw_html"] == ""
    assert article["metadata"]["source_type"] == "youtube_transcript"
    mock_fetch_youtube_transcript_with_markitdown.assert_called_once_with("https://www.youtube.com/watch?v=abc123")


def test_fetch_youtube_transcript_with_markitdown_uses_language_priority() -> None:
    mock_markitdown = MagicMock()
    mock_markitdown.return_value.convert.return_value.text_content = (
        "# YouTube\n\n## Video title\n\n### Transcript\nTranscript body"
    )
    fake_module = MagicMock()
    fake_module.MarkItDown = mock_markitdown

    with patch.dict("sys.modules", {"markitdown": fake_module}):
        article = rss_ingest_flow._fetch_youtube_transcript_with_markitdown("https://youtu.be/abc123")

    assert article["title"] == "Video title"
    assert "### Transcript\nTranscript body" in article["content"]
    assert article["metadata"]["transcript_provider"] == "markitdown"
    assert article["metadata"]["transcript_language_priority"] == ["ja", "en"]
    mock_markitdown.return_value.convert.assert_called_once_with(
        "https://youtu.be/abc123",
        youtube_transcript_languages=["ja", "en"],
    )


def test_fetch_youtube_transcript_with_markitdown_raises_when_transcript_missing() -> None:
    mock_markitdown = MagicMock()
    mock_markitdown.return_value.convert.return_value.text_content = (
        "# YouTube\n\n## Video title\n\n### Description\nSummary only"
    )
    fake_module = MagicMock()
    fake_module.MarkItDown = mock_markitdown

    with patch.dict("sys.modules", {"markitdown": fake_module}):
        with pytest.raises(ValueError, match="YouTube transcript section"):
            rss_ingest_flow._fetch_youtube_transcript_with_markitdown("https://youtu.be/abc123")


def test_fetch_youtube_transcript_with_markitdown_raises_when_markitdown_is_missing() -> None:
    original_import = __import__

    def side_effect(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "markitdown":
            raise ImportError("markitdown is not installed")
        return original_import(name, globals, locals, fromlist, level)

    with patch("builtins.__import__", side_effect=side_effect):
        with pytest.raises(ValueError, match="markitdown Python package"):
            rss_ingest_flow._fetch_youtube_transcript_with_markitdown("https://youtu.be/abc123")


@patch("flows.rss_ingest_flow.trafilatura.extract")
def test_extract_article_content_and_metadata_falls_back_when_trafilatura_returns_none(
    mock_trafilatura_extract: MagicMock,
) -> None:
    mock_trafilatura_extract.return_value = None

    extracted = rss_ingest_flow._extract_article_content_and_metadata(ARTICLE_HTML.decode("utf-8"))

    assert extracted["content"] == "Heading\n\nParagraph A\\.\n\nParagraph B\\."
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


def test_check_s3_object_exists_task_returns_true_when_embedding_record_exists() -> None:
    """Test case: check s3 object exists task returns true when embedding record exists."""
    collection = FakeCollection()
    collection.upsert(
        ids=["de0617c481337158695d4e48d5c275d2"],
        embeddings=[[0.1, 0.2]],
        documents=["summary"],
        metadatas=[{"fetch_timestamp": "2026-04-04T00:00:00+00:00", "fetch_timestamp_epoch": 1775260800}],
    )
    with patch("flows.rss_ingest_flow._get_embeddings_collection", return_value=collection):
        result = rss_ingest_flow.check_s3_object_exists_task.fn(
            article_url="https://example.com/posts/1",
            chroma_config={"host": "127.0.0.1", "port": 8000, "ssl": False, "collection_name": "rss-articles"},
        )

    assert result["exists"] is True
    assert result["id"] == "de0617c481337158695d4e48d5c275d2"


def test_check_s3_object_exists_task_returns_false_when_embedding_record_not_found() -> None:
    """Test case: check s3 object exists task returns false when embedding record not found."""
    with patch("flows.rss_ingest_flow._get_embeddings_collection", return_value=FakeCollection()):
        result = rss_ingest_flow.check_s3_object_exists_task.fn(
            article_url="https://example.com/posts/1",
            chroma_config={"host": "127.0.0.1", "port": 8000, "ssl": False, "collection_name": "rss-articles"},
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
@patch("flows.rss_ingest_flow._get_embeddings_collection")
@patch("flows.rss_ingest_flow.load_llm_connection_secret")
@patch("flows.rss_ingest_flow.load_config_task")
def test_rss_ingest_flow_continues_when_article_fetch_fails(
    mock_load_config_task: MagicMock,
    mock_load_llm_connection_secret: MagicMock,
    mock_get_embeddings_collection: MagicMock,
    mock_validate_prerequisites_task: MagicMock,
    mock_fetch_feed_task: MagicMock,
    mock_fetch_and_summarize_article_flow: MagicMock,
    mock_store_to_s3_task: MagicMock,
    mock_check_embedding_record_exists_task: MagicMock,
) -> None:
    mock_load_config_task.return_value = {
        "chroma": {"host": "127.0.0.1", "port": 8000, "ssl": False, "collection_name": "rss-articles"},
        "rss_ingest": {
            "rss_urls": ["https://example.com/rss.xml"],
            "s3_bucket": "news-bucket",
            "s3_prefix": "rss",
            "llm_model": "qwen3.5:0.8b",
        },
        "retry": {"max_retries": 3},
        "ollama": {"request_timeout_sec": 120},
        "prefect_blocks": {
            "aws_credentials_block": "aws-credentials-prod",
            "llm_connection_block": "llm-connection",
        },
    }
    mock_load_llm_connection_secret.return_value = {"base_url": "http://localhost:11434", "model": "llama3.1:8b"}
    mock_get_embeddings_collection.return_value = FakeCollection()
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

    mock_logger.info.assert_any_call("all rss feeds fetched: link_count=%d", 2)
    mock_logger.info.assert_any_call("deduplicated links prepared: link_count=%d", 2)
    mock_logger.info.assert_any_call("page summarization started: progress=%d/%d url=%s", 1, 2, "https://example.com/a")
    mock_logger.info.assert_any_call("page summarization started: progress=%d/%d url=%s", 2, 2, "https://example.com/b")
    skip_call = next(
        call for call in mock_logger.info.call_args_list if call.args[0] == "page summarization skipped: url=%s reason=%s"
    )
    assert skip_call.args[1] == "https://example.com/a"
    assert str(skip_call.args[2]) == "unexpected status code: 500"


@patch("flows.rss_ingest_flow.check_embedding_record_exists_task")
@patch("flows.rss_ingest_flow.store_to_s3_task")
@patch("flows.rss_ingest_flow.fetch_and_summarize_article_flow")
@patch("flows.rss_ingest_flow.fetch_feed_task")
@patch("flows.rss_ingest_flow.validate_prerequisites_task")
@patch("flows.rss_ingest_flow._get_embeddings_collection")
@patch("flows.rss_ingest_flow.load_llm_connection_secret")
@patch("flows.rss_ingest_flow.load_config_task")
def test_rss_ingest_flow_continues_when_article_fetch_raises_unexpected_exception(
    mock_load_config_task: MagicMock,
    mock_load_llm_connection_secret: MagicMock,
    mock_get_embeddings_collection: MagicMock,
    mock_validate_prerequisites_task: MagicMock,
    mock_fetch_feed_task: MagicMock,
    mock_fetch_and_summarize_article_flow: MagicMock,
    mock_store_to_s3_task: MagicMock,
    mock_check_embedding_record_exists_task: MagicMock,
) -> None:
    mock_load_config_task.return_value = {
        "chroma": {"host": "127.0.0.1", "port": 8000, "ssl": False, "collection_name": "rss-articles"},
        "rss_ingest": {
            "rss_urls": ["https://example.com/rss.xml"],
            "s3_bucket": "news-bucket",
            "s3_prefix": "rss",
            "llm_model": "qwen3.5:0.8b",
        },
        "retry": {"max_retries": 3},
        "ollama": {"request_timeout_sec": 120},
        "prefect_blocks": {
            "aws_credentials_block": "aws-credentials-prod",
            "llm_connection_block": "llm-connection",
        },
    }
    mock_load_llm_connection_secret.return_value = {"base_url": "http://localhost:11434", "model": "llama3.1:8b"}
    mock_get_embeddings_collection.return_value = FakeCollection()
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

    mock_logger.info.assert_any_call("all rss feeds fetched: link_count=%d", 2)
    mock_logger.info.assert_any_call("deduplicated links prepared: link_count=%d", 2)
    skip_call = next(
        call for call in mock_logger.info.call_args_list if call.args[0] == "page summarization skipped: url=%s reason=%s"
    )
    assert skip_call.args[1] == "https://example.com/a"
    assert str(skip_call.args[2]) == "boom"


@patch("flows.rss_ingest_flow.check_embedding_record_exists_task")
@patch("flows.rss_ingest_flow.store_to_s3_task")
@patch("flows.rss_ingest_flow.fetch_and_summarize_article_flow")
@patch("flows.rss_ingest_flow.fetch_feed_task")
@patch("flows.rss_ingest_flow.validate_prerequisites_task")
@patch("flows.rss_ingest_flow._get_embeddings_collection")
@patch("flows.rss_ingest_flow.load_llm_connection_secret")
@patch("flows.rss_ingest_flow.load_config_task")
def test_rss_ingest_flow_skips_fetch_when_s3_object_exists(
    mock_load_config_task: MagicMock,
    mock_load_llm_connection_secret: MagicMock,
    mock_get_embeddings_collection: MagicMock,
    mock_validate_prerequisites_task: MagicMock,
    mock_fetch_feed_task: MagicMock,
    mock_fetch_and_summarize_article_flow: MagicMock,
    mock_store_to_s3_task: MagicMock,
    mock_check_embedding_record_exists_task: MagicMock,
) -> None:
    mock_load_config_task.return_value = {
        "chroma": {"host": "127.0.0.1", "port": 8000, "ssl": False, "collection_name": "rss-articles"},
        "rss_ingest": {
            "rss_urls": ["https://example.com/rss.xml"],
            "s3_bucket": "news-bucket",
            "s3_prefix": "rss",
            "llm_model": "qwen3.5:0.8b",
        },
        "retry": {"max_retries": 3},
        "ollama": {"request_timeout_sec": 120},
        "prefect_blocks": {
            "aws_credentials_block": "aws-credentials-prod",
            "llm_connection_block": "llm-connection",
        },
    }
    mock_load_llm_connection_secret.return_value = {"base_url": "http://localhost:11434", "model": "llama3.1:8b"}
    mock_get_embeddings_collection.return_value = FakeCollection()
    mock_fetch_feed_task.return_value = ["https://example.com/a"]
    mock_check_embedding_record_exists_task.return_value = {"exists": True, "id": "aaaa"}

    with patch("flows.rss_ingest_flow._get_task_logger") as mock_get_task_logger:
        mock_logger = MagicMock()
        mock_get_task_logger.return_value = mock_logger
        rss_ingest_flow.rss_ingest_flow.fn("config.yaml")

    mock_fetch_and_summarize_article_flow.assert_not_called()
    mock_store_to_s3_task.assert_not_called()
    mock_logger.info.assert_any_call("all rss feeds fetched: link_count=%d", 1)
    mock_logger.info.assert_any_call("deduplicated links prepared: link_count=%d", 0)


@patch("flows.rss_ingest_flow.check_embedding_record_exists_task")
@patch("flows.rss_ingest_flow.store_to_s3_task")
@patch("flows.rss_ingest_flow.fetch_and_summarize_article_flow")
@patch("flows.rss_ingest_flow.fetch_feed_task")
@patch("flows.rss_ingest_flow.validate_prerequisites_task")
@patch("flows.rss_ingest_flow._get_embeddings_collection")
@patch("flows.rss_ingest_flow.load_llm_connection_secret")
@patch("flows.rss_ingest_flow.load_config_task")
def test_rss_ingest_flow_skips_article_when_summarization_fails(
    mock_load_config_task: MagicMock,
    mock_load_llm_connection_secret: MagicMock,
    mock_get_embeddings_collection: MagicMock,
    mock_validate_prerequisites_task: MagicMock,
    mock_fetch_feed_task: MagicMock,
    mock_fetch_and_summarize_article_flow: MagicMock,
    mock_store_to_s3_task: MagicMock,
    mock_check_embedding_record_exists_task: MagicMock,
) -> None:
    mock_load_config_task.return_value = {
        "chroma": {"host": "127.0.0.1", "port": 8000, "ssl": False, "collection_name": "rss-articles"},
        "rss_ingest": {
            "rss_urls": ["https://example.com/rss.xml"],
            "s3_bucket": "news-bucket",
            "s3_prefix": "rss",
            "llm_model": "qwen3.5:0.8b",
        },
        "retry": {"max_retries": 3},
        "ollama": {"request_timeout_sec": 120},
        "prefect_blocks": {
            "aws_credentials_block": "aws-credentials-prod",
            "llm_connection_block": "llm-connection",
        },
    }
    mock_load_llm_connection_secret.return_value = {"base_url": "http://localhost:11434", "model": "llama3.1:8b"}
    mock_get_embeddings_collection.return_value = FakeCollection()
    mock_fetch_feed_task.return_value = ["https://example.com/a"]
    mock_check_embedding_record_exists_task.return_value = {"exists": False, "id": "aaaa"}
    mock_fetch_and_summarize_article_flow.side_effect = RuntimeError("ollama error")

    with patch("flows.rss_ingest_flow._get_task_logger") as mock_get_task_logger:
        mock_logger = MagicMock()
        mock_get_task_logger.return_value = mock_logger
        rss_ingest_flow.rss_ingest_flow.fn("config.yaml")

    mock_store_to_s3_task.assert_not_called()
    mock_logger.info.assert_any_call("page summarization started: progress=%d/%d url=%s", 1, 1, "https://example.com/a")
    skip_call = next(
        call for call in mock_logger.info.call_args_list if call.args[0] == "page summarization skipped: url=%s reason=%s"
    )
    assert skip_call.args[1] == "https://example.com/a"
    assert str(skip_call.args[2]) == "ollama error"


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
@patch("flows.rss_ingest_flow.load_llm_connection_secret")
@patch("flows.rss_ingest_flow.load_config_task")
def test_fetch_summarize_url_flow_returns_article_with_summaries(
    mock_load_config_task: MagicMock,
    mock_load_llm_connection_secret: MagicMock,
    mock_validate_prerequisites_task: MagicMock,
    mock_fetch_and_summarize_article_flow: MagicMock,
) -> None:
    mock_load_config_task.return_value = {
        "chroma": {"host": "127.0.0.1", "port": 8000, "ssl": False, "collection_name": "rss-articles"},
        "rss_ingest": {
            "rss_urls": ["https://example.com/rss.xml"],
            "s3_bucket": "news-bucket",
            "s3_prefix": "rss",
            "llm_model": "qwen3.5:0.8b",
        },
        "retry": {"max_retries": 3},
        "ollama": {"request_timeout_sec": 45},
        "prefect_blocks": {
            "aws_credentials_block": "aws-credentials-prod",
            "llm_connection_block": "llm-connection",
        },
    }
    mock_load_llm_connection_secret.return_value = {"base_url": "http://localhost:11434"}
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
        llm_connection={"base_url": "http://localhost:11434", "model": "qwen3.5:0.8b"},
        timeout_sec=20,
    )

    assert article["briefing_summary"] == "briefing summary"
    assert article["one_sentence_summary"] == "one sentence"
    mock_fetch_article_task.assert_called_once_with("https://example.com/a")
    assert mock_summarize_briefing_task.call_args.kwargs["timeout_sec"] == 20
    assert mock_summarize_one_sentence_task.call_args.kwargs["timeout_sec"] == 20
