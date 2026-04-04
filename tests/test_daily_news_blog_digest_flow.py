from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from flows import daily_news_blog_digest_flow


def test_load_daily_digest_config_task_returns_config_when_valid(tmp_path) -> None:
    """Test case: load daily digest config task returns config when valid."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
storage:
  s3_bucket: my-bucket
  s3_prefix: rss
prefect_blocks:
  aws_credentials_block: aws-credentials
  ollama_connection_secret_block: ollama-connection
ollama:
  models:
    daily_news_blog_digest_flow: qwen3.5:0.8b
""".strip(),
        encoding="utf-8",
    )

    config = daily_news_blog_digest_flow.load_daily_digest_config_task.fn(str(config_path))

    assert config["storage"]["s3_bucket"] == "my-bucket"
    assert config["storage"]["s3_prefix"] == "rss"


@patch("flows.daily_news_blog_digest_flow.AwsCredentials")
def test_fetch_daily_articles_from_s3_task_filters_by_target_date(mock_aws_credentials: MagicMock) -> None:
    """Test case: fetch daily articles from s3 task filters by target date."""
    mock_logger = MagicMock()

    mock_s3_client = MagicMock()
    mock_s3_client.get_paginator.return_value.paginate.return_value = [
        {
            "Contents": [
                {
                    "Key": "rss/2026-04-02/aa/20260402-a.json",
                    "LastModified": datetime(2026, 4, 2, 0, 10, tzinfo=timezone.utc),
                },
                {
                    "Key": "rss/2026-04-03/bb/20260403-b.json",
                    "LastModified": datetime(2026, 4, 3, 0, 10, tzinfo=timezone.utc),
                },
            ]
        }
    ]
    mock_s3_client.get_object.return_value = {"Body": MagicMock(read=MagicMock(return_value=b'{"id":"a"}'))}

    mock_aws_credentials.load.return_value.get_boto3_session.return_value.client.return_value = mock_s3_client

    with patch("flows.daily_news_blog_digest_flow._get_task_logger", return_value=mock_logger):
        articles = daily_news_blog_digest_flow.fetch_daily_articles_from_s3_task.fn(
            target_date="2026-04-02",
            storage={"s3_bucket": "news-bucket", "s3_prefix": "rss"},
            aws_credentials_block_name="aws-credentials",
        )

    assert articles == [{"id": "a"}]
    mock_s3_client.get_paginator.return_value.paginate.assert_called_once_with(
        Bucket="news-bucket",
        Prefix="rss",
    )
    mock_logger.info.assert_any_call(
        "target S3 data fetch path: %s",
        "s3://news-bucket/rss/2026-04-02/aa/20260402-a.json",
    )
    mock_logger.info.assert_any_call(
        "matched s3 object: path=%s last_modified=%s data_length=%d",
        "s3://news-bucket/rss/2026-04-02/aa/20260402-a.json",
        "2026-04-02T00:10:00+00:00",
        10,
    )


def test_fetch_daily_articles_from_s3_task_raises_when_target_date_invalid() -> None:
    """Test case: fetch daily articles from s3 task raises when target date invalid."""
    with pytest.raises(ValueError, match="target_date must be in YYYY-MM-DD format"):
        daily_news_blog_digest_flow.fetch_daily_articles_from_s3_task.fn(
            target_date="20260402",
            storage={"s3_bucket": "news-bucket", "s3_prefix": "rss"},
            aws_credentials_block_name="aws-credentials",
        )


@patch("flows.daily_news_blog_digest_flow.AwsCredentials")
def test_fetch_daily_articles_from_s3_task_respects_max_articles(mock_aws_credentials: MagicMock) -> None:
    """Test case: fetch daily articles from s3 task respects max articles."""
    mock_s3_client = MagicMock()
    mock_s3_client.get_paginator.return_value.paginate.return_value = [
        {
            "Contents": [
                {
                    "Key": "rss/2026-04-02/aa/20260402-a.json",
                    "LastModified": datetime(2026, 4, 2, 0, 10, tzinfo=timezone.utc),
                },
                {
                    "Key": "rss/2026-04-02/bb/20260402-b.json",
                    "LastModified": datetime(2026, 4, 2, 0, 20, tzinfo=timezone.utc),
                },
            ]
        }
    ]
    mock_s3_client.get_object.side_effect = [
        {"Body": MagicMock(read=MagicMock(return_value=b'{"id":"a"}'))},
        {"Body": MagicMock(read=MagicMock(return_value=b'{"id":"b"}'))},
    ]
    mock_aws_credentials.load.return_value.get_boto3_session.return_value.client.return_value = mock_s3_client

    articles = daily_news_blog_digest_flow.fetch_daily_articles_from_s3_task.fn(
        target_date="2026-04-02",
        storage={"s3_bucket": "news-bucket", "s3_prefix": "rss"},
        aws_credentials_block_name="aws-credentials",
        max_articles=1,
    )

    assert articles == [{"id": "a"}]
    assert mock_s3_client.get_object.call_count == 1


def test_resolve_target_date_uses_config_value_when_flow_param_is_none() -> None:
    """Test case: resolve target date uses config value when flow param is none."""
    resolved = daily_news_blog_digest_flow._resolve_target_date(
        target_date=None,
        config={"target_date": "2026-04-02"},
    )

    assert resolved == "2026-04-02"


def test_resolve_target_date_defaults_to_today_when_config_missing() -> None:
    """Test case: resolve target date defaults to today when config missing."""
    expected = datetime.now(timezone.utc).date().isoformat()

    resolved = daily_news_blog_digest_flow._resolve_target_date(
        target_date=None,
        config={},
    )

    assert resolved == expected


def test_validate_daily_digest_config_raises_when_ollama_secret_block_missing() -> None:
    """Test case: validate daily digest config raises when ollama secret block missing."""
    with pytest.raises(
        ValueError,
        match="config.prefect_blocks.ollama_connection_secret_block must be a non-empty string",
    ):
        daily_news_blog_digest_flow._validate_daily_digest_config(
            {
                "storage": {"s3_bucket": "bucket", "s3_prefix": "rss"},
                "prefect_blocks": {"aws_credentials_block": "aws-credentials"},
            }
        )


def test_validate_daily_digest_config_raises_when_max_articles_invalid() -> None:
    """Test case: validate daily digest config raises when max articles invalid."""
    with pytest.raises(ValueError, match="config.max_articles must be a positive integer when provided"):
        daily_news_blog_digest_flow._validate_daily_digest_config(
            {
                "max_articles": 0,
                "storage": {"s3_bucket": "bucket", "s3_prefix": "rss"},
                "prefect_blocks": {
                    "aws_credentials_block": "aws-credentials",
                    "ollama_connection_secret_block": "ollama-secret",
                },
                "ollama": {"models": {"daily_news_blog_digest_flow": "llama3.1:8b"}},
            }
        )


@patch("flows.daily_news_blog_digest_flow.invoke_ollama_generate")
def test_design_macro_themes_with_ollama_task_returns_python_object(mock_invoke_ollama_generate: MagicMock) -> None:
    """Test case: design macro themes with ollama task returns python object."""
    mock_invoke_ollama_generate.return_value = (
        '{"taxonomy_summary":"summary","themes":[],"unclassifiable_rule":"rule"}'
    )
    mock_logger = MagicMock()
    with patch("flows.daily_news_blog_digest_flow._get_task_logger", return_value=mock_logger):
        result = daily_news_blog_digest_flow.design_macro_themes_with_ollama_task.fn(
            articles=[
                {"title": "A", "one_sentence_summary": "A summary", "content": "ignored"},
                {"title": "B", "one_sentence_summary": "B summary"},
            ],
            ollama_connection={"base_url": "http://localhost:11434", "model": "llama3.1:8b"},
            timeout_sec=60,
        )

    assert result == {
        "taxonomy_summary": "summary",
        "themes": [],
        "unclassifiable_rule": "rule",
    }
    assert mock_invoke_ollama_generate.call_args.kwargs["logger"] is mock_logger
    mock_logger.info.assert_any_call(
        "design_macro_themes_with_ollama_task result: %s",
        {"taxonomy_summary": "summary", "themes": [], "unclassifiable_rule": "rule"},
    )


@patch("flows.daily_news_blog_digest_flow.invoke_ollama_generate")
def test_assign_articles_to_themes_with_ollama_task_returns_assignments(mock_invoke_ollama_generate: MagicMock) -> None:
    """Test case: assign articles to themes with ollama task returns assignments."""
    mock_invoke_ollama_generate.return_value = """
{
  "assignments": [
    {"article_index": 0, "theme_id": "T01", "confidence": 0.95, "reason": "policy news"},
    {"article_index": 1, "theme_id": "UNCLASSIFIABLE", "confidence": 0.40, "reason": "edge case"}
  ]
}
""".strip()

    result = daily_news_blog_digest_flow.assign_articles_to_themes_with_ollama_task.fn(
        articles=[
            {"id": "a", "title": "A", "one_sentence_summary": "A summary"},
            {"id": "b", "title": "B", "one_sentence_summary": "B summary"},
        ],
        taxonomy={
            "themes": [
                {"theme_id": "T01", "theme_name": "Politics"},
                {"theme_id": "T02", "theme_name": "Markets"},
            ]
        },
        ollama_connection={"base_url": "http://localhost:11434", "model": "llama3.1:8b"},
        timeout_sec=60,
    )

    assert result == [
        {"article_index": 0, "theme_id": "T01", "confidence": 0.95, "reason": "policy news"},
        {"article_index": 1, "theme_id": "UNCLASSIFIABLE", "confidence": 0.40, "reason": "edge case"},
    ]


@patch("flows.daily_news_blog_digest_flow.invoke_ollama_generate")
def test_assign_articles_to_themes_with_ollama_task_raises_when_unknown_theme_id(
    mock_invoke_ollama_generate: MagicMock,
) -> None:
    """Test case: assign articles to themes with ollama task raises when unknown theme id."""
    mock_invoke_ollama_generate.return_value = """
{
  "assignments": [
    {"article_index": 0, "theme_id": "T99", "confidence": 0.8, "reason": "unknown"}
  ]
}
""".strip()

    with pytest.raises(ValueError, match="assignment.theme_id is not in taxonomy"):
        daily_news_blog_digest_flow.assign_articles_to_themes_with_ollama_task.fn(
            articles=[{"id": "a", "title": "A", "one_sentence_summary": "A summary"}],
            taxonomy={"themes": [{"theme_id": "T01"}]},
            ollama_connection={"base_url": "http://localhost:11434", "model": "llama3.1:8b"},
            timeout_sec=60,
        )
