from __future__ import annotations

import json
import sqlite3
from unittest.mock import MagicMock, patch

import pytest

from flows import daily_news_blog_digest_flow


def test_parse_target_date_raises_when_invalid() -> None:
    with pytest.raises(ValueError, match="target_date must be in YYYY-MM-DD format"):
        daily_news_blog_digest_flow._parse_target_date("20260402")


def test_load_daily_articles_from_sqlite_task_filters_by_time_range_and_orders(tmp_path) -> None:
    db_path = tmp_path / "rss_embeddings.sqlite3"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE article_embeddings (
                article_id TEXT PRIMARY KEY,
                article_url TEXT NOT NULL,
                title TEXT,
                published_timestamp TEXT,
                fetch_timestamp TEXT,
                briefing_summary TEXT,
                one_sentence_summary TEXT,
                metadata_json TEXT,
                embedding_json TEXT NOT NULL,
                embedding_timestamp TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO article_embeddings VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "a",
                "https://example.com/a",
                "A",
                "2026-04-02T00:00:00+00:00",
                "2026-04-02T01:00:00+00:00",
                "brief A",
                "one A",
                "{}",
                json.dumps([0.1, 0.2]),
                "2026-04-02T01:00:01+00:00",
            ),
        )
        conn.execute(
            """
            INSERT INTO article_embeddings VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "b",
                "https://example.com/b",
                "B",
                "2026-04-02T00:00:00+00:00",
                "2026-04-02T23:00:00+00:00",
                "brief B",
                "one B",
                "{}",
                json.dumps([0.3, 0.4]),
                "2026-04-02T23:00:01+00:00",
            ),
        )
        conn.execute(
            """
            INSERT INTO article_embeddings VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "c",
                "https://example.com/c",
                "C",
                "2026-04-03T00:00:00+00:00",
                "2026-04-03T01:00:00+00:00",
                "brief C",
                "one C",
                "{}",
                json.dumps([0.9, 0.8]),
                "2026-04-03T01:00:01+00:00",
            ),
        )
        conn.commit()

    result = daily_news_blog_digest_flow.load_daily_articles_from_sqlite_task.fn(
        target_date="2026-04-02",
        sqlite_path=str(db_path),
    )

    assert [row["id"] for row in result] == ["b", "a"]


def test_build_category_clusters_task_builds_6_to_12_when_articles_are_enough() -> None:
    articles = [
        {"id": f"a{i}", "title": f"A{i}", "url": f"https://example.com/{i}", "embedding": [1.0, 0.0]}
        for i in range(8)
    ] + [
        {"id": f"b{i}", "title": f"B{i}", "url": f"https://example.com/b{i}", "embedding": [0.0, 1.0]}
        for i in range(8)
    ]

    categories = daily_news_blog_digest_flow.build_category_clusters_task.fn(
        articles=articles,
        min_categories=6,
        max_categories=12,
        max_iterations=3,
    )

    assert 2 <= len(categories) <= 12
    assert sum(category["article_count"] for category in categories) == len(articles)


def test_compose_blog_style_digest_task_uses_category_payload() -> None:
    payload = {
        "categories": [
            {
                "category_id": "C01",
                "category_name": "AI",
                "category_summary": "AI summary",
                "key_points": ["p1", "p2"],
                "article_count": 1,
                "articles": [{"title": "A", "url": "https://example.com/a"}],
            }
        ]
    }

    markdown = daily_news_blog_digest_flow.compose_blog_style_digest_task.fn(
        target_date="2026-04-02",
        category_payload=payload,
    )

    assert "# Daily News Digest (2026-04-02)" in markdown
    assert "## AI" in markdown
    assert "https://example.com/a" in markdown


@patch("flows.daily_news_blog_digest_flow.AwsCredentials")
def test_load_categories_from_s3_task_returns_none_when_not_found(mock_aws_credentials: MagicMock) -> None:
    mock_s3 = MagicMock()
    not_found = Exception("not found")
    not_found.response = {"Error": {"Code": "404"}}
    mock_s3.head_object.side_effect = not_found
    mock_aws_credentials.load.return_value.get_boto3_session.return_value.client.return_value = mock_s3

    result = daily_news_blog_digest_flow.load_categories_from_s3_task.fn(
        target_date="2026-04-02",
        storage={"s3_bucket": "news", "s3_prefix": "rss"},
        aws_credentials_block_name="aws-credentials",
    )
    assert result is None


def test_resolve_daily_digest_storage_defaults_to_storage_prefix() -> None:
    resolved = daily_news_blog_digest_flow._resolve_daily_digest_storage(
        {"s3_bucket": "news", "s3_prefix": "rss"}
    )
    assert resolved == {"s3_bucket": "news", "s3_prefix": "rss"}


def test_resolve_daily_digest_storage_uses_override_when_provided() -> None:
    resolved = daily_news_blog_digest_flow._resolve_daily_digest_storage(
        {
            "s3_bucket": "news",
            "s3_prefix": "rss",
            "daily_digest_s3_bucket": "digest-bucket",
            "daily_digest_s3_prefix": "digest-prefix",
        }
    )
    assert resolved == {"s3_bucket": "digest-bucket", "s3_prefix": "digest-prefix"}
