from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from flows import daily_news_blog_digest_flow


def test_parse_target_date_raises_when_invalid() -> None:
    with pytest.raises(ValueError, match="target_date must be in YYYY-MM-DD format"):
        daily_news_blog_digest_flow._parse_target_date("20260402")


def test_validate_daily_digest_config_raises_when_chroma_port_is_invalid() -> None:
    with pytest.raises(ValueError, match="config.chroma.port"):
        daily_news_blog_digest_flow._validate_daily_digest_config(
            {
                "chroma": {"host": "127.0.0.1", "port": 0, "ssl": False, "collection_name": "rss-articles"},
                "llm": {"request_timeout_sec": 120},
                "prefect_blocks": {
                    "aws_credentials_block": "aws-credentials",
                    "llm_connection_block": "llm-connection",
                },
                "daily_news_blog_digest": {
                    "s3_bucket": "news-bucket",
                    "s3_prefix": "daily-digest",
                    "llm_model": "openai/gpt-4o-mini",
                },
            }
        )


def test_load_daily_articles_from_chroma_task_filters_by_time_range_and_orders() -> None:
    collection = MagicMock()
    collection.get.return_value = {
        "ids": ["a", "b"],
        "metadatas": [
            {
                "article_url": "https://example.com/a",
                "title": "A",
                "fetch_timestamp": "2026-04-02T01:00:00+00:00",
                "fetch_timestamp_epoch": 1775091600,
                "one_sentence_summary": "one A",
                "embedding_timestamp": "2026-04-02T01:00:01+00:00",
                "metadata_json": "{}",
            },
            {
                "article_url": "https://example.com/b",
                "title": "B",
                "fetch_timestamp": "2026-04-02T23:00:00+00:00",
                "fetch_timestamp_epoch": 1775170800,
                "one_sentence_summary": "one B",
                "embedding_timestamp": "2026-04-02T23:00:01+00:00",
                "metadata_json": "{}",
            },
        ],
        "embeddings": [[0.1, 0.2], [0.3, 0.4]],
        "documents": ["brief A", "brief B"],
    }

    with patch("flows.daily_news_blog_digest_flow._get_embeddings_collection", return_value=collection):
        result = daily_news_blog_digest_flow.load_daily_articles_from_chroma_task.fn(
            target_date="2026-04-02",
            chroma_config={"host": "127.0.0.1", "port": 8000, "ssl": False, "collection_name": "rss-articles"},
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
