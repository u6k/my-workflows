from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from flows import fetch_summarize_url_flow


def test_build_parser_requires_article_url() -> None:
    """Test case: parser requires article url argument."""
    parser = fetch_summarize_url_flow.build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args([])


def test_build_parser_uses_default_config_path() -> None:
    """Test case: parser uses config yaml by default."""
    parser = fetch_summarize_url_flow.build_parser()

    args = parser.parse_args(["--article-url", "https://example.com/article"])

    assert args.article_url == "https://example.com/article"
    assert args.config_path == "config.yaml"


def test_select_output_fields_returns_only_expected_keys() -> None:
    """Test case: cli output excludes article body fields."""
    result = fetch_summarize_url_flow.select_output_fields(
        {
            "id": "aaaa",
            "title": "A",
            "one_sentence_summary": "one sentence",
            "briefing_summary": "briefing",
            "content": "body",
            "raw_html": "<html></html>",
            "metadata": {"k": "v"},
            "url": "https://example.com/article",
        }
    )

    assert result == {
        "id": "aaaa",
        "title": "A",
        "one_sentence_summary": "one sentence",
        "briefing_summary": "briefing",
    }


@patch("flows.fetch_summarize_url_flow.fetch_summarize_url_flow")
def test_main_calls_flow_and_prints_filtered_json(
    mock_fetch_summarize_url_flow: MagicMock,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test case: main prints only summary fields as json."""
    mock_fetch_summarize_url_flow.return_value = {
        "id": "aaaa",
        "title": "A",
        "one_sentence_summary": "one sentence",
        "briefing_summary": "briefing",
        "content": "body",
        "raw_html": "<html></html>",
        "metadata": {"k": "v"},
        "url": "https://example.com/article",
    }

    exit_code = fetch_summarize_url_flow.main(
        ["--article-url", "https://example.com/article", "--config-path", "custom.yaml"]
    )

    assert exit_code == 0
    mock_fetch_summarize_url_flow.assert_called_once_with(
        article_url="https://example.com/article",
        config_path="custom.yaml",
    )
    output = json.loads(capsys.readouterr().out)
    assert output == {
        "id": "aaaa",
        "title": "A",
        "one_sentence_summary": "one sentence",
        "briefing_summary": "briefing",
    }
    assert "content" not in output
    assert "raw_html" not in output
