from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parent.parent))
    from flows.rss_ingest_flow import fetch_summarize_url_flow
else:
    from .rss_ingest_flow import fetch_summarize_url_flow


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser for the single URL summarize runner."""
    parser = argparse.ArgumentParser(description="Run fetch-summarize-url-flow.")
    parser.add_argument(
        "--article-url",
        dest="article_url",
        required=True,
        help="Target article URL.",
    )
    parser.add_argument(
        "--config-path",
        dest="config_path",
        default="config.yaml",
        help="Path to config YAML file.",
    )
    return parser


def select_output_fields(article: dict[str, Any]) -> dict[str, Any]:
    """Return only the CLI output fields for the summarized article."""
    return {
        "id": article.get("id"),
        "title": article.get("title"),
        "one_sentence_summary": article.get("one_sentence_summary"),
        "briefing_summary": article.get("briefing_summary"),
    }


def main(argv: list[str] | None = None) -> int:
    """Execute the single URL summarize flow and print the filtered result."""
    args = build_parser().parse_args(argv)
    article = fetch_summarize_url_flow(
        article_url=args.article_url,
        config_path=args.config_path,
    )
    print(json.dumps(select_output_fields(article), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
