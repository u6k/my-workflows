from __future__ import annotations

import json
import logging
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from prefect import flow, get_run_logger, task
from prefect.exceptions import MissingContextError
from prefect_aws.credentials import AwsCredentials

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parent))
    from common import (
        build_ollama_connection,
        create_s3_client,
        invoke_ollama_generate,
        load_ollama_connection_secret,
        load_yaml_config,
    )
else:
    from .common import (
        build_ollama_connection,
        create_s3_client,
        invoke_ollama_generate,
        load_ollama_connection_secret,
        load_yaml_config,
    )


def _get_task_logger() -> logging.Logger:
    """Prefect実行コンテキストに応じたロガーを返す。

    処理内容:
        実行中のPrefectランコンテキストがある場合は `get_run_logger` を返し、
        無い場合はモジュールロガーを返す。
    入力:
        なし。
    出力:
        logging.Logger: 利用可能なロガー。
    例外:
        なし（`MissingContextError` は内部で吸収）。
    外部依存リソース:
        Prefect実行コンテキスト。
    """
    try:
        return get_run_logger()
    except MissingContextError:
        return logging.getLogger(__name__)


def _load_yaml_config(config_path: str) -> dict[str, Any]:
    """共通ユーティリティ経由でYAML設定を読み込む。

    処理内容:
        `flows.common.load_yaml_config` を呼び出して設定辞書を取得する。
    入力:
        config_path: YAML設定ファイルパス。
    出力:
        dict[str, Any]: 設定辞書。
    例外:
        ValueError: 設定ファイル不備時。
    外部依存リソース:
        ローカルファイルシステム。
    """
    return load_yaml_config(config_path)


def _validate_daily_digest_config(config: dict[str, Any]) -> None:
    """日次ダイジェストフロー設定の妥当性を検証する。

    処理内容:
        storage / prefect_blocks / ollama 配下の必須キー、型、空文字、数値範囲を検証する。
    入力:
        config: 検証対象の設定辞書。
    出力:
        なし。
    例外:
        ValueError: 必須キー不足、型不正、値不正の場合。
    外部依存リソース:
        なし。
    """
    storage = config.get("storage")
    if not isinstance(storage, dict):
        raise ValueError("config.storage must be an object")

    s3_bucket = storage.get("s3_bucket")
    if not isinstance(s3_bucket, str) or not s3_bucket:
        raise ValueError("config.storage.s3_bucket must be a non-empty string")

    s3_prefix = storage.get("s3_prefix")
    if not isinstance(s3_prefix, str):
        raise ValueError("config.storage.s3_prefix must be a string")

    prefect_blocks = config.get("prefect_blocks")
    if not isinstance(prefect_blocks, dict):
        raise ValueError("config.prefect_blocks must be an object")

    aws_credentials_block = prefect_blocks.get("aws_credentials_block")
    if not isinstance(aws_credentials_block, str) or not aws_credentials_block:
        raise ValueError("config.prefect_blocks.aws_credentials_block must be a non-empty string")

    ollama_connection_secret_block = prefect_blocks.get("ollama_connection_secret_block")
    if not isinstance(ollama_connection_secret_block, str) or not ollama_connection_secret_block:
        raise ValueError("config.prefect_blocks.ollama_connection_secret_block must be a non-empty string")

    ollama = config.get("ollama")
    if not isinstance(ollama, dict):
        raise ValueError("config.ollama must be an object")

    request_timeout_sec = ollama.get("request_timeout_sec")
    if request_timeout_sec is not None and (not isinstance(request_timeout_sec, int) or request_timeout_sec <= 0):
        raise ValueError("config.ollama.request_timeout_sec must be a positive integer")

    models = ollama.get("models")
    if not isinstance(models, dict):
        raise ValueError("config.ollama.models must be an object")

    digest_model = models.get("daily_news_blog_digest_flow")
    if not isinstance(digest_model, str) or not digest_model:
        raise ValueError("config.ollama.models.daily_news_blog_digest_flow must be a non-empty string")


def _parse_target_date(target_date: str) -> date:
    """`YYYY-MM-DD` 形式の日付文字列を `date` に変換する。

    処理内容:
        `datetime.strptime` で文字列を日付へ変換し、日付オブジェクトを返す。
    入力:
        target_date: `YYYY-MM-DD` 形式の文字列。
    出力:
        date: 変換された日付。
    例外:
        ValueError: 日付形式が不正な場合。
    外部依存リソース:
        なし。
    """
    try:
        return datetime.strptime(target_date, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError("target_date must be in YYYY-MM-DD format") from exc


def _resolve_target_date(target_date: str | None, config: dict[str, Any]) -> str:
    """実行対象日を引数・設定・現在日付から解決して返す。

    処理内容:
        優先順位を「引数 > config.target_date > UTC当日」として解決し、
        最終的に妥当性検証後のISO日付文字列を返す。
    入力:
        target_date: フロー引数の日付（任意）。
        config: 設定辞書。
    出力:
        str: `YYYY-MM-DD` 形式の日付文字列。
    例外:
        ValueError: 日付が空、型不正、形式不正の場合。
    外部依存リソース:
        システム時刻（UTC現在日付）。
    """
    if target_date is not None:
        resolved_target_date = target_date
    else:
        config_target_date = config.get("target_date")
        if config_target_date is None:
            resolved_target_date = datetime.now(timezone.utc).date().isoformat()
        else:
            resolved_target_date = config_target_date

    if not isinstance(resolved_target_date, str) or not resolved_target_date:
        raise ValueError("target_date must be a non-empty string")

    return _parse_target_date(resolved_target_date).isoformat()


def _create_s3_client(aws_credentials: AwsCredentials) -> Any:
    """共通ユーティリティ経由でS3クライアントを生成する。

    処理内容:
        共通関数 `create_s3_client` を呼び出してクライアントを返す。
    入力:
        aws_credentials: Prefect AwsCredentials ブロック。
    出力:
        Any: boto3 S3クライアント。
    例外:
        ValueError: クライアント設定が不正な場合。
    外部依存リソース:
        Prefectブロック、AWS SDK。
    """
    return create_s3_client(aws_credentials)


def _build_macro_theme_prompt(articles: list[dict[str, str]]) -> str:
    """記事一覧からマクロテーマ設計用プロンプトを構築する。

    処理内容:
        記事配列をJSON整形し、固定テンプレートへ埋め込んでLLM入力文を生成する。
    入力:
        articles: title / one_sentence_summary を含む記事配列。
    出力:
        str: Ollamaへ送るプロンプト文字列。
    例外:
        なし。
    外部依存リソース:
        なし。
    """
    articles_json = json.dumps(articles, ensure_ascii=False, indent=2)
    return f"""あなたはニュース記事のテーマ設計者です。目的は、個別記事を分類するための「マクロテーマ体系」を作ることです。

# 制約
- テーマ数は6〜12個
- テーマ同士はできるだけ重複させない
- 粒度を揃える
- 「AI」や「テクノロジー」のような広すぎるテーマは禁止
- できるだけ実務で使える分類軸にする
- それでも分類不能な記事のために "UNCLASSIFIABLE" を設ける
- 出力はJSONのみ

# 入力データの要素
各記事には以下の要素がある
- title
- one_sentence_summary

# タスク
1. 記事群全体を見て、6〜12個のマクロテーマを提案する
2. 各テーマについて以下を出力する
   - theme_id
   - theme_name
   - description
   - include_rules
   - exclude_rules
   - example_keywords
3. 似ているテーマがある場合は統合する
4. 粒度が揃っていない場合は修正する
5. 最後に、全体を100字以内で説明する

# 出力JSONスキーマ
{{
  "taxonomy_summary": "string",
  "themes": [
    {{
      "theme_id": "T01",
      "theme_name": "string",
      "description": "string",
      "include_rules": ["string"],
      "exclude_rules": ["string"],
      "example_keywords": ["string"]
    }}
  ],
  "unclassifiable_rule": "string"
}}

# 入力データ
{articles_json}
"""


@task(name="load_daily_digest_config_task")
def load_daily_digest_config_task(config_path: str = "config.yaml") -> dict[str, Any]:
    """日次ダイジェスト設定を読み込み、検証済みで返す。

    処理内容:
        YAML設定を読み込み `_validate_daily_digest_config` で妥当性検証する。
    入力:
        config_path: 設定ファイルパス。
    出力:
        dict[str, Any]: 検証済み設定辞書。
    例外:
        ValueError: 設定不正時。
    外部依存リソース:
        ローカルファイルシステム。
    """
    config = _load_yaml_config(config_path)
    _validate_daily_digest_config(config)
    return config


@task(name="fetch_daily_articles_from_s3_task")
def fetch_daily_articles_from_s3_task(
    target_date: str,
    storage: dict[str, Any],
    aws_credentials_block_name: str,
) -> list[dict[str, Any]]:
    """対象日のS3オブジェクトを取得して記事配列として返す。

    処理内容:
        対象日境界をUTCで計算し、S3オブジェクト一覧を走査して `LastModified` で
        フィルタしたオブジェクトのみダウンロードし、JSONを辞書へ変換して蓄積する。
    入力:
        target_date: 取得対象日（`YYYY-MM-DD`）。
        storage: `s3_bucket` / `s3_prefix` を含む設定。
        aws_credentials_block_name: AwsCredentialsブロック名。
    出力:
        list[dict[str, Any]]: 対象日の記事データ配列。
    例外:
        ValueError: target_date形式不正時。
        JSONDecodeError: オブジェクト本文がJSONでない場合。
        boto3由来の例外: S3アクセス失敗時。
    外部依存リソース:
        AWS S3、Prefect AwsCredentialsブロック。
    """
    logger = _get_task_logger()
    target = _parse_target_date(target_date)
    day_start = datetime.combine(target, datetime.min.time(), tzinfo=timezone.utc)
    day_end = day_start + timedelta(days=1)

    aws_credentials = AwsCredentials.load(aws_credentials_block_name)
    s3_client = _create_s3_client(aws_credentials)

    matched_articles: list[dict[str, Any]] = []
    matched_object_logs: list[dict[str, Any]] = []
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=storage["s3_bucket"], Prefix=storage["s3_prefix"].strip("/"))

    for page in pages:
        for obj in page.get("Contents", []):
            object_key = obj["Key"]
            s3_path = f"s3://{storage['s3_bucket']}/{object_key}"
            size = int(obj.get("Size", 0))
            last_modified = obj["LastModified"]
            if last_modified.tzinfo is None:
                last_modified = last_modified.replace(tzinfo=timezone.utc)
            else:
                last_modified = last_modified.astimezone(timezone.utc)

            logger.debug(
                "checking s3 object: path=%s last_modified=%s data_length=%d",
                s3_path,
                last_modified.isoformat(),
                size,
            )
            if not (day_start <= last_modified < day_end):
                logger.debug(
                    "skip s3 object by target_date: path=%s last_modified=%s data_length=%d",
                    s3_path,
                    last_modified.isoformat(),
                    size,
                )
                continue

            logger.info("target S3 data fetch path: %s", s3_path)

            response = s3_client.get_object(Bucket=storage["s3_bucket"], Key=object_key)
            body_raw = response["Body"].read()
            body = body_raw.decode("utf-8")
            data_length = len(body_raw)
            logger.debug("downloaded s3 object: path=%s downloaded data_length=%d", s3_path, data_length)

            matched_articles.append(json.loads(body))
            matched_object_logs.append(
                {
                    "s3_path": s3_path,
                    "last_modified": last_modified.isoformat(),
                    "data_length": data_length,
                }
            )

    for matched in matched_object_logs:
        logger.info(
            "matched s3 object: path=%s last_modified=%s data_length=%d",
            matched["s3_path"],
            matched["last_modified"],
            matched["data_length"],
        )

    return matched_articles


@task(name="design_macro_themes_with_ollama_task")
def design_macro_themes_with_ollama_task(
    articles: list[dict[str, Any]],
    ollama_connection: dict[str, str],
    timeout_sec: int = 120,
) -> dict[str, Any]:
    """記事要約群をもとにマクロテーマ体系を生成する。

    処理内容:
        title と one_sentence_summary を持つ記事のみ抽出し、プロンプト生成後に
        OllamaへJSON形式で生成依頼して、結果JSON文字列を辞書へ変換する。
    入力:
        articles: 記事配列。
        ollama_connection: Ollama接続設定（base_url/model）。
        timeout_sec: API呼び出しタイムアウト秒。
    出力:
        dict[str, Any]: テーマ体系JSONを辞書化した値。
    例外:
        ValueError: 有効記事がない、空応答、JSONオブジェクトでない場合。
        JSONDecodeError: Ollama応答のJSON解析失敗時。
    外部依存リソース:
        Ollama HTTP API。
    """
    logger = _get_task_logger()
    compact_articles = [
        {
            "title": article["title"],
            "one_sentence_summary": article["one_sentence_summary"],
        }
        for article in articles
        if isinstance(article.get("title"), str)
        and article["title"]
        and isinstance(article.get("one_sentence_summary"), str)
        and article["one_sentence_summary"]
    ]
    if not compact_articles:
        raise ValueError("articles must include at least one valid title and one_sentence_summary pair")

    prompt = _build_macro_theme_prompt(compact_articles)
    logger.info("send macro-theme prompt to ollama: article_count=%d", len(compact_articles))
    raw_response = invoke_ollama_generate(
        ollama_connection=ollama_connection,
        prompt=prompt,
        timeout_sec=timeout_sec,
        response_format="json",
        logger=logger,
    )
    logger.info("received macro-theme response from ollama")
    if not raw_response:
        raise ValueError("Ollama response does not contain taxonomy JSON text")

    taxonomy = json.loads(raw_response)
    if not isinstance(taxonomy, dict):
        raise ValueError("Ollama taxonomy response must be a JSON object")
    logger.info("design_macro_themes_with_ollama_task result: %s", taxonomy)
    return taxonomy


@flow(name="daily-news-blog-digest-flow")
def daily_news_blog_digest_flow(target_date: str | None = None, config_path: str = "config.yaml") -> dict[str, Any]:
    """指定日の記事からマクロテーマ体系を生成するメインフロー。

    処理内容:
        設定読込・検証、対象日解決、S3記事取得、Ollama接続取得、テーマ生成タスク実行を順に行う。
    入力:
        target_date: 対象日（任意、`YYYY-MM-DD`）。
        config_path: 設定ファイルパス。
    出力:
        dict[str, Any]: 生成されたテーマ体系。
    例外:
        ValueError: 設定や日付、応答内容が不正な場合。
        boto3/Ollama由来例外: 外部I/O失敗時。
    外部依存リソース:
        ローカル設定ファイル、AWS S3、Prefectブロック、Ollama HTTP API。
    """
    logger = _get_task_logger()

    config = load_daily_digest_config_task(config_path)
    resolved_target_date = _resolve_target_date(target_date, config)
    logger.info("daily-news-blog-digest-flow start: target_date=%s", resolved_target_date)

    articles = fetch_daily_articles_from_s3_task(
        target_date=resolved_target_date,
        storage=config["storage"],
        aws_credentials_block_name=config["prefect_blocks"]["aws_credentials_block"],
    )
    ollama_secret = load_ollama_connection_secret(config["prefect_blocks"]["ollama_connection_secret_block"])
    ollama_connection = build_ollama_connection(
        ollama_secret,
        config["ollama"]["models"]["daily_news_blog_digest_flow"],
    )
    ollama_timeout_sec = config.get("ollama", {}).get("request_timeout_sec", 120)
    return design_macro_themes_with_ollama_task(
        articles=articles,
        ollama_connection=ollama_connection,
        timeout_sec=ollama_timeout_sec,
    )


if __name__ == "__main__":
    daily_news_blog_digest_flow()
