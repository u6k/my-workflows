from __future__ import annotations

import argparse
import json
import logging
import math
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
        build_llm_connection,
        create_s3_client,
        get_chroma_collection,
        invoke_llm_generate,
        load_llm_connection_secret,
        load_yaml_config,
        validate_chroma_config,
    )
else:
    from .common import (
        build_llm_connection,
        create_s3_client,
        get_chroma_collection,
        invoke_llm_generate,
        load_llm_connection_secret,
        load_yaml_config,
        validate_chroma_config,
    )

build_ollama_connection = build_llm_connection
load_ollama_connection_secret = load_llm_connection_secret
invoke_ollama_generate = invoke_llm_generate


CATEGORY_SUMMARY_JSON_SCHEMA: dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "NewsCategorySummary",
    "type": "object",
    "additionalProperties": False,
    "required": ["category_name", "category_summary", "key_points"],
    "properties": {
        "category_name": {
            "type": "string",
            "minLength": 1,
            "maxLength": 80,
            "description": "記事群の中心的な共通テーマを表すカテゴリー名",
        },
        "category_summary": {
            "type": "string",
            "minLength": 80,
            "maxLength": 1500,
            "description": "記事群全体を統合した要約。主要論点をできるだけ欠損なく含める",
        },
        "key_points": {
            "type": "array",
            "minItems": 3,
            "maxItems": 7,
            "uniqueItems": True,
            "description": "記事群から抽出した重要ポイントの配列",
            "items": {
                "type": "string",
                "minLength": 10,
                "maxLength": 240,
            },
        },
    },
}

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


def _validate_daily_digest_config(config: dict[str, Any]) -> None:
    """日次ダイジェストフロー設定の妥当性を検証する。

    処理内容:
        daily_news_blog_digest / prefect_blocks / ollama 配下の必須キー、型、
        数値範囲を検証する。`target_date` は設定ファイルからは読まないため
        検証対象に含めない。
    入力:
        config: 検証対象の設定辞書。
    出力:
        なし。
    例外:
        ValueError: 必須キー不足、型不正、値不正の場合。
    外部依存リソース:
        なし。
    """
    digest_config = config.get("daily_news_blog_digest")
    if not isinstance(digest_config, dict):
        raise ValueError("config.daily_news_blog_digest must be an object")
    s3_bucket = digest_config.get("s3_bucket")
    if not isinstance(s3_bucket, str) or not s3_bucket:
        raise ValueError("config.daily_news_blog_digest.s3_bucket must be a non-empty string")
    s3_prefix = digest_config.get("s3_prefix")
    if not isinstance(s3_prefix, str) or not s3_prefix:
        raise ValueError("config.daily_news_blog_digest.s3_prefix must be a non-empty string")

    prefect_blocks = config.get("prefect_blocks")
    if not isinstance(prefect_blocks, dict):
        raise ValueError("config.prefect_blocks must be an object")
    aws_block = prefect_blocks.get("aws_credentials_block")
    if not isinstance(aws_block, str) or not aws_block:
        raise ValueError("config.prefect_blocks.aws_credentials_block must be a non-empty string")
    llm_block = prefect_blocks.get("llm_connection_block", prefect_blocks.get("ollama_connection_block"))
    if not isinstance(llm_block, str) or not llm_block:
        raise ValueError("config.prefect_blocks.llm_connection_block must be a non-empty string")

    llm_config = config.get("llm", config.get("ollama"))
    if not isinstance(llm_config, dict):
        raise ValueError("config.llm or config.ollama must be an object")
    request_timeout_sec = llm_config.get("request_timeout_sec")
    if request_timeout_sec is not None and (not isinstance(request_timeout_sec, int) or request_timeout_sec <= 0):
        raise ValueError("config.llm.request_timeout_sec must be a positive integer")
    digest_model = digest_config.get("llm_model")
    if not isinstance(digest_model, str) or not digest_model:
        raise ValueError("config.daily_news_blog_digest.llm_model must be a non-empty string")

    validate_chroma_config(config.get("chroma"))

    max_articles = config.get("max_articles")
    if max_articles is not None and (not isinstance(max_articles, int) or max_articles <= 0):
        raise ValueError("config.max_articles must be a positive integer when provided")


def _build_digest_s3_keys(target_date: str, storage: dict[str, Any]) -> dict[str, str]:
    """日次ダイジェスト成果物のS3キーを組み立てる。

    処理内容:
        `storage.s3_prefix` 配下の `daily_digest` ディレクトリに、
        カテゴリーJSONとブログMarkdownのキーを生成する。
    入力:
        target_date: 対象日（`YYYY-MM-DD`）。
        storage: `s3_prefix` を含む設定。
    出力:
        dict[str, str]: `categories_key` と `blog_key` を持つ辞書。
    例外:
        なし。
    外部依存リソース:
        なし。
    """
    base = storage.get("s3_prefix", "").strip("/")
    root = f"{base}/daily_digest" if base else "daily_digest"
    return {
        "categories_key": f"{root}/categories/{target_date}.json",
        "blog_key": f"{root}/blog/{target_date}.md",
    }


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


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    """2つのベクトルのコサイン類似度を計算する。"""
    if len(a) != len(b):
        raise ValueError("embedding vectors must have same dimension")
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def _average_embedding(vectors: list[list[float]]) -> list[float]:
    """埋め込みベクトル群の要素平均を返す。"""
    if not vectors:
        return []
    dims = len(vectors[0])
    if dims == 0:
        return []
    for vector in vectors:
        if len(vector) != dims:
            raise ValueError("embedding vectors must have same dimension")
    return [sum(vector[i] for vector in vectors) / len(vectors) for i in range(dims)]


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


def _get_embeddings_collection(chroma_config: dict[str, Any]) -> Any:
    """日次ダイジェストが参照する Chroma collection を取得する。"""
    return get_chroma_collection(chroma_config=chroma_config)


@task(name="load_daily_articles_from_chroma_task")
def load_daily_articles_from_chroma_task(
    target_date: str,
    chroma_config: dict[str, Any],
    max_articles: int | None = None,
) -> list[dict[str, Any]]:
    """Chromaから対象日の記事（要約＋埋め込み付き）を時間範囲条件で取得する。"""
    logger = _get_task_logger()
    day = _parse_target_date(target_date)
    if max_articles is not None and max_articles <= 0:
        raise ValueError("max_articles must be a positive integer when provided")

    day_start = int(datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc).timestamp())
    day_end = int(datetime.combine(day + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc).timestamp())
    collection = _get_embeddings_collection(chroma_config)
    result = collection.get(
        where={
            "$and": [
                {"fetch_timestamp_epoch": {"$gte": day_start}},
                {"fetch_timestamp_epoch": {"$lt": day_end}},
            ]
        },
        include=["metadatas", "embeddings", "documents"],
    )

    ids = result.get("ids", []) if isinstance(result, dict) else []
    metadatas = result.get("metadatas", []) if isinstance(result, dict) else []
    embeddings = result.get("embeddings", []) if isinstance(result, dict) else []
    documents = result.get("documents", []) if isinstance(result, dict) else []

    articles: list[dict[str, Any]] = []
    for article_id, metadata, embedding, document in zip(ids, metadatas, embeddings, documents):
        if not isinstance(metadata, dict) or not isinstance(embedding, list) or not embedding:
            continue
        metadata_json = metadata.get("metadata_json")
        article = {
            "id": article_id,
            "url": metadata.get("article_url"),
            "title": metadata.get("title"),
            "published_timestamp": None,
            "fetch_timestamp": metadata.get("fetch_timestamp"),
            "briefing_summary": document,
            "one_sentence_summary": metadata.get("one_sentence_summary"),
            "metadata": json.loads(metadata_json) if isinstance(metadata_json, str) and metadata_json else {},
            "embedding": [float(v) for v in embedding],
            "embedding_timestamp": metadata.get("embedding_timestamp"),
            "fetch_timestamp_epoch": int(metadata.get("fetch_timestamp_epoch", 0)),
        }
        if not isinstance(article["fetch_timestamp"], str) or not article["fetch_timestamp"]:
            continue
        articles.append(article)

    articles.sort(key=lambda article: int(article.get("fetch_timestamp_epoch", 0)), reverse=True)
    if max_articles is not None:
        articles = articles[:max_articles]
    for article in articles:
        article.pop("fetch_timestamp_epoch", None)

    logger.info("loaded chroma articles: total=%d target_date=%s", len(articles), target_date)
    return articles


@task(name="load_categories_from_s3_task")
def load_categories_from_s3_task(
    target_date: str,
    storage: dict[str, Any],
    aws_credentials_block_name: str,
) -> dict[str, Any] | None:
    """S3上のカテゴリースナップショットを読み込む。

    処理内容:
        `daily_digest/categories/{target_date}.json` を `head_object` で確認し、
        存在する場合は `get_object` してJSONを辞書として返す。
    入力:
        target_date: 対象日（`YYYY-MM-DD`）。
        storage: `s3_bucket` / `s3_prefix` を含む設定。
        aws_credentials_block_name: AwsCredentialsブロック名。
    出力:
        dict[str, Any] | None: カテゴリーデータ。未存在時は `None`。
    例外:
        JSONDecodeError: JSON破損時。
        boto3由来例外: S3アクセス失敗時（404系以外）。
    外部依存リソース:
        AWS S3、Prefect AwsCredentialsブロック。
    """
    logger = _get_task_logger()
    keys = _build_digest_s3_keys(target_date, storage)
    aws_credentials = AwsCredentials.load(aws_credentials_block_name)
    s3_client = _create_s3_client(aws_credentials)

    try:
        s3_client.head_object(Bucket=storage["s3_bucket"], Key=keys["categories_key"])
    except Exception as exc:
        response = getattr(exc, "response", {})
        code = str(response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound"}:
            logger.info("category snapshot not found on S3: s3://%s/%s", storage["s3_bucket"], keys["categories_key"])
            return None
        raise

    response = s3_client.get_object(Bucket=storage["s3_bucket"], Key=keys["categories_key"])
    payload = json.loads(response["Body"].read().decode("utf-8"))
    logger.info("category snapshot loaded from S3: s3://%s/%s", storage["s3_bucket"], keys["categories_key"])
    return payload


@task(name="build_category_clusters_task")
def build_category_clusters_task(
    articles: list[dict[str, Any]],
    min_categories: int = 6,
    max_categories: int = 12,
    max_iterations: int = 6,
) -> list[dict[str, Any]]:
    """埋め込みベクトルから6〜12カテゴリ程度の集合を構築し、記事を所属させる。

    処理内容:
        本タスクでは、記事埋め込みに対して軽量なk-means風手順でカテゴリ集合を作る。

        1. **カテゴリ数の目標値決定**
           - 記事数 `n` に応じて `sqrt(n) * 2` を基準にしつつ、
             `min_categories`〜`max_categories` 範囲へクランプする。
           - 記事数が少ない場合は `n` を上限にする。
        2. **初期重心生成**
           - 入力順に `k` 件の埋め込みを初期重心として採用。
        3. **反復再割当（max_iterations回）**
           - 各記事を、コサイン類似度が最大の重心へ割当。
           - 各クラスタの重心を所属記事の平均埋め込みで更新。
           - 空クラスタが出た場合は除去。
        4. **カテゴリレコード化**
           - `category_id`（C01..）を採番。
           - `category_name` は仮名（代表記事タイトルベース）を付与。
           - 所属記事（id/title/url/summary等）と重心を保存する。

        この結果を後段の要約生成・S3保存にそのまま使える。
    入力:
        articles: `embedding` を含む記事配列。
        min_categories: 最小カテゴリ数。
        max_categories: 最大カテゴリ数。
        max_iterations: 割当反復回数。
    出力:
        list[dict[str, Any]]: カテゴリ集合。各要素に `category_id`, `category_name`,
            `article_count`, `centroid`, `articles` を含む。
    例外:
        ValueError: 引数不正、記事が空、ベクトル次元不一致時。
    外部依存リソース:
        なし。
    """
    if not articles:
        raise ValueError("articles must not be empty")
    if min_categories <= 0 or max_categories <= 0 or min_categories > max_categories:
        raise ValueError("min_categories and max_categories must be positive and min<=max")
    if max_iterations <= 0:
        raise ValueError("max_iterations must be a positive integer")

    valid_articles = [a for a in articles if isinstance(a.get("embedding"), list) and a["embedding"]]
    if not valid_articles:
        raise ValueError("articles must include at least one non-empty embedding")

    n = len(valid_articles)
    target_k = int(round(math.sqrt(n) * 2))
    target_k = max(min_categories, min(max_categories, target_k))
    target_k = min(target_k, n)

    # 初期重心は単純な先頭k件ではなく、farthest-point方式で多様性を確保する。
    # これにより、入力順が偏っていても初期クラスタが1つに潰れにくくなる。
    centroids: list[list[float]] = [list(valid_articles[0]["embedding"])]
    while len(centroids) < target_k:
        best_article: dict[str, Any] | None = None
        best_distance = -1.0
        for article in valid_articles:
            sims = [_cosine_similarity(article["embedding"], centroid) for centroid in centroids]
            nearest_similarity = max(sims)
            distance = 1.0 - nearest_similarity
            if distance > best_distance:
                best_distance = distance
                best_article = article
        if best_article is None:
            break
        centroids.append(list(best_article["embedding"]))
    assignments = [0] * n

    for _ in range(max_iterations):
        for i, article in enumerate(valid_articles):
            sims = [_cosine_similarity(article["embedding"], centroid) for centroid in centroids]
            assignments[i] = max(range(len(sims)), key=lambda idx: sims[idx])

        next_centroids: list[list[float]] = []
        for cluster_idx in range(len(centroids)):
            members = [valid_articles[i]["embedding"] for i, a in enumerate(assignments) if a == cluster_idx]
            if members:
                next_centroids.append(_average_embedding(members))

        if not next_centroids:
            break
        centroids = next_centroids

    grouped: dict[int, list[dict[str, Any]]] = {idx: [] for idx in range(len(centroids))}
    for i, article in enumerate(valid_articles):
        sims = [_cosine_similarity(article["embedding"], centroid) for centroid in centroids]
        best = max(range(len(sims)), key=lambda idx: sims[idx])
        grouped.setdefault(best, []).append(article)

    categories: list[dict[str, Any]] = []
    ordered = sorted(grouped.items(), key=lambda item: len(item[1]), reverse=True)
    for idx, (_, members) in enumerate(ordered, start=1):
        if not members:
            continue
        representative = members[0]
        categories.append(
            {
                "category_id": f"C{idx:02d}",
                "category_name": str(representative.get("title") or f"Category {idx}"),
                "article_count": len(members),
                "centroid": _average_embedding([m["embedding"] for m in members]),
                "articles": members,
            }
        )
    return categories


@task(name="summarize_each_category_task")
def summarize_each_category_task(
    categories: list[dict[str, Any]],
    llm_connection: dict[str, str],
    timeout_sec: int = 120,
) -> list[dict[str, Any]]:
    """カテゴリごとに名称と要約を生成し、カテゴリ情報を完成させる。

    処理内容:
        各カテゴリの代表記事群を入力にLLMへJSON生成依頼し、
        `category_name`, `category_summary`, `key_points` を補完する。
        JSONパース失敗時は既存名称を維持してフォールバック要約を設定する。
    入力:
        categories: `build_category_clusters_task` の出力。
        llm_connection: LLM接続設定。
        timeout_sec: API呼び出しタイムアウト秒。
    出力:
        list[dict[str, Any]]: 要約付与済みカテゴリ配列。
    例外:
        ValueError: 入力カテゴリが空の場合。
        LLM由来例外: 生成API失敗時。
    外部依存リソース:
        LiteLLM 対応 LLM API。
    """
    if not categories:
        raise ValueError("categories must not be empty")

    logger = _get_task_logger()
    enriched: list[dict[str, Any]] = []
    for category in categories:
        compact_articles = [
            {
                "title": article.get("title", ""),
                "one_sentence_summary": article.get("one_sentence_summary", ""),
                "briefing_summary": article.get("briefing_summary", ""),
            }
            for article in category.get("articles", [])
        ]
        prompt = (
            "以下に与える「ニュース記事データ」は、ある1つのカテゴリーに属する複数の記事情報です。\n"
            "あなたの役割は、記事群全体を読み解き、このカテゴリーを人間にとって読みやすく、かつ情報欠損をできるだけ抑えて要約することです。\n\n"
            "# 目的\n"
            "- 記事群に共通するテーマから「カテゴリー名」を生成する\n"
            "- 複数記事の内容を統合して「カテゴリー要約」を生成する\n"
            "- 重要論点を「キーポイント」として複数抽出する\n\n"
            "# 入力データ\n"
            "各記事には以下の情報が含まれます。\n"
            "- title: 記事タイトル\n"
            "- one_sentence_summary: 記事の一文要約\n"
            "- briefing_summary: 記事の詳細要約\n\n"
            "# 出力方針\n"
            "1. 記事群全体の共通テーマを表す、簡潔で具体的なカテゴリー名を付けること\n"
            "2. category_summary は、記事ごとの内容をなるべく欠損させずに統合し、自然で読みやすい日本語で 1〜3 段落相当の密度で要約すること\n"
            "3. 単なる羅列ではなく、全体像 → 主要論点 → 注意点や示唆、の流れでまとめること\n"
            "4. 記事間で共通する論点は整理・統合し、個別記事に固有だが重要な論点は落とさないこと\n"
            "5. briefing_summary 内に周辺的・補助的・ノイズ的な情報が含まれていても、カテゴリ全体との関連が薄いものは主軸にしないこと\n"
            "6. 事実関係は入力データの範囲からのみ述べ、外部知識を補わないこと\n"
            "7. key_points には、そのカテゴリーを理解するうえで重要な論点を 3〜7 個程度、簡潔な文で入れること\n"
            "8. key_points は重複を避け、それぞれ異なる観点を持たせること\n"
            "9. 出力は必ず指定の JSON 形式のみとし、Markdown、説明文、前置き、コードブロックは一切出力しないこと\n\n"
            "# 出力形式\n"
            "{\n"
            '  "category_name": "カテゴリー名",\n'
            '  "category_summary": "要約",\n'
            '  "key_points": [\n'
            '    "キーポイント1",\n'
            '    "キーポイント2",\n'
            '    "キーポイント3"\n'
            "  ]\n"
            "}\n\n"
            f"# ニュース記事データ\n{json.dumps(compact_articles, ensure_ascii=False)}"
        )
        raw = invoke_llm_generate(
            llm_connection=llm_connection,
            prompt=prompt,
            timeout_sec=timeout_sec,
            response_format=CATEGORY_SUMMARY_JSON_SCHEMA,
            logger=logger,
        )

        category_name = category.get("category_name", category.get("category_id", "Category"))
        category_summary = "要約を生成できませんでした。"
        key_points: list[str] = []
        if raw:
            try:
                payload = json.loads(raw)
                if isinstance(payload, dict):
                    category_name = str(payload.get("category_name") or category_name)
                    category_summary = str(payload.get("category_summary") or category_summary)
                    if isinstance(payload.get("key_points"), list):
                        key_points = [str(v) for v in payload["key_points"] if isinstance(v, str)]
            except json.JSONDecodeError:
                logger.warning("invalid category summary JSON: category_id=%s", category.get("category_id"))

        enriched.append(
            {
                **category,
                "category_name": category_name,
                "category_summary": category_summary,
                "key_points": key_points,
            }
        )

    return enriched


@task(name="save_categories_to_s3_task")
def save_categories_to_s3_task(
    target_date: str,
    categories: list[dict[str, Any]],
    storage: dict[str, Any],
    aws_credentials_block_name: str,
) -> str:
    """カテゴリースナップショットをS3へ保存する。

    処理内容:
        `daily_digest/categories/{target_date}.json` に、カテゴリ構築に必要な情報
        （ID、名称、所属記事、重心、要約、キーポイント）をJSONとして保存する。
    入力:
        target_date: 対象日（`YYYY-MM-DD`）。
        categories: 保存対象カテゴリ配列。
        storage: `s3_bucket` / `s3_prefix` を含む設定。
        aws_credentials_block_name: AwsCredentialsブロック名。
    出力:
        str: 保存先S3キー。
    例外:
        boto3由来例外: S3書き込み失敗時。
    外部依存リソース:
        AWS S3、Prefect AwsCredentialsブロック。
    """
    keys = _build_digest_s3_keys(target_date, storage)
    aws_credentials = AwsCredentials.load(aws_credentials_block_name)
    s3_client = _create_s3_client(aws_credentials)
    payload = {
        "target_date": target_date,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "categories": categories,
    }
    s3_client.put_object(
        Bucket=storage["s3_bucket"],
        Key=keys["categories_key"],
        Body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )
    return keys["categories_key"]


@task(name="compose_blog_style_digest_task")
def compose_blog_style_digest_task(target_date: str, category_payload: dict[str, Any]) -> str:
    """S3に保存されたカテゴリ情報からブログ風Markdownを生成する。

    処理内容:
        カテゴリーペイロードの `categories` を読み取り、
        全体像・カテゴリ別要約・代表記事・まとめをMarkdownで組み立てる。
    入力:
        target_date: 対象日（`YYYY-MM-DD`）。
        category_payload: S3から読んだカテゴリーデータ。
    出力:
        str: ブログ風Markdown本文。
    例外:
        ValueError: category_payload が不正な場合。
    外部依存リソース:
        なし。
    """
    categories = category_payload.get("categories")
    if not isinstance(categories, list) or not categories:
        raise ValueError("category_payload.categories must be a non-empty list")

    total_articles = sum(int(category.get("article_count", 0)) for category in categories)
    lines = [f"# Daily News Digest ({target_date})", "", "## 今日の全体像"]
    lines.append(f"- 全{total_articles}件の記事を{len(categories)}カテゴリに整理しました。")
    lines.append("")

    for category in categories:
        lines.append(f"## {category.get('category_name', category.get('category_id', 'カテゴリ'))}")
        lines.append(str(category.get("category_summary", "")))
        key_points = category.get("key_points") or []
        if key_points:
            lines.append("")
            for point in key_points:
                lines.append(f"- {point}")
        lines.append("")
        lines.append("代表記事:")
        for article in category.get("articles", [])[:3]:
            lines.append(f"- [{article.get('title', 'Untitled')}]({article.get('url', '#')})")
        lines.append("")

    lines.append("## まとめ")
    lines.append("- 主要カテゴリの変化を継続観測し、次日のトレンド変化に備えます。")
    return "\n".join(lines).rstrip() + "\n"


@task(name="save_blog_markdown_to_s3_task")
def save_blog_markdown_to_s3_task(
    target_date: str,
    markdown: str,
    storage: dict[str, Any],
    aws_credentials_block_name: str,
) -> str:
    """生成したブログ風MarkdownをS3へ保存する。

    処理内容:
        `daily_digest/blog/{target_date}.md` にMarkdown本文を保存する。
    入力:
        target_date: 対象日（`YYYY-MM-DD`）。
        markdown: 保存対象Markdown文字列。
        storage: `s3_bucket` / `s3_prefix` を含む設定。
        aws_credentials_block_name: AwsCredentialsブロック名。
    出力:
        str: 保存先S3キー。
    例外:
        boto3由来例外: S3書き込み失敗時。
    外部依存リソース:
        AWS S3、Prefect AwsCredentialsブロック。
    """
    keys = _build_digest_s3_keys(target_date, storage)
    aws_credentials = AwsCredentials.load(aws_credentials_block_name)
    s3_client = _create_s3_client(aws_credentials)
    s3_client.put_object(
        Bucket=storage["s3_bucket"],
        Key=keys["blog_key"],
        Body=markdown.encode("utf-8"),
        ContentType="text/markdown; charset=utf-8",
    )
    return keys["blog_key"]


@flow(name="daily-news-blog-digest-flow")
def daily_news_blog_digest_flow(target_date: str, config_path: str = "config.yaml") -> dict[str, Any]:
    """Chroma記事埋め込みを用いて日次カテゴリ集合とブログ文書を生成・保存する。

    処理内容:
        1. 設定を読み込み・検証する。
        2. `target_date`（引数必須）を検証する。
        3. S3上のカテゴリースナップショット有無を確認する。
           - 既存ならカテゴリ構築をスキップ。
           - 未存在なら Chroma から対象日記事を時間範囲条件で読み出し、
             6〜12カテゴリ程度へクラスタリングしてカテゴリ要約を生成し、S3へ保存する。
        4. ブログ生成時は必ずS3上のカテゴリーデータを参照する。
        5. 生成MarkdownをS3へ保存し、保存先キーと件数情報を返す。
    入力:
        target_date: 対象日（`YYYY-MM-DD`）。必須。
        config_path: 設定ファイルパス。
    出力:
        dict[str, Any]: 処理結果サマリ（件数・S3キー・スキップ有無）。
    例外:
        ValueError: 引数/設定/データ不正時。
        boto3/ChromaDB/LLM由来例外: 外部I/O失敗時。
    外部依存リソース:
        ローカル設定ファイル、ChromaDB HTTP サーバー、AWS S3、Prefectブロック、LiteLLM 対応 LLM API。
    """
    logger = _get_task_logger()
    parsed_target_date = _parse_target_date(target_date).isoformat()

    config = load_daily_digest_config_task(config_path)
    logger.info("daily-news-blog-digest-flow start: target_date=%s", parsed_target_date)

    digest_storage = {
        "s3_bucket": config["daily_news_blog_digest"]["s3_bucket"],
        "s3_prefix": config["daily_news_blog_digest"]["s3_prefix"],
    }
    aws_block = config["prefect_blocks"]["aws_credentials_block"]
    categories_payload = load_categories_from_s3_task(
        target_date=parsed_target_date,
        storage=digest_storage,
        aws_credentials_block_name=aws_block,
    )

    skipped_category_build = categories_payload is not None
    if categories_payload is None:
        articles = load_daily_articles_from_chroma_task(
            target_date=parsed_target_date,
            chroma_config=validate_chroma_config(config["chroma"]),
            max_articles=config.get("max_articles"),
        )
        if not articles:
            raise ValueError(f"no chroma articles found for target_date={parsed_target_date}")

        clusters = build_category_clusters_task(
            articles=articles,
            min_categories=6,
            max_categories=12,
            max_iterations=6,
        )
        llm_block_name = config["prefect_blocks"].get("llm_connection_block", config["prefect_blocks"].get("ollama_connection_block"))
        llm_secret = load_llm_connection_secret(llm_block_name)
        llm_connection = build_llm_connection(
            llm_secret,
            config["daily_news_blog_digest"]["llm_model"],
        )
        categories = summarize_each_category_task(
            categories=clusters,
            llm_connection=llm_connection,
            timeout_sec=config.get("llm", config.get("ollama", {})).get("request_timeout_sec", 120),
        )
        categories_key = save_categories_to_s3_task(
            target_date=parsed_target_date,
            categories=categories,
            storage=digest_storage,
            aws_credentials_block_name=aws_block,
        )
        logger.info("category snapshot saved: s3://%s/%s", digest_storage["s3_bucket"], categories_key)
        categories_payload = load_categories_from_s3_task(
            target_date=parsed_target_date,
            storage=digest_storage,
            aws_credentials_block_name=aws_block,
        )

    if categories_payload is None:
        raise ValueError("failed to load category payload from S3")

    markdown = compose_blog_style_digest_task(
        target_date=parsed_target_date,
        category_payload=categories_payload,
    )
    blog_key = save_blog_markdown_to_s3_task(
        target_date=parsed_target_date,
        markdown=markdown,
        storage=digest_storage,
        aws_credentials_block_name=aws_block,
    )

    category_count = len(categories_payload.get("categories", [])) if isinstance(categories_payload, dict) else 0
    article_count = sum(
        int(category.get("article_count", 0))
        for category in categories_payload.get("categories", [])
        if isinstance(category, dict)
    )
    return {
        "target_date": parsed_target_date,
        "skipped_category_build": skipped_category_build,
        "category_count": category_count,
        "article_count": article_count,
        "categories_s3_key": _build_digest_s3_keys(parsed_target_date, digest_storage)["categories_key"],
        "blog_s3_key": blog_key,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run daily-news-blog-digest-flow.")
    parser.add_argument(
        "--target-date",
        dest="target_date",
        required=True,
        help="Target date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--config-path",
        dest="config_path",
        default="config.yaml",
        help="Path to config YAML file.",
    )
    args = parser.parse_args()
    daily_news_blog_digest_flow(target_date=args.target_date, config_path=args.config_path)
