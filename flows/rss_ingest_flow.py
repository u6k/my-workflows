from __future__ import annotations

import logging
import hashlib
import json
import sqlite3
import sys
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET

from prefect import flow, get_run_logger, task
from prefect.exceptions import MissingContextError
from prefect_aws.credentials import AwsCredentials
import trafilatura

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parent))
    from common import (
        build_ollama_connection,
        create_s3_client,
        invoke_ollama_generate,
        invoke_ollama_embeddings,
        load_ollama_connection_secret,
        load_yaml_config,
    )
else:
    from .common import (
        build_ollama_connection,
        create_s3_client,
        invoke_ollama_generate,
        invoke_ollama_embeddings,
        load_ollama_connection_secret,
        load_yaml_config,
    )


REQUIRED_PREFECT_BLOCK_KEYS = (
    "aws_credentials_block",
    "ollama_connection_secret_block",
)
BRIEFING_PROMPT_TEMPLATE = """以下のニュース記事から主要なテーマとアイデアを統合した包括的なブリーフィングドキュメントを作成してください。まずは、最も重要なポイントを簡潔にまとめたエグゼクティブサマリーから始めましょう。本文では、情報源に含まれる主要なテーマ、証拠、そして結論を​​詳細かつ徹底的に検証する必要があります。分析は、明瞭性を確保するために、見出しと箇条書きを用いて論理的に構成する必要があります。トーンは客観的かつ鋭いものでなければなりません。

# ニュース記事
```txt
{article_content}
```"""
ONE_SENTENCE_PROMPT_TEMPLATE = """以下のニュース記事を、内容が過不足なく伝わるように「一文」で要約してください。
余計な解説は不要です。

# ニュース記事
```txt
{article_content}
```"""


def _load_yaml_config(config_path: str) -> dict[str, Any]:
    """共通ユーティリティ経由でYAML設定を読み込む。

    処理内容:
        `load_yaml_config` を呼び出して設定辞書を取得する。
    入力:
        config_path: 設定ファイルパス。
    出力:
        dict[str, Any]: 設定辞書。
    例外:
        ValueError: 設定ファイルが不正な場合。
    外部依存リソース:
        ローカルファイルシステム。
    """
    return load_yaml_config(config_path)


def _validate_config(config: dict[str, Any]) -> None:
    """RSS収集フローの設定値を検証する。

    処理内容:
        rss_urls / retry / storage / prefect_blocks / ollama の必須キー、型、値域を検証する。
    入力:
        config: 検証対象設定辞書。
    出力:
        なし。
    例外:
        ValueError: キー不足、型不正、値不正がある場合。
    外部依存リソース:
        なし。
    """
    rss_urls = config.get("rss_urls")
    if not isinstance(rss_urls, list) or len(rss_urls) == 0:
        raise ValueError("config.rss_urls must be a non-empty list")

    invalid_urls = [url for url in rss_urls if not isinstance(url, str) or not url.startswith(("http://", "https://"))]
    if invalid_urls:
        raise ValueError("all config.rss_urls values must start with http:// or https://")

    retry = config.get("retry")
    if not isinstance(retry, dict):
        raise ValueError("config.retry must be an object")

    max_retries = retry.get("max_retries")
    if not isinstance(max_retries, int):
        raise ValueError("config.retry.max_retries must be an integer")

    if "initial_delay_sec" in retry and not isinstance(retry["initial_delay_sec"], int):
        raise ValueError("config.retry.initial_delay_sec must be an integer")

    if "backoff_multiplier" in retry and not isinstance(retry["backoff_multiplier"], (int, float)):
        raise ValueError("config.retry.backoff_multiplier must be a number")

    storage = config.get("storage")
    if not isinstance(storage, dict):
        raise ValueError("config.storage must be an object")

    s3_prefix = storage.get("s3_prefix")
    if not isinstance(s3_prefix, str) or not s3_prefix:
        raise ValueError("config.storage.s3_prefix must be a non-empty string")

    s3_bucket = storage.get("s3_bucket")
    if not isinstance(s3_bucket, str) or not s3_bucket:
        raise ValueError("config.storage.s3_bucket must be a non-empty string")

    prefect_blocks = config.get("prefect_blocks")
    if not isinstance(prefect_blocks, dict):
        raise ValueError("config.prefect_blocks must be an object")

    missing_block_keys = [key for key in REQUIRED_PREFECT_BLOCK_KEYS if key not in prefect_blocks]
    if missing_block_keys:
        raise ValueError(f"config.prefect_blocks is missing required keys: {', '.join(missing_block_keys)}")

    invalid_block_names = [
        key
        for key in REQUIRED_PREFECT_BLOCK_KEYS
        if not isinstance(prefect_blocks.get(key), str) or not prefect_blocks[key]
    ]
    if invalid_block_names:
        raise ValueError(f"config.prefect_blocks values must be non-empty strings: {', '.join(invalid_block_names)}")

    ollama = config.get("ollama")
    if not isinstance(ollama, dict):
        raise ValueError("config.ollama must be an object")

    request_timeout_sec = ollama.get("request_timeout_sec")
    if request_timeout_sec is not None and (not isinstance(request_timeout_sec, int) or request_timeout_sec <= 0):
        raise ValueError("config.ollama.request_timeout_sec must be a positive integer")

    models = ollama.get("models")
    if not isinstance(models, dict):
        raise ValueError("config.ollama.models must be an object")

    rss_ingest_model = models.get("rss_ingest_flow")
    if not isinstance(rss_ingest_model, str) or not rss_ingest_model:
        raise ValueError("config.ollama.models.rss_ingest_flow must be a non-empty string")
    rss_ingest_embedding_model = models.get("rss_ingest_flow_embedding")
    if not isinstance(rss_ingest_embedding_model, str) or not rss_ingest_embedding_model:
        raise ValueError("config.ollama.models.rss_ingest_flow_embedding must be a non-empty string")

    embeddings = config.get("embeddings", {})
    if not isinstance(embeddings, dict):
        raise ValueError("config.embeddings must be an object when provided")
    sqlite_path = embeddings.get("sqlite_path", ".data/rss_embeddings.sqlite3")
    if not isinstance(sqlite_path, str) or not sqlite_path:
        raise ValueError("config.embeddings.sqlite_path must be a non-empty string")


def _get_task_logger() -> logging.Logger:
    """実行コンテキストに応じたロガーを返す。

    処理内容:
        Prefectラン中は `get_run_logger`、それ以外はモジュールロガーを返す。
    入力:
        なし。
    出力:
        logging.Logger: 利用可能なロガー。
    例外:
        なし（`MissingContextError` は内部処理）。
    外部依存リソース:
        Prefect実行コンテキスト。
    """
    try:
        return get_run_logger()
    except MissingContextError:
        return logging.getLogger(__name__)


def _local_name(tag: str) -> str:
    """XMLタグ名から名前空間を除いたローカル名を返す。

    処理内容:
        `}` 区切りで末尾要素を取り出し、名前空間なしタグはそのまま返す。
    入力:
        tag: XML要素タグ。
    出力:
        str: ローカルタグ名。
    例外:
        なし。
    外部依存リソース:
        なし。
    """
    if "}" in tag:
        return tag.rsplit("}", 1)[1]
    return tag


def _normalize_extracted_link(link: str) -> str:
    """抽出リンクを正規化し、GoogleリダイレクトURLなら実URLへ変換する。

    処理内容:
        Googleニュースの `https://www.google.com/url` 形式を解析し、`url` クエリ値を返す。
        それ以外は入力値をそのまま返す。
    入力:
        link: 抽出リンク文字列。
    出力:
        str: 正規化後リンク。
    例外:
        なし。
    外部依存リソース:
        なし。
    """
    if not link.startswith("https://www.google.com/url"):
        return link

    parsed = urlparse(link)
    query = parse_qs(parsed.query)
    return query.get("url", [link])[0]


def _url_to_md5(url: str) -> str:
    """URLから記事識別用のMD5ハッシュを生成する。

    処理内容:
        URL文字列をUTF-8エンコードしてMD5ハッシュ化する。
    入力:
        url: 記事URL。
    出力:
        str: 32文字の16進MD5文字列。
    例外:
        なし。
    外部依存リソース:
        なし。
    """
    return hashlib.md5(url.encode("utf-8")).hexdigest()


def _build_s3_object_key(s3_prefix: str, article_id: str) -> str:
    """記事IDからS3保存用オブジェクトキーを生成する。

    処理内容:
        プレフィックスの前後スラッシュを除去し、`<prefix>/<id先頭2文字>/<id>.json` 形式で返す。
    入力:
        s3_prefix: S3プレフィックス。
        article_id: 記事ID（MD5想定）。
    出力:
        str: S3オブジェクトキー。
    例外:
        なし。
    外部依存リソース:
        なし。
    """
    normalized_prefix = s3_prefix.strip("/")
    return f"{normalized_prefix}/{article_id[:2]}/{article_id}.json"


def _create_s3_client(aws_credentials: AwsCredentials) -> Any:
    """共通ユーティリティ経由でS3クライアントを生成する。

    処理内容:
        `create_s3_client` を呼び出して接続設定済みクライアントを返す。
    入力:
        aws_credentials: Prefect AwsCredentials ブロック。
    出力:
        Any: boto3 S3クライアント。
    例外:
        ValueError: AWSクライアント設定が不正な場合。
    外部依存リソース:
        Prefectブロック、AWS SDK。
    """
    return create_s3_client(aws_credentials)


def _initialize_embeddings_sqlite(sqlite_path: str) -> None:
    """埋め込み保存用SQLiteを初期化する。"""
    db_path = Path(sqlite_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS article_embeddings (
                article_id TEXT PRIMARY KEY,
                article_url TEXT NOT NULL,
                model_name TEXT NOT NULL,
                embedding_json TEXT NOT NULL,
                source_text TEXT NOT NULL,
                article_published_timestamp TEXT,
                embedding_created_at_utc TEXT
            )
            """
        )
        existing_columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info(article_embeddings)")
        }
        if "article_published_timestamp" not in existing_columns:
            conn.execute("ALTER TABLE article_embeddings ADD COLUMN article_published_timestamp TEXT")
        if "embedding_created_at_utc" not in existing_columns:
            conn.execute("ALTER TABLE article_embeddings ADD COLUMN embedding_created_at_utc TEXT")
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_article_embeddings_model
            ON article_embeddings(model_name)
            """
        )
        conn.commit()


def _build_briefing_prompt(article_content: str) -> str:
    """詳細ブリーフィング要約用のLLMプロンプトを生成する。

    処理内容:
        固定テンプレートへ記事本文を埋め込んで文字列を返す。
    入力:
        article_content: 記事本文。
    出力:
        str: 生成プロンプト。
    例外:
        なし。
    外部依存リソース:
        なし。
    """
    return BRIEFING_PROMPT_TEMPLATE.format(article_content=article_content)


def _build_one_sentence_prompt(article_content: str) -> str:
    """一文要約用のLLMプロンプトを生成する。

    処理内容:
        固定テンプレートへ記事本文を埋め込んで文字列を返す。
    入力:
        article_content: 記事本文。
    出力:
        str: 生成プロンプト。
    例外:
        なし。
    外部依存リソース:
        なし。
    """
    return ONE_SENTENCE_PROMPT_TEMPLATE.format(article_content=article_content)


def _extract_links_from_feed_xml(feed_xml: bytes) -> list[str]:
    """RSS/Atom XMLから記事リンク一覧を抽出して返す。

    処理内容:
        XMLをパースしてRSS item/link と Atom entry/link を収集し、
        Googleリダイレクト正規化、HTTP(S)チェック、重複排除を行う。
    入力:
        feed_xml: フィードXMLバイト列。
    出力:
        list[str]: 正規化済み記事URL配列。
    例外:
        ValueError: XML解析失敗時。
    外部依存リソース:
        なし。
    """
    try:
        root = ET.fromstring(feed_xml)
    except ET.ParseError as exc:
        raise ValueError("failed to parse RSS/Atom XML") from exc

    links: list[str] = []

    for element in root.iter():
        name = _local_name(element.tag)

        if name == "item":
            for child in element:
                if _local_name(child.tag) == "link" and child.text:
                    links.append(child.text.strip())
        elif name == "entry":
            for child in element:
                if _local_name(child.tag) != "link":
                    continue

                href = child.attrib.get("href")
                rel = child.attrib.get("rel", "alternate")
                if href and rel in ("alternate", ""):
                    links.append(href.strip())

    unique_links: list[str] = []
    seen: set[str] = set()
    for link in links:
        normalized_link = _normalize_extracted_link(link)
        if not normalized_link or not normalized_link.startswith(("http://", "https://")):
            continue
        if normalized_link in seen:
            continue
        seen.add(normalized_link)
        unique_links.append(normalized_link)

    return unique_links


class _ArticleHTMLParser(HTMLParser):
    """記事HTMLからタイトル・可視テキスト・メタ情報を抽出する。"""

    def __init__(self) -> None:
        """抽出状態と出力バッファを初期化する。

        処理内容:
            タイトル抽出状態、script/styleスキップ深度、可視テキスト蓄積先、メタ情報辞書を初期化する。
        入力:
            なし。
        出力:
            なし。
        例外:
            なし。
        外部依存リソース:
            なし。
        """
        super().__init__(convert_charrefs=True)
        self.in_title = False
        self.skip_depth = 0
        self.title: str | None = None
        self.visible_text_parts: list[str] = []
        self.metadata: dict[str, Any] = {"tags": []}

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        """開始タグを解釈して内部状態を更新する。

        処理内容:
            script/style等のスキップ制御、title抽出開始、metaタグからOGP/author/keywords等を収集する。
        入力:
            tag: 開始タグ名。
            attrs: タグ属性配列。
        出力:
            なし。
        例外:
            なし。
        外部依存リソース:
            なし。
        """
        attrs_dict = {k.lower(): (v or "") for k, v in attrs}
        lower_tag = tag.lower()

        if lower_tag in {"script", "style", "noscript"}:
            self.skip_depth += 1
            return

        if lower_tag == "title":
            self.in_title = True
            return

        if lower_tag != "meta":
            return

        property_name = attrs_dict.get("property", "").lower()
        name = attrs_dict.get("name", "").lower()
        content = attrs_dict.get("content", "").strip()
        if not content:
            return

        if property_name == "og:title" and not self.title:
            self.title = content
        elif property_name == "og:site_name":
            self.metadata["site_name"] = content
        elif property_name == "og:image":
            self.metadata["image_url"] = content
        elif property_name == "article:published_time":
            self.metadata["published_timestamp"] = content
        elif property_name == "article:modified_time":
            self.metadata["updated_timestamp"] = content
        elif property_name == "article:tag":
            self.metadata["tags"].append(content)

        if name == "author":
            self.metadata["author"] = content
        elif name == "keywords":
            keywords = [tag.strip() for tag in content.split(",") if tag.strip()]
            self.metadata["tags"].extend(keywords)
        elif name in {"lang", "language"}:
            self.metadata["language"] = content

    def handle_endtag(self, tag: str) -> None:
        """終了タグを解釈して内部状態を更新する。

        処理内容:
            script/style等のスキップ深度を減算し、title終了時にタイトル抽出状態を解除する。
        入力:
            tag: 終了タグ名。
        出力:
            なし。
        例外:
            なし。
        外部依存リソース:
            なし。
        """
        lower_tag = tag.lower()
        if lower_tag in {"script", "style", "noscript"} and self.skip_depth > 0:
            self.skip_depth -= 1
            return

        if lower_tag == "title":
            self.in_title = False

    def handle_data(self, data: str) -> None:
        """テキストノードをタイトルまたは本文候補として収集する。

        処理内容:
            スキップ対象内の文字列を無視し、空白除去後テキストを title または本文配列へ追加する。
        入力:
            data: テキストノード。
        出力:
            なし。
        例外:
            なし。
        外部依存リソース:
            なし。
        """
        if self.skip_depth > 0:
            return

        text = data.strip()
        if not text:
            return

        if self.in_title:
            if self.title:
                self.title = f"{self.title} {text}".strip()
            else:
                self.title = text
            return

        self.visible_text_parts.append(text)


def _extract_article_content_and_metadata(article_html: str) -> dict[str, Any]:
    """記事HTMLから本文とメタデータを抽出して返す。

    処理内容:
        自前HTMLパーサーでタイトル・メタ情報・可視テキストを抽出し、本文は trafilatura を優先、
        空の場合は可視テキスト連結へフォールバックする。
    入力:
        article_html: 記事HTML文字列。
    出力:
        dict[str, Any]: `title` / `content` / `metadata` を含む辞書。
    例外:
        なし（抽出失敗時は空文字列やフォールバックで継続）。
    外部依存リソース:
        trafilaturaライブラリ。
    """
    parser = _ArticleHTMLParser()
    parser.feed(article_html)

    tags = sorted(set(parser.metadata.get("tags", [])))
    metadata = {key: value for key, value in parser.metadata.items() if key != "tags" and value}
    if tags:
        metadata["tags"] = tags

    extracted_content = trafilatura.extract(
        article_html,
        output_format="txt",
        include_comments=False,
        include_tables=False,
        deduplicate=True,
    )
    if isinstance(extracted_content, str):
        content = extracted_content.strip()
    else:
        content = ""

    if not content:
        content = "\n".join(parser.visible_text_parts).strip()

    return {
        "title": parser.title or "",
        "content": content,
        "metadata": metadata,
    }


@task(name="fetch_feed_task")
def fetch_feed_task(feed_url: str) -> list[str]:
    """フィードURLから記事リンク一覧を取得する。

    処理内容:
        フィードURLへHTTPアクセスしてXMLを取得し、リンク抽出関数でURL配列化する。
        取得件数が0件ならエラーとする。
    入力:
        feed_url: RSS/AtomフィードURL。
    出力:
        list[str]: 記事URL配列。
    例外:
        ValueError: フィードに有効エントリが無い場合。
        URLError/HTTPError: フィード取得に失敗した場合。
    外部依存リソース:
        HTTPフィード配信元。
    """
    logger = _get_task_logger()

    with urlopen(feed_url, timeout=30) as response:
        feed_xml = response.read()

    links = _extract_links_from_feed_xml(feed_xml)
    logger.debug("extracted links: feed_url=%s links=%s", feed_url, links)
    if len(links) == 0:
        raise ValueError(f"feed has no entries: {feed_url}")

    return links


@task(name="fetch_article_task")
def fetch_article_task(article_url: str) -> dict[str, Any]:
    """記事URLからHTMLを取得し、本文抽出結果を返す。

    処理内容:
        User-Agent付きリクエストで記事ページを取得し、ステータス検証後にHTMLをデコードして
        本文/メタ抽出を行い、URL・ID・raw_htmlを付与して返す。
    入力:
        article_url: 記事URL。
    出力:
        dict[str, Any]: 抽出結果を含む記事辞書。
    例外:
        ValueError: HTTPステータスが200以外の場合。
        URLError/HTTPError: 通信失敗時。
    外部依存リソース:
        HTTP記事ページ。
    """
    request = Request(
        article_url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; rss-ingest-flow/1.0)",
        },
    )

    with urlopen(request, timeout=30) as response:
        status = getattr(response, "status", 200)
        if status != 200:
            raise ValueError(f"unexpected status code: {status}")

        raw_html = response.read()
        content_type = response.headers.get_content_charset() or "utf-8"

    article_html = raw_html.decode(content_type, errors="replace")
    extracted = _extract_article_content_and_metadata(article_html)
    extracted["url"] = article_url
    extracted["id"] = _url_to_md5(article_url)
    extracted["raw_html"] = article_html
    return extracted


@task(name="store_to_s3_task")
def store_to_s3_task(article: dict[str, Any], storage: dict[str, Any], aws_credentials_block_name: str) -> str:
    """記事データをJSONとしてS3へ保存する。

    処理内容:
        記事IDからオブジェクトキーを生成し、AwsCredentialsブロック経由のS3クライアントで
        `put_object` を実行する。
    入力:
        article: 保存対象記事辞書（`id` 必須）。
        storage: `s3_bucket` / `s3_prefix` を含む設定。
        aws_credentials_block_name: AwsCredentialsブロック名。
    出力:
        str: 保存したS3オブジェクトキー。
    例外:
        KeyError: 必須キー不足時。
        boto3由来例外: S3書き込み失敗時。
    外部依存リソース:
        AWS S3、Prefect AwsCredentialsブロック。
    """
    article_id = article["id"]
    object_key = _build_s3_object_key(storage["s3_prefix"], article_id)

    aws_credentials = AwsCredentials.load(aws_credentials_block_name)
    s3_client = _create_s3_client(aws_credentials)
    s3_client.put_object(
        Bucket=storage["s3_bucket"],
        Key=object_key,
        Body=json.dumps(article, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )
    return object_key


@task(name="check_s3_object_exists_task")
def check_s3_object_exists_task(article_url: str, storage: dict[str, Any], aws_credentials_block_name: str) -> dict[str, Any]:
    """記事URL対応のS3オブジェクト存在有無を確認する。

    処理内容:
        URLをID化してオブジェクトキーを計算し、`head_object` を呼び出して存在判定する。
        404系エラーは未存在として扱い、その他エラーは再送出する。
    入力:
        article_url: 記事URL。
        storage: `s3_bucket` / `s3_prefix` を含む設定。
        aws_credentials_block_name: AwsCredentialsブロック名。
    出力:
        dict[str, Any]: `exists` / `id` / `object_key` を含む辞書。
    例外:
        boto3由来例外: 404系以外のS3エラー時。
    外部依存リソース:
        AWS S3、Prefect AwsCredentialsブロック。
    """
    article_id = _url_to_md5(article_url)
    object_key = _build_s3_object_key(storage["s3_prefix"], article_id)

    aws_credentials = AwsCredentials.load(aws_credentials_block_name)
    s3_client = _create_s3_client(aws_credentials)
    try:
        s3_client.head_object(Bucket=storage["s3_bucket"], Key=object_key)
        return {"exists": True, "id": article_id, "object_key": object_key}
    except Exception as exc:
        response = getattr(exc, "response", {})
        error = response.get("Error", {})
        error_code = str(error.get("Code", ""))
        if error_code in {"404", "NoSuchKey", "NotFound"}:
            return {"exists": False, "id": article_id, "object_key": object_key}
        raise


@task(name="summarize_briefing_task")
def summarize_briefing_task(article_content: str, ollama_connection: dict[str, str], timeout_sec: int = 120) -> str:
    """記事本文を詳細ブリーフィング形式で要約する。

    処理内容:
        ブリーフィング用プロンプトを作成し、Ollama生成APIを呼び出して要約文を返す。
    入力:
        article_content: 記事本文。
        ollama_connection: Ollama接続設定。
        timeout_sec: APIタイムアウト秒。
    出力:
        str: 要約テキスト。
    例外:
        ValueError: Ollama応答が空の場合。
        Ollama呼び出し由来例外: 通信・解析失敗時。
    外部依存リソース:
        Ollama HTTP API。
    """
    logger = _get_task_logger()
    prompt = _build_briefing_prompt(article_content)
    logger.debug("ollama briefing prompt: %s", prompt)
    summary = invoke_ollama_generate(
        ollama_connection=ollama_connection,
        prompt=prompt,
        timeout_sec=timeout_sec,
        logger=logger,
    )
    logger.debug("ollama briefing response: %s", summary)
    if not summary:
        raise ValueError("Ollama response does not contain summary text")
    return summary


@task(name="summarize_one_sentence_task")
def summarize_one_sentence_task(article_content: str, ollama_connection: dict[str, str], timeout_sec: int = 120) -> str:
    """記事本文を一文で要約する。

    処理内容:
        一文要約用プロンプトを作成し、Ollama生成APIから要約文を取得して返す。
    入力:
        article_content: 記事本文。
        ollama_connection: Ollama接続設定。
        timeout_sec: APIタイムアウト秒。
    出力:
        str: 一文要約テキスト。
    例外:
        ValueError: Ollama応答が空の場合。
        Ollama呼び出し由来例外: 通信・解析失敗時。
    外部依存リソース:
        Ollama HTTP API。
    """
    logger = _get_task_logger()
    prompt = _build_one_sentence_prompt(article_content)
    logger.debug("ollama one-sentence prompt: %s", prompt)
    summary = invoke_ollama_generate(
        ollama_connection=ollama_connection,
        prompt=prompt,
        timeout_sec=timeout_sec,
        logger=logger,
    )
    logger.debug("ollama one-sentence response: %s", summary)
    if not summary:
        raise ValueError("Ollama response does not contain one sentence summary text")
    return summary


@task(name="upsert_briefing_embedding_to_sqlite_task")
def upsert_briefing_embedding_to_sqlite_task(
    article: dict[str, Any],
    embedding_connection: dict[str, str],
    sqlite_path: str,
    timeout_sec: int = 120,
) -> None:
    """記事ブリーフィング要約を埋め込み化してSQLiteへ保存する。"""
    logger = _get_task_logger()
    briefing_summary = article.get("briefing_summary")
    if not isinstance(briefing_summary, str) or not briefing_summary:
        raise ValueError("article.briefing_summary must be a non-empty string")

    embedding = invoke_ollama_embeddings(
        ollama_connection=embedding_connection,
        text=briefing_summary,
        timeout_sec=timeout_sec,
        logger=logger,
    )
    _initialize_embeddings_sqlite(sqlite_path)
    published_timestamp = article.get("published_timestamp")
    if not isinstance(published_timestamp, str):
        metadata = article.get("metadata")
        if isinstance(metadata, dict):
            meta_published_timestamp = metadata.get("published_timestamp")
            if isinstance(meta_published_timestamp, str):
                published_timestamp = meta_published_timestamp
            else:
                published_timestamp = None
        else:
            published_timestamp = None
    created_at_utc = datetime.now(timezone.utc).isoformat()

    with sqlite3.connect(sqlite_path) as conn:
        conn.execute(
            """
            INSERT INTO article_embeddings(
                article_id, article_url, model_name, embedding_json, source_text, article_published_timestamp, embedding_created_at_utc
            )
            VALUES(?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(article_id) DO UPDATE SET
                article_url=excluded.article_url,
                model_name=excluded.model_name,
                embedding_json=excluded.embedding_json,
                source_text=excluded.source_text,
                article_published_timestamp=excluded.article_published_timestamp,
                embedding_created_at_utc=excluded.embedding_created_at_utc
            """,
            (
                str(article["id"]),
                str(article["url"]),
                embedding_connection["model"],
                json.dumps(embedding),
                briefing_summary,
                published_timestamp,
                created_at_utc,
            ),
        )
        conn.commit()


@flow(name="fetch_and_summarize_article_flow")
def fetch_and_summarize_article_flow(
    article_url: str,
    ollama_connection: dict[str, str],
    timeout_sec: int = 120,
) -> dict[str, Any]:
    """単一記事の取得・本文抽出・要約を実行するサブフロー。

    処理内容:
        記事取得タスクと2種類の要約タスクを順に実行し、要約付き記事辞書を返す。
    入力:
        article_url: 記事URL。
        ollama_connection: Ollama接続設定。
        timeout_sec: APIタイムアウト秒。
    出力:
        dict[str, Any]: `fetch_article_task` の結果に要約2種を付与した辞書。
    例外:
        記事取得・要約処理で発生した例外をそのまま送出する。
    外部依存リソース:
        HTTP記事ページ、Ollama HTTP API。
    """
    article = fetch_article_task(article_url)
    article["briefing_summary"] = summarize_briefing_task(
        article_content=article["content"],
        ollama_connection=ollama_connection,
        timeout_sec=timeout_sec,
    )
    article["one_sentence_summary"] = summarize_one_sentence_task(
        article_content=article["content"],
        ollama_connection=ollama_connection,
        timeout_sec=timeout_sec,
    )
    return article


@task(name="load_config_task")
def load_config_task(config_path: str = "config.yaml") -> dict[str, Any]:
    """RSS収集フロー設定を読み込み、検証済みで返す。

    処理内容:
        YAML設定を読み込み `_validate_config` で妥当性を検証する。
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
    _validate_config(config)
    return config


@task(name="validate_prerequisites_task")
def validate_prerequisites_task(config: dict[str, Any]) -> None:
    """フロー実行前提となるPrefectブロック参照を検証する。

    処理内容:
        RSS URLの重複状況をログ出力し、AwsCredentialsブロックとOllama Secretブロックを
        実際にロードして参照可能性を確認する。
    入力:
        config: 検証対象設定辞書。
    出力:
        なし。
    例外:
        ValueError: Secret値不正時。
        Prefect由来例外: ブロックロード失敗時。
    外部依存リソース:
        Prefect Blocks（AwsCredentials/Secret）。
    """
    logger = get_run_logger()

    rss_urls = config["rss_urls"]
    unique_count = len(set(rss_urls))
    logger.info("rss_urls validation passed: total=%d unique=%d", len(rss_urls), unique_count)

    block_config = config["prefect_blocks"]

    aws_credentials_block = block_config["aws_credentials_block"]
    AwsCredentials.load(aws_credentials_block)
    logger.info("Prefect block loaded: key=aws_credentials_block name=%s", aws_credentials_block)

    ollama_secret_block = block_config["ollama_connection_secret_block"]
    load_ollama_connection_secret(ollama_secret_block, logger=logger)


@flow(name="rss_ingest_flow")
def rss_ingest_flow(config_path: str = "config.yaml") -> None:
    """RSS取得から記事要約保存までを実行するメインフロー。

    処理内容:
        設定読込、前提検証、フィード巡回、重複排除、既存S3チェック、記事取得、Ollama要約、
        S3保存、件数ログ出力を順に実行する。
    入力:
        config_path: 設定ファイルパス。
    出力:
        なし。
    例外:
        設定読込や外部I/O失敗時の例外（個別記事処理失敗は警告ログで継続）。
    外部依存リソース:
        ローカル設定ファイル、HTTPフィード/記事ページ、Prefect Blocks、Ollama API、AWS S3。
    """
    logger = _get_task_logger()

    config = load_config_task(config_path)
    validate_prerequisites_task(config)
    ollama_secret = load_ollama_connection_secret(config["prefect_blocks"]["ollama_connection_secret_block"])
    models = config["ollama"]["models"]
    ollama_connection = build_ollama_connection(ollama_secret, models["rss_ingest_flow"])
    embedding_model = models.get("rss_ingest_flow_embedding")
    if not isinstance(embedding_model, str) or not embedding_model:
        raise ValueError("config.ollama.models.rss_ingest_flow_embedding must be a non-empty string")
    embedding_connection = build_ollama_connection(ollama_secret, embedding_model)
    ollama_timeout_sec = config.get("ollama", {}).get("request_timeout_sec", 120)
    sqlite_path = config.get("embeddings", {}).get("sqlite_path", ".data/rss_embeddings.sqlite3")
    _initialize_embeddings_sqlite(sqlite_path)

    all_links: list[str] = []
    for feed_url in list(dict.fromkeys(config["rss_urls"])):
        links = fetch_feed_task(feed_url)
        all_links.extend(links)

    for link in all_links:
        logger.debug("all_links record: %s", link)

    unique_links = list(dict.fromkeys(all_links))
    logger.info("feed links extracted: total=%d", len(all_links))
    logger.info("feed links extracted: total=%d unique=%d", len(all_links), len(unique_links))

    article_count = 0
    stored_count = 0
    skipped_existing_count = 0
    for link in unique_links:
        s3_lookup = check_s3_object_exists_task(
            article_url=link,
            storage=config["storage"],
            aws_credentials_block_name=config["prefect_blocks"]["aws_credentials_block"],
        )
        if s3_lookup["exists"]:
            logger.info(
                "article already exists in s3, skip fetch/summarize: id=%s url=%s s3://%s/%s",
                s3_lookup["id"],
                link,
                config["storage"]["s3_bucket"],
                s3_lookup["object_key"],
            )
            skipped_existing_count += 1
            continue

        try:
            article = fetch_and_summarize_article_flow(
                article_url=link,
                ollama_connection=ollama_connection,
                timeout_sec=ollama_timeout_sec,
            )
            if isinstance(article.get("briefing_summary"), str) and article["briefing_summary"]:
                upsert_briefing_embedding_to_sqlite_task(
                    article=article,
                    embedding_connection=embedding_connection,
                    sqlite_path=sqlite_path,
                    timeout_sec=ollama_timeout_sec,
                )
        except Exception as exc:
            logger.warning("article fetch/summarize skipped: url=%s reason=%s", link, exc)
            continue

        object_key = store_to_s3_task(
            article=article,
            storage=config["storage"],
            aws_credentials_block_name=config["prefect_blocks"]["aws_credentials_block"],
        )

        logger.debug(
            "article fetched and stored: id=%s url=%s s3://%s/%s title=%s metadata=%s content_length=%d",
            article["id"],
            article["url"],
            config["storage"]["s3_bucket"],
            object_key,
            article["title"],
            article["metadata"],
            len(article["content"]),
        )
        article_count += 1
        stored_count += 1

    logger.info("article fetching completed: total=%d", article_count)
    logger.info("article storing completed: total=%d", stored_count)
    logger.info("article skipped by existing s3 object: total=%d", skipped_existing_count)


@flow(name="fetch_summarize_url_flow")
def fetch_summarize_url_flow(article_url: str, config_path: str = "config.yaml") -> dict[str, Any]:
    """単一URLの記事取得・本文抽出・要約を実行する独立フロー。

    処理内容:
        設定読込、Prefectブロック検証、記事取得、Ollama要約（詳細/一文）を順に実行し、
        流用しやすい記事辞書として返す。
    入力:
        article_url: 取得対象の記事URL。
        config_path: 設定ファイルパス。
    出力:
        dict[str, Any]: `id/url/title/content/metadata/briefing_summary/one_sentence_summary/raw_html` を含む記事辞書。
    例外:
        ValueError: URL形式不正、設定不正、要約失敗時など。
        外部I/O由来例外: HTTP取得・Prefectブロック参照失敗時。
    外部依存リソース:
        ローカル設定ファイル、HTTP記事ページ、Prefect Blocks、Ollama API。
    """
    if not isinstance(article_url, str) or not article_url.startswith(("http://", "https://")):
        raise ValueError("article_url must start with http:// or https://")

    logger = _get_task_logger()
    config = load_config_task(config_path)
    validate_prerequisites_task(config)
    ollama_secret = load_ollama_connection_secret(config["prefect_blocks"]["ollama_connection_secret_block"])
    ollama_connection = build_ollama_connection(ollama_secret, config["ollama"]["models"]["rss_ingest_flow"])
    ollama_timeout_sec = config.get("ollama", {}).get("request_timeout_sec", 120)

    article = fetch_and_summarize_article_flow(
        article_url=article_url,
        ollama_connection=ollama_connection,
        timeout_sec=ollama_timeout_sec,
    )

    logger.info(
        "single article summarize completed: id=%s url=%s title=%s content_length=%d",
        article["id"],
        article["url"],
        article["title"],
        len(article["content"]),
    )
    return article


if __name__ == "__main__":
    rss_ingest_flow()
