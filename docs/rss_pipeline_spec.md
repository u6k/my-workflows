# RSS Ingest Pipeline 仕様（Prefect）

本ドキュメントは、既存の Prefect ベース構成（`flows/hello_flow.py`）に準拠し、将来的に `flows/rss_ingest_flow.py` として実装するための要件定義である。

---

## 1. 入力設定の形式（`config.yaml`）

### 1.1 ファイル配置
- パス: `config.yaml`
- 文字コード: UTF-8
- 形式: YAML（トップレベルはオブジェクト）

### 1.2 想定構造

```yaml
rss_urls:
  - https://example.com/rss.xml
  - https://another.example.org/feed

retry:
  max_retries: 3
  initial_delay_sec: 2
  backoff_multiplier: 2

storage:
  s3_bucket: news-bucket
  s3_prefix: rss
```

### 1.3 キー定義
- `rss_urls`（必須）
  - 型: 配列（文字列）
  - 説明: RSS/Atom フィード URL の一覧。
  - 本仕様では URL 配列のみを定義できればよい。
- `retry`（必須）
  - `max_retries`（必須, 整数）: 失敗時の最大リトライ回数。
  - `initial_delay_sec`（任意, 整数）: 初回リトライ待機秒。
  - `backoff_multiplier`（任意, 数値）: 指数バックオフ係数。
- `storage`（必須）
  - `s3_bucket`（必須, 文字列）: S3互換ストレージのバケット名。
  - `s3_prefix`（必須, 文字列）: S3保存時のプレフィックス。

### 1.4 バリデーション要件
- `rss_urls` が空配列または未定義の場合、フローは開始時点で `FAILED` とする。
- `rss_urls` の各値は `http://` または `https://` で始まる必要がある。
- URL重複は許容するが、実行時に重複排除して同一 URL を二重取得しない。

---

## 2. 秘密情報管理（Prefect Blocks）

環境変数を直接運用するのではなく、Prefect の Block 機能で接続情報・秘密情報を管理する。

### 2.1 利用する Block
- S3互換ストレージ接続情報
  - 例: `S3 Bucket` / `AWS Credentials` 系 Block（利用ライブラリに応じて選択）
- Ollama 接続情報
  - 例: `Secret` Block に `OLLAMA_BASE_URL` とモデル名を保存、または JSON で接続設定を保存

### 2.2 フロー内での扱い
- `flows/rss_ingest_flow.py` の開始時に必要 Block をロードする。
- Block のロードに失敗した場合は `FAILED`。
- ログには Block 名やキー名のみを出力し、シークレット値は出力しない。

### 2.3 実装方針
- ローカル開発時のみ `.env` などのフォールバックを許容してもよいが、本番運用は Prefect Block を正とする。
- Block 名は `config.yaml` で参照可能にしてもよい（例: `prefect_blocks.s3_credentials_block`）。

---

## 3. 出力ファイル仕様（1記事 = 1JSONファイル）

### 3.1 保存単位
- 1記事ごとに1つの JSON ファイルを生成して S3 に保存する。
- JSON Lines / 連結レコード形式は採用しない（ファイル肥大化回避）。

### 3.2 JSON 必須キー
以下8キーを必須とする:
- `url`
- `title`
- `published_timestamp`
- `fetch_timestamp`
- `content`
- `raw_html`
- `briefing_summary`
- `one_sentence_summary`

### 3.3 JSON 例

```json
{
  "url": "https://example.com/posts/123",
  "title": "記事タイトル",
  "published_timestamp": "2026-04-01T12:34:56Z",
  "fetch_timestamp": "2026-04-02T00:00:00Z",
  "content": "記事本文（抽出済みテキスト）",
  "raw_html": "<!doctype html>...",
  "briefing_summary": "複数段落の要約（ブリーフィング向け）",
  "one_sentence_summary": "1文要約",
  "metadata": {
    "source_feed_url": "https://example.com/rss.xml",
    "author": "著者名",
    "tags": ["ai", "tech"],
    "language": "ja",
    "image_url": "https://example.com/hero.jpg",
    "entry_id": "tag:example.com,2026:123"
  }
}
```

### 3.4 metadata 要件
- `metadata` は任意拡張オブジェクトとして保持する。
- RSS/Atom や本文抽出から取得可能な属性を「可能な限り」格納する。
  - 例: `source_feed_url`, `author`, `categories/tags`, `language`, `image_url`, `entry_id`, `updated_timestamp`, `site_name` など。

### 3.5 S3 保存パス
記事 URL の MD5 を `url_md5` としたとき、以下に保存する。

- `s3://{S3_BUCKET}/{S3_PREFIX}/{url_md5の先頭2文字}/{url_md5}.json`

例:
- `s3://news-bucket/rss/ab/ab1234...ffff.json`

---

## 4. 取得件数ポリシー

- 1フィード当たりの最大取得件数は設定で定義しない。
- 各フィードに含まれる全リンク（全エントリ）を取得対象とする。
- 実装上の安全策として、異常に大量な場合は運用ガード（タイムアウトや総処理時間制限）を別途設けてもよいが、設定項目として `max_entries` は持たない。
- 記事URLのMD5から導出されるS3オブジェクトが既に存在する場合、当該記事の本文取得・要約・保存は再実行せず `SKIPPED` とする。

---

## 5. 失敗時ポリシー

リトライ回数は `config.yaml` の `retry.max_retries` を使用する（待機秒やバックオフは `retry` 設定に従う）。

### 5.1 フィード取得失敗
- 対象: RSS/Atom フィード URL の取得・パース失敗。
- 挙動:
  - 設定回数までリトライ。
  - リトライ後も失敗した場合、フロー全体を `FAILED` とする。

### 5.2 記事取得失敗
- 対象: 個別記事 URL の本文取得失敗。
- 挙動:
  - 設定回数までリトライ。
  - リトライ後も失敗した記事は `SKIPPED` として次の記事へ進む。

### 5.3 要約失敗
- 対象: `briefing_summary` / `one_sentence_summary` 生成失敗。
- 挙動:
  - 設定回数までリトライ。
  - リトライ後も失敗した記事は `SKIPPED`（保存しない）として次の記事へ進む。

### 5.4 S3保存失敗
- 対象: 記事 JSON の S3 アップロード失敗。
- 挙動:
  - 設定回数までリトライ。
  - リトライ後も失敗した場合、フロー全体を `FAILED` とする。

---

## 6. Prefect 実装前提（`flows/rss_ingest_flow.py`）

`flows/hello_flow.py` と同様、`@flow` デコレータを用いた単一エントリーポイントを持つ。実装時は以下タスク分割を推奨する。

1. `load_config_task`: `config.yaml` 読込・検証
2. `load_prefect_blocks_task`: S3/Ollama 接続情報 Block 読込
3. `fetch_feed_task`: フィード取得・全エントリ展開
4. `fetch_article_task`: 記事本文取得
5. `summarize_task`: `briefing_summary` と `one_sentence_summary` 生成
6. `build_json_task`: 出力 JSON 生成（`metadata` 拡張含む）
7. `store_to_s3_task`: 指定パスに1記事1ファイル保存
8. `report_task`: 成功/スキップ/失敗件数サマリ

タスクごとのリトライ設定は `config.yaml` の `retry` と整合させること。
