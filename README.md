# my-workflows

日常生活ワークフロー集。

## 依存管理方針

- Python は **3.13** を標準利用バージョンとする。
- パッケージ管理は **uv** を利用し、依存定義は `pyproject.toml` で管理する。

## セットアップ手順

```bash
# uv が未インストールの場合のみ
curl -LsSf https://astral.sh/uv/install.sh | sh

# 仮想環境作成（Python 3.13）
uv venv --python 3.13

# 仮想環境を有効化
source .venv/bin/activate

# 依存インストール
uv sync
```

## Prefect ダッシュボード起動手順

```bash
# Prefect API サーバー + UI を起動
uv run prefect server start
```

起動後、ブラウザで `http://127.0.0.1:4200` を開くと Prefect ダッシュボードを確認できます。

## ChromaDB サーバー起動手順

```bash
# ChromaDB HTTP サーバーを起動
uv run chroma run --host 127.0.0.1 --port 8000
```

起動後、`config.yaml` / `config.example.yaml` の `chroma.host` と `chroma.port` を同じ値に合わせてください。  
ローカルでは `127.0.0.1:8000` を前提にしておくと、viewer などの周辺ツールからも接続しやすくなります。

## ローカル開発での起動順

1. `uv sync`
2. `uv run prefect server start`
3. 別ターミナルで `uv run chroma run --host 127.0.0.1 --port 8000`
4. 必要に応じて `config.yaml` の `prefect_blocks` と `chroma` 設定を確認
5. `uv run python flows/rss_ingest_flow.py`
6. `uv run python flows/daily_news_blog_digest_flow.py --target-date 2026-04-02`

## RSS Ingest Flow の設定手順

### 1. 設定ファイルを作成

`config.example.yaml` をコピーして `config.yaml` を作成し、環境に合わせて値を編集します。

```bash
cp config.example.yaml config.yaml
```

`config.yaml` の主な項目:

- `rss_ingest.rss_urls`: 取得対象 RSS/Atom フィード URL の配列（`http://` または `https://`）
- `retry.max_retries`: 失敗時の最大リトライ回数（整数）
- `retry.initial_delay_sec`: 初回リトライ待機秒（任意）
- `retry.backoff_multiplier`: バックオフ係数（任意）
- `rss_ingest.s3_bucket`: S3互換ストレージのバケット名
- `rss_ingest.s3_prefix`: S3 保存プレフィックス
- 既存オブジェクト判定: `s3://{s3_bucket}/{s3_prefix}/{urlのmd5先頭2文字}/{urlのmd5}.json` が存在する記事は再取得せずスキップ
- 保存JSON: 通常記事は Trafilatura による Markdown 本文 `content` と生HTML `raw_html` を保持し、分割記事は「次のページ」を最大10ページまで辿って全ページ分を結合する。YouTube は `markitdown` の Markdown 本文 `content` を保持する
- YouTube URL は `markitdown[youtube-transcription]` の Python API で処理し、`youtube_transcript_languages=["ja", "en"]` を指定して日本語字幕を優先取得する
- YouTube URL の `content` には `markitdown` が返す Markdown 全文を保持し、`### Transcript` セクションを含む動画文字起こしを後続の要約に利用する
- YouTube URL で `### Transcript` セクションを取得できない場合は、説明文やメタデータだけで成功扱いにせずエラーとして扱う
- YouTube 文字起こし処理は Python API 専用で、CLI フォールバックは行わない
- LiteLLM 要約: `briefing_summary` と `one_sentence_summary` を本文から生成して保存
- `llm.request_timeout_sec`: LLM API呼び出しタイムアウト秒（省略時120秒）
- `chroma.host`: 別プロセスで起動した ChromaDB サーバーのホスト名または IP
- `chroma.port`: ChromaDB サーバーのポート番号
- `chroma.ssl`: ChromaDB サーバーへ HTTPS で接続する場合は `true`
- `chroma.collection_name`: RSS ingest と日次ダイジェストで共有する Chroma collection 名
- `rss_ingest.llm_model`: `rss_ingest_flow` で使うモデル名
- `daily_news_blog_digest.s3_bucket`: 日次ダイジェスト成果物の保存バケット
- `daily_news_blog_digest.s3_prefix`: 日次ダイジェスト成果物の保存プレフィックス
- `daily_news_blog_digest.llm_model`: `daily-news-blog-digest-flow` で使うモデル名
- `prefect_blocks.aws_credentials_block`: AWS Credentials Block 名
- `prefect_blocks.llm_connection_block`: 生成用 LiteLLM 接続情報(JSON)を格納した Secret Block 名

`config.yaml` の例:

```yaml
retry:
  max_retries: 3
  initial_delay_sec: 2
  backoff_multiplier: 2

llm:
  request_timeout_sec: 120

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
  llm_model: ollama/qwen3.5:0.8b

daily_news_blog_digest:
  s3_bucket: news-bucket
  s3_prefix: daily-digest
  llm_model: ollama/qwen3.5:0.8b
```

### 2. Prefect Block / Secret を作成

`flows/rss_ingest_flow.py` は、起動時に `config.yaml` の `prefect_blocks` で指定した名前の Block/Secret を読み込みます。事前に Prefect UI で以下を作成してください。

1. Prefect サーバーを起動し、`http://127.0.0.1:4200` を開く
2. **Blocks** 画面で AWS Credentials Block を作成し、名前を `prefect_blocks.aws_credentials_block` の値と一致させる
3. **Blocks** 画面で Secret Block を作成し、値は JSON 形式で保存する
   - Ollama 例: `{"provider":"ollama","api_base":"http://localhost:11434"}`
   - OpenAI 例: `{"api_key":"<OPENAI_API_KEY>"}`
   - Anthropic 例: `{"api_key":"<ANTHROPIC_API_KEY>"}`
4. 生成用 Secret Block の名前を `prefect_blocks.llm_connection_block` と一致させる

モデル指定は LiteLLM 形式です。たとえば `ollama/qwen3.5:0.8b`、`openai/gpt-4o-mini`、`anthropic/claude-3-5-sonnet-latest` のように設定します。

#### Ollama を使う場合の例

Prefect Secret Block (`llm-connection`) の値:

```json
{"provider":"ollama","api_base":"http://localhost:11434"}
```

`config.yaml` の該当部分:

```yaml
prefect_blocks:
  llm_connection_block: llm-connection

llm:
  request_timeout_sec: 120

rss_ingest:
  llm_model: ollama/qwen3.5:0.8b

daily_news_blog_digest:
  llm_model: ollama/qwen3.5:0.8b
```

#### OpenAI を使う場合の例

Prefect Secret Block (`llm-connection`) の値:

```json
{"api_key":"<OPENAI_API_KEY>"}
```

`config.yaml` の該当部分:

```yaml
prefect_blocks:
  llm_connection_block: llm-connection

llm:
  request_timeout_sec: 120

rss_ingest:
  llm_model: openai/gpt-4o-mini

daily_news_blog_digest:
  llm_model: openai/gpt-4o-mini
```

#### Anthropic を使う場合の例

Prefect Secret Block (`llm-connection`) の値:

```json
{"api_key":"<ANTHROPIC_API_KEY>"}
```

`config.yaml` の該当部分:

```yaml
prefect_blocks:
  llm_connection_block: llm-connection

llm:
  request_timeout_sec: 120

rss_ingest:
  llm_model: anthropic/claude-3-5-sonnet-latest

daily_news_blog_digest:
  llm_model: anthropic/claude-3-5-sonnet-latest
```

### 3. フロー実行

```bash
uv run python flows/rss_ingest_flow.py
```

### 4. ChromaDB サーバーのセットアップ（RSS ingest / digest 共通）

`rss_ingest_flow` と `daily_news_blog_digest_flow` は、別プロセスで起動した ChromaDB サーバーへ HTTP 接続します。  
アプリ側は `config.yaml` の `chroma.host/port/ssl/collection_name` を読み、`HttpClient` 経由で同じ collection を参照します。

```bash
uv sync
```

ローカルで動かす場合は、たとえば `127.0.0.1:8000` で Chroma サーバーを起動し、その値を `config.yaml` に設定してください。viewer などの周辺ツールも同じサーバーへ接続させる前提です。

保存レコードは `article_id` を主キーとして upsert されるため、同じ記事を再処理しても最新データで更新されます。  
埋め込みベクトルは Chroma のデフォルト embedding function で生成し、metadata には `article_url/title/fetch_timestamp/fetch_timestamp_epoch/one_sentence_summary/embedding_timestamp/metadata_json` を保持します。日次ダイジェストは `fetch_timestamp_epoch` の数値範囲条件で対象日を読み出します。

Python から Chroma に接続して、`embeddings` を含めて collection 全件を取得する例:

```python
import chromadb

client = chromadb.HttpClient(host="127.0.0.1", port=8000, ssl=False)
collection = client.get_collection("rss-articles")

result = collection.get(include=["documents", "metadatas", "embeddings"])

print("count:", len(result["ids"]))
print("first id:", result["ids"][0] if result["ids"] else None)
print("first metadata:", result["metadatas"][0] if result["metadatas"] else None)
print("first embedding dim:", len(result["embeddings"][0]) if result["embeddings"] else 0)
```

`embeddings` は `include` に明示しないと返らないため、viewer で見えない場合でも `collection.get(include=["documents", "metadatas", "embeddings"])` を使って確認してください。

## Daily News Blog Digest Flow の設定と実行

`flows/daily_news_blog_digest_flow.py` は、`target_date` 引数を**必須**で受け取ります。`config.yaml` から `target_date` は読みません。

```bash
uv run python flows/daily_news_blog_digest_flow.py --target-date 2026-04-02
```

### 指定日をコマンドプロンプトから手動実行で渡す

`--target-date` に `YYYY-MM-DD` を渡すと、指定日で実行できます。

```bash
uv run python flows/daily_news_blog_digest_flow.py --target-date 2026-04-02
```

必要に応じて設定ファイルも変更できます。

```bash
uv run python flows/daily_news_blog_digest_flow.py --target-date 2026-04-02 --config-path ./config.yaml
```

## 単一URL要約フローの実行

`fetch_summarize_url_flow` は、1つのURLだけを取得して `briefing_summary` と `one_sentence_summary` を返す独立フローです。内部では通常記事の `content` を Markdown 形式で抽出してから要約します。

```bash
uv run python flows/fetch_summarize_url_flow.py --article-url "https://example.com/article" --config-path config.yaml
```

YouTube URL を単一要約フローへ渡す場合も同じフローを利用できます。YouTube のときは `markitdown[youtube-transcription]` が必要で、字幕が取得できない動画は失敗として返ります。

標準出力には次の項目だけが JSON で含まれます。

- `id`
- `title`
- `one_sentence_summary`
- `briefing_summary`

注意:

- `article_url` は `http://` または `https://` で始まる必要があります
- このフローでも `config.yaml` と Prefect Block の参照は必要です
- 記事の単独要約では S3 保存や Chroma upsert は行いません

Prefect Deployment として使う場合は、対象フローに `flows/rss_ingest_flow.py:fetch_summarize_url_flow` を指定します。

```bash
uv run prefect deploy flows/rss_ingest_flow.py:fetch_summarize_url_flow -n fetch-summarize-url
```

手動実行例:

```bash
uv run prefect deployment run "fetch-summarize-url-flow/fetch-summarize-url" --params "{\"article_url\":\"https://example.com/article\"}"
```

### Prefect Deployment から手動実行時にパラメーターを渡す

Prefect の `Parameters` は、**フロー関数の引数**（このフローでは `target_date`, `config_path`）に対応します。  
このため、性質としては次の2種類があります。

- **定義（固定の受け口）**: `@flow` 関数シグネチャで定義される  
  - 例: `def daily_news_blog_digest_flow(target_date: str, config_path: str = "config.yaml")`
- **実行時の値（毎回変更可能）**: 手動実行時に UI / CLI で指定する  
  - 例: `target_date=2026-04-02`

つまり、`Parameters` 自体は「Prefect 画面で自由に新規追加する項目」ではなく、**コード側で定義された引数に対して、実行時に値を与える仕組み**です。  
（補足: deployment 作成時にデフォルト値を持たせることは可能で、手動実行時に上書きできます）

1. まず deployment を作成します（未作成の場合）。

```bash
uv run prefect deploy flows/daily_news_blog_digest_flow.py:daily_news_blog_digest_flow -n daily-news-blog-digest-manual
```

2. コマンドで手動実行する場合は、`--params` で JSON を渡します。

```bash
uv run prefect deployment run "daily-news-blog-digest-flow/daily-news-blog-digest-manual" \
  --params '{"target_date":"2026-04-02"}'
```

3. Prefect UI で手動実行する場合は、**Deployments** 画面から対象 deployment を開き、**Run** を押して Parameters の `target_date` に `2026-04-02` のように入力して実行します。

#### 使い分けの目安

- 「通常は当日UTCで実行、たまに過去日を再実行したい」  
  - スケジューラ側で毎日の `target_date` を明示設定し、必要なときだけ Run 画面で上書き
- 「この deployment は常に前日分を対象にしたい」  
  - deployment 側のデフォルト Parameters を設定し、例外時だけ手動実行で上書き

## 開発ルール

- Docstring の書式は [docs/docstring_style.md](docs/docstring_style.md) に統一ルールを定義しています。
- 追加・変更する関数、メソッド、クラスの docstring は上記ルールに従い、少なくとも `目的` `処理内容` `入力` `出力` `例外` を必ず記載してください。
