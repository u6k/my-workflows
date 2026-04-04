# my-workflows

日常生活ワークフロー集。

## 依存管理方針

- Python は **3.10 以上**をサポート対象とする。
- パッケージ管理は **uv** を利用し、依存定義は `pyproject.toml` で管理する。

## セットアップ手順

```bash
# uv が未インストールの場合のみ
curl -LsSf https://astral.sh/uv/install.sh | sh

# 仮想環境作成（Python 3.10+）
uv venv --python 3.10

# 仮想環境を有効化
source .venv/bin/activate

# 依存インストール
uv sync
```

## Prefect ダッシュボード起動手順

```bash
# プロジェクト直下で仮想環境を有効化
source .venv/bin/activate

# Prefect API サーバー + UI を起動
prefect server start
```

起動後、ブラウザで `http://127.0.0.1:4200` を開くと Prefect ダッシュボードを確認できます。

## RSS Ingest Flow の設定手順

### 1. 設定ファイルを作成

`config.example.yaml` をコピーして `config.yaml` を作成し、環境に合わせて値を編集します。

```bash
cp config.example.yaml config.yaml
```

`config.yaml` の主な項目:

- `rss_urls`: 取得対象 RSS/Atom フィード URL の配列（`http://` または `https://`）
- `retry.max_retries`: 失敗時の最大リトライ回数（整数）
- `retry.initial_delay_sec`: 初回リトライ待機秒（任意）
- `retry.backoff_multiplier`: バックオフ係数（任意）
- `storage.s3_bucket`: S3互換ストレージのバケット名
- `storage.s3_prefix`: S3 保存プレフィックス
- `target_date`: `daily-news-blog-digest-flow` で `target_date` パラメータ未指定時に使う日付（`YYYY-MM-DD`、省略時は実行日のUTC日付）
- 既存オブジェクト判定: `s3://{s3_bucket}/{s3_prefix}/{urlのmd5先頭2文字}/{urlのmd5}.json` が存在する記事は再取得せずスキップ
- 保存JSON: Trafilatura による本文抽出結果 `content` と、生HTML `raw_html` の両方を保持
- Ollama 要約: `briefing_summary` と `one_sentence_summary` を本文から生成して保存
- `ollama.request_timeout_sec`: Ollama API呼び出しタイムアウト秒（省略時120秒）
- `ollama.models.rss_ingest_flow`: `rss_ingest_flow` で使うモデル名
- `ollama.models.rss_ingest_flow_embedding`: `rss_ingest_flow` でブリーフィング要約の埋め込み生成に使うモデル名
- `ollama.models.daily_news_blog_digest_flow`: `daily-news-blog-digest-flow` で使うモデル名
- `ollama.models.daily_news_blog_digest_flow_embedding`: `daily-news-blog-digest-flow` で記事分類用埋め込みに使うモデル名
- `embeddings.sqlite_path`: RSS ingest 時にブリーフィング埋め込みを保存する SQLite ファイルパス（例: `.data/rss_embeddings.sqlite3`）
- `prefect_blocks.aws_credentials_block`: AWS Credentials Block 名
- `prefect_blocks.ollama_connection_secret_block`: Ollama 接続情報(JSON)を格納した Secret Block 名

### 2. Prefect Block / Secret を作成

`flows/rss_ingest_flow.py` は、起動時に `config.yaml` の `prefect_blocks` で指定した名前の Block/Secret を読み込みます。事前に Prefect UI で以下を作成してください。

1. Prefect サーバーを起動し、`http://127.0.0.1:4200` を開く
2. **Blocks** 画面で AWS Credentials Block を作成し、名前を `prefect_blocks.aws_credentials_block` の値と一致させる
3. **Blocks** 画面で Secret Block を1つ作成し、値は JSON 形式で保存する（例）
   - `{"base_url":"http://localhost:11434"}`
4. Secret Block の名前を `prefect_blocks.ollama_connection_secret_block` と一致させる

> 注意: フローは Block 名・キー名のみログに出力し、Secret の値そのものはログ出力しません。

### 3. フロー実行

```bash
source .venv/bin/activate
python flows/rss_ingest_flow.py
```

### 4. SQLite 埋め込みストアのセットアップ（RSS ingest）

`rss_ingest_flow` は、記事ごとの `briefing_summary` を埋め込み化し、`embeddings.sqlite_path` で指定した SQLite に保存します。  
SQLite ファイルはフロー実行時に自動作成されますが、事前に場所を固定したい場合は次の準備をしておくと安全です。

```bash
# 例: デフォルト保存先ディレクトリを事前作成
mkdir -p .data

# （任意）空のSQLiteファイルを事前作成
python - <<'PY'
import sqlite3
conn = sqlite3.connect(".data/rss_embeddings.sqlite3")
conn.close()
print("initialized .data/rss_embeddings.sqlite3")
PY
```

保存テーブルは `article_embeddings` です。`article_id` を主キーとして upsert されるため、同じ記事を再処理しても最新ベクトルで更新されます。  
テーブルには作成時刻 `embedding_created_at_utc`（UTC ISO8601）と記事公開時刻 `article_published_timestamp`（取得できた場合）が格納されます。
あわせて、`briefing_summary` と `one_sentence_summary` も保存するため、後段でS3本文を再読込せず SQLite 側の要約データを参照できます。

## Daily News Blog Digest Flow の設定と実行

`flows/daily_news_blog_digest_flow.py` は、`target_date` 引数が未指定の場合に `config.yaml` の `target_date` を参照します。`config.yaml` に `target_date` が無い場合は、実行日の UTC 日付が使われます。

```bash
source .venv/bin/activate
python flows/daily_news_blog_digest_flow.py
```

### 指定日をコマンドプロンプトから手動実行で渡す

`--target-date` に `YYYY-MM-DD` を渡すと、指定日で実行できます。

```bash
source .venv/bin/activate
python flows/daily_news_blog_digest_flow.py --target-date 2026-04-02
```

必要に応じて設定ファイルも変更できます。

```bash
python flows/daily_news_blog_digest_flow.py --target-date 2026-04-02 --config-path ./config.yaml
```

### Prefect Deployment から手動実行時にパラメーターを渡す

Prefect の `Parameters` は、**フロー関数の引数**（このフローでは `target_date`, `config_path`）に対応します。  
このため、性質としては次の2種類があります。

- **定義（固定の受け口）**: `@flow` 関数シグネチャで定義される  
  - 例: `def daily_news_blog_digest_flow(target_date: str | None = None, config_path: str = "config.yaml")`
- **実行時の値（毎回変更可能）**: 手動実行時に UI / CLI で指定する  
  - 例: `target_date=2026-04-02`

つまり、`Parameters` 自体は「Prefect 画面で自由に新規追加する項目」ではなく、**コード側で定義された引数に対して、実行時に値を与える仕組み**です。  
（補足: deployment 作成時にデフォルト値を持たせることは可能で、手動実行時に上書きできます）

1. まず deployment を作成します（未作成の場合）。

```bash
source .venv/bin/activate
prefect deploy flows/daily_news_blog_digest_flow.py:daily_news_blog_digest_flow -n daily-news-blog-digest-manual
```

2. コマンドで手動実行する場合は、`--params` で JSON を渡します。

```bash
prefect deployment run "daily-news-blog-digest-flow/daily-news-blog-digest-manual" \
  --params '{"target_date":"2026-04-02"}'
```

3. Prefect UI で手動実行する場合は、**Deployments** 画面から対象 deployment を開き、**Run** を押して Parameters の `target_date` に `2026-04-02` のように入力して実行します。

#### 使い分けの目安

- 「通常は当日UTCで実行、たまに過去日を再実行したい」  
  - コードのデフォルト（`target_date=None`）を使い、必要なときだけ Run 画面で `target_date` を入力
- 「この deployment は常に前日分を対象にしたい」  
  - deployment 側のデフォルト Parameters を設定し、例外時だけ手動実行で上書き
