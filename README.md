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
- `prefect_blocks.aws_credentials_block`: AWS Credentials Block 名
- `prefect_blocks.ollama_connection_secret_block`: Ollama 接続情報(JSON)を格納した Secret Block 名

### 2. Prefect Block / Secret を作成

`flows/rss_ingest_flow.py` は、起動時に `config.yaml` の `prefect_blocks` で指定した名前の Block/Secret を読み込みます。事前に Prefect UI で以下を作成してください。

1. Prefect サーバーを起動し、`http://127.0.0.1:4200` を開く
2. **Blocks** 画面で AWS Credentials Block を作成し、名前を `prefect_blocks.aws_credentials_block` の値と一致させる
3. **Blocks** 画面で Secret Block を1つ作成し、値は JSON 形式で保存する（例）
   - `{"base_url":"http://localhost:11434","model":"llama3.1:8b"}`
4. Secret Block の名前を `prefect_blocks.ollama_connection_secret_block` と一致させる

> 注意: フローは Block 名・キー名のみログに出力し、Secret の値そのものはログ出力しません。

### 3. フロー実行

```bash
source .venv/bin/activate
python flows/rss_ingest_flow.py
```

## Daily News Blog Digest Flow の設定と実行

`flows/daily_news_blog_digest_flow.py` は、`target_date` 引数が未指定の場合に `config.yaml` の `target_date` を参照します。`config.yaml` に `target_date` が無い場合は、実行日の UTC 日付が使われます。

```bash
source .venv/bin/activate
python flows/daily_news_blog_digest_flow.py
```
