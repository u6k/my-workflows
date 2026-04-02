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
- `storage.s3_prefix`: S3 保存プレフィックス
- `prefect_blocks.s3_credentials_block`: S3 接続情報 Block 名
- `prefect_blocks.ollama_base_url_block`: Ollama Base URL を格納した Secret Block 名
- `prefect_blocks.ollama_model_block`: Ollama モデル名を格納した Secret Block 名

### 2. Prefect Block / Secret を作成

`flows/rss_ingest_flow.py` は、起動時に `config.yaml` の `prefect_blocks` で指定した名前の Block/Secret を読み込みます。事前に Prefect UI で以下を作成してください。

1. Prefect サーバーを起動し、`http://127.0.0.1:4200` を開く
2. **Blocks** 画面で S3 接続用 Block を作成し、名前を `prefect_blocks.s3_credentials_block` の値と一致させる
3. **Blocks** 画面で Secret Block を2つ作成する
   - Ollama Base URL 用（例: `http://localhost:11434`）
   - Ollama モデル名用（例: `llama3.1:8b`）
4. Secret Block の名前をそれぞれ `prefect_blocks.ollama_base_url_block` / `prefect_blocks.ollama_model_block` と一致させる

> 注意: フローは Block 名・キー名のみログに出力し、Secret の値そのものはログ出力しません。

### 3. フロー実行

```bash
source .venv/bin/activate
python flows/rss_ingest_flow.py
```
