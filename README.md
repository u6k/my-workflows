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
