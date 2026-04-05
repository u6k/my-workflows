# daily-news-blog-digest-flow 仕様書

## 1. 目的

指定日（例: 2026-04-02）のニュース記事データをS3互換ストレージから取得し、記事群を整理・要約して、**ブログ記事風の読み物**として出力する。

- 主目的: ニュースを読みやすいブログ形式で要約・再構成すること
- 手段: カテゴリー分類（クラスタリング）は、ブログ構成を作るための中間処理として利用する

---

## 2. フロー概要

- **Flow Name**: `daily-news-blog-digest-flow`
- **想定実行タイミング**: 日次バッチ（手動実行も可）
- **入力**: 指定日 + S3互換ストレージ上の記事JSON
- **出力**:
  - 構造化結果（カテゴリ、記事割当、メタ情報）
  - 最終成果物 `blog_summary_<date>.md`

---

## 3. 前提条件

1. S3互換ストレージへ接続可能であること
2. 指定プレフィックス配下に記事JSONが保存されていること
3. 記事JSONが最低限 `id`, `title`, `content`, `published_at` を持つこと
4. 要約に使用するLLM/埋め込みモデルが利用可能であること

---

## 4. 入出力仕様

## 4.1 入力パラメータ（Flow Parameters）

- `target_date` (string, `YYYY-MM-DD`)
  - 取得対象日のニュース日付
- `bucket` (string)
- `prefix` (string)
- `endpoint_url` (string)
- `region` (string, optional)
- `max_articles` (int, optional)
- `clustering_algorithm` (string: `hdbscan` or `kmeans`, default: `hdbscan`)
- `summary_style` (string, default: `blog-ja`)

### 4.1.1 保存先設定（config.yaml）

日次ダイジェスト成果物（`categories` / `blog`）は、HTMLキャッシュと別ロケーションに保存できる。

- 既定: `storage.s3_bucket` + `storage.s3_prefix`
- 上書き（任意）:
  - `storage.daily_digest_s3_bucket`
  - `storage.daily_digest_s3_prefix`

未指定時は既定値を使用し、指定時のみ日次ダイジェスト成果物の保存先が切り替わる。

## 4.2 入力記事JSON（例）

```json
{
  "id": "news-20260402-001",
  "title": "記事タイトル",
  "content": "本文...",
  "published_at": "2026-04-02T08:30:00Z",
  "url": "https://example.com/news/1",
  "source": "Example News"
}
```

## 4.3 出力物

1. `outputs/<target_date>/categories.json`
2. `outputs/<target_date>/article_assignments.json`
3. `outputs/<target_date>/run_metadata.json`
4. `outputs/<target_date>/blog_summary_<target_date>.md`

---

## 5. 処理ステップ（Prefect Tasks想定）

## 5.1 `load_config`

- 接続情報・パラメータを統合
- 必須項目不足時はフロー失敗

## 5.2 `fetch_articles_from_s3`

- S3互換ストレージから `bucket/prefix` を列挙
- `target_date` に該当する記事を抽出
- `max_articles` があれば件数制限

## 5.3 `validate_and_normalize_articles`

- 必須項目の検証
- 欠損・不正データを除外して件数を記録
- 本文正規化（空白・改行・制御文字など）

## 5.4 `build_embeddings`

- 記事ごとに埋め込みベクトル生成
- 入力テキストは `title + content`（長文は上限トークンで切り詰め）

## 5.5 `cluster_articles_for_structure`

- 埋め込みをクラスタリング
- 各記事にクラスタID付与
- 外れ値は `misc` クラスタへ寄せる（初期方針）

## 5.6 `generate_category_labels`

- 各クラスタの代表語を抽出
- 人が読めるカテゴリ名に整形
- 出力例: `国内政治`, `生成AI`, `マーケット動向`

## 5.7 `summarize_each_category`

- カテゴリごとに記事群を要約
- 要約に含める要素:
  - このカテゴリで何が起きたか
  - 背景・文脈
  - 注目ポイント

## 5.8 `compose_blog_style_digest`

- カテゴリ要約を統合し、ブログ記事風Markdownを生成
- 構成テンプレート:
  1. タイトル
  2. 導入（その日の全体像）
  3. カテゴリ別セクション
  4. まとめ

## 5.9 `persist_outputs`

- JSON成果物とMarkdown成果物を保存
- 保存先はローカルまたはS3互換ストレージ（運用設定で切替）

## 5.10 `emit_run_report`

- 処理件数、除外件数、失敗件数、処理時間を出力
- `run_metadata.json` に記録

---

## 6. エラーハンドリング

- S3接続失敗: 指数バックオフでリトライ
- JSONパース失敗: 当該ファイルをスキップしてログ化
- 埋め込み失敗: リトライ後失敗なら除外
- 要約失敗: 抽出型フォールバック（見出し+先頭要点）

---

## 7. 品質要件

- 対象記事の90%以上をブログ本文に反映
- カテゴリ要約は重複記述を最小化
- 同一入力で大きく構成が崩れない再現性
- 実行ログで失敗要因を追跡可能

---

## 8. ブログ出力仕様（Markdown）

`blog_summary_<target_date>.md` の最小構成:

```md
# Daily News Digest (2026-04-02)

## 今日の全体像
- 1〜2段落で全体トレンド

## カテゴリ1: 生成AI
- カテゴリ要約
- 代表記事

## カテゴリ2: 経済・マーケット
- カテゴリ要約
- 代表記事

## まとめ
- 明日以降の注目点
```

---

## 9. 受け入れ基準（Acceptance Criteria）

1. `target_date` を指定してフローを実行できる
2. 指定日の記事のみを取得できる
3. カテゴリ分類を経由してカテゴリ要約を生成できる
4. ブログ記事風Markdownを出力できる
5. 実行メタデータと中間成果物を保存できる

---

## 10. 将来拡張

- カテゴリ名の手動編集・固定辞書化
- 前日比較（増減トピック）
- 投稿先（CMS / Notion / Slack）への自動配信
- 重要度スコアでセクション順を最適化
