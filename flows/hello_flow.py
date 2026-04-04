from prefect import flow, get_run_logger


@flow
def hello_flow() -> None:
    """疎通確認のため固定メッセージをログ出力する。

    処理内容:
        Prefectランロガーを取得し、`hello` メッセージをINFOで出力する。
    入力:
        なし。
    出力:
        なし。
    例外:
        Prefect実行コンテキストに依存する例外が発生しうる。
    外部依存リソース:
        Prefect実行コンテキスト。
    """
    logger = get_run_logger()
    logger.info("hello")


if __name__ == "__main__":
    hello_flow()
