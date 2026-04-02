from prefect import flow, get_run_logger


@flow
def hello_flow() -> None:
    logger = get_run_logger()
    logger.info("hello")


if __name__ == "__main__":
    hello_flow()
