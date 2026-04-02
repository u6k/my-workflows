from prefect import flow


@flow
def hello_flow() -> None:
    print("hello")


if __name__ == "__main__":
    hello_flow()
