from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

import yaml
from botocore.config import Config as BotocoreConfig
from prefect.blocks.system import Secret
from prefect_aws.credentials import AwsCredentials

REQUIRED_OLLAMA_CONNECTION_KEYS = (
    "base_url",
)


def load_yaml_config(config_path: str) -> dict[str, Any]:
    """YAML設定ファイルを読み込み、辞書形式の設定として返す。

    処理内容:
        指定パスの存在確認を行い、UTF-8で読み込んだ内容を `yaml.safe_load` で
        パースし、トップレベルが辞書であることを検証する。
    入力:
        config_path: YAMLファイルパス。
    出力:
        dict[str, Any]: 読み込まれた設定辞書。
    例外:
        ValueError: ファイルが存在しない、またはトップレベルが辞書でない場合。
    外部依存リソース:
        ローカルファイルシステム。
    """
    path = Path(config_path)
    if not path.exists():
        raise ValueError(f"config file is not found: {config_path}")

    with path.open("r", encoding="utf-8") as fp:
        config = yaml.safe_load(fp)

    if not isinstance(config, dict):
        raise ValueError("config must be a YAML object")

    return config


def normalize_botocore_config(raw_config: Any) -> BotocoreConfig | None:
    """S3クライアント設定値を `botocore.config.Config` に正規化する。

    処理内容:
        None / Config / dict / JSON文字列を受け取り、JSON文字列は辞書へ変換したうえで
        `BotocoreConfig` を生成する。空文字は未指定として `None` を返す。
    入力:
        raw_config: 生の設定値。
    出力:
        BotocoreConfig | None: 正規化済み設定、または未指定時の None。
    例外:
        ValueError: JSON不正、または許容型以外の値が渡された場合。
    外部依存リソース:
        なし。
    """
    if raw_config is None:
        return None

    if isinstance(raw_config, BotocoreConfig):
        return raw_config

    normalized_config = raw_config
    if isinstance(raw_config, str):
        normalized_config = raw_config.strip()
        if normalized_config == "":
            return None
        try:
            normalized_config = json.loads(normalized_config)
        except json.JSONDecodeError as exc:
            raise ValueError("aws_client_parameters.config must be valid JSON when provided as a string") from exc

    if not isinstance(normalized_config, dict):
        raise ValueError("aws_client_parameters.config must be a dict, JSON string, or botocore Config")

    return BotocoreConfig(**normalized_config)


def get_aws_client_parameters(aws_credentials: AwsCredentials) -> dict[str, Any]:
    """AwsCredentials から boto3 client 用の上書きパラメータを抽出する。

    処理内容:
        `aws_client_parameters` を参照し、`get_params_override` があれば優先利用する。
        利用できない場合は dict または属性から `endpoint_url` と `config` を抽出する。
    入力:
        aws_credentials: Prefect の AwsCredentials ブロックインスタンス。
    出力:
        dict[str, Any]: boto3 `client()` に渡せる上書き引数。
    例外:
        なし。
    外部依存リソース:
        Prefectブロックオブジェクト属性。
    """
    aws_client_parameters = getattr(aws_credentials, "aws_client_parameters", None)
    if aws_client_parameters is None:
        return {}

    if hasattr(aws_client_parameters, "get_params_override"):
        params_override = aws_client_parameters.get_params_override()
        if isinstance(params_override, dict):
            return params_override

    if isinstance(aws_client_parameters, dict):
        return aws_client_parameters

    extracted: dict[str, Any] = {}
    for key in ("endpoint_url", "config"):
        value = getattr(aws_client_parameters, key, None)
        if value is not None:
            if key == "endpoint_url" and not isinstance(value, str):
                continue
            if key == "config" and not isinstance(value, (str, dict, BotocoreConfig)):
                continue
            extracted[key] = value
    return extracted


def create_s3_client(aws_credentials: AwsCredentials) -> Any:
    """AwsCredentials の情報を使って設定済み S3 クライアントを作成する。

    処理内容:
        AWSクライアント上書きパラメータを取得し、`endpoint_url` と `config` を反映して
        boto3 session の `client("s3")` を生成する。
    入力:
        aws_credentials: Prefect の AwsCredentials ブロックインスタンス。
    出力:
        Any: boto3 S3クライアント。
    例外:
        ValueError: `config` の正規化に失敗した場合。
    外部依存リソース:
        Prefectブロック、AWS SDK（boto3/botocore）。
    """
    client_kwargs: dict[str, Any] = {}
    client_parameters = get_aws_client_parameters(aws_credentials)

    endpoint_url = client_parameters.get("endpoint_url")
    if isinstance(endpoint_url, str) and endpoint_url:
        client_kwargs["endpoint_url"] = endpoint_url

    config = normalize_botocore_config(client_parameters.get("config"))
    if config is not None:
        client_kwargs["config"] = config

    return aws_credentials.get_boto3_session().client("s3", **client_kwargs)


def validate_ollama_connection(ollama_connection: Any) -> dict[str, str]:
    """Ollama接続情報の必須キーを検証して利用可能な辞書を返す。

    処理内容:
        入力が辞書であることを確認し、`base_url` が非空文字列として
        含まれているかを検証して必要キーのみ返す。
    入力:
        ollama_connection: Secretから取得した接続情報オブジェクト。
    出力:
        dict[str, str]: `base_url` を含む接続辞書。
    例外:
        ValueError: 辞書でない、または必須キーが不足/不正な場合。
    外部依存リソース:
        なし。
    """
    if not isinstance(ollama_connection, dict):
        raise ValueError("Ollama Secret block value must be a dict")

    missing_ollama_keys = [
        key for key in REQUIRED_OLLAMA_CONNECTION_KEYS if not isinstance(ollama_connection.get(key), str) or not ollama_connection[key]
    ]
    if missing_ollama_keys:
        raise ValueError(f"Ollama Secret JSON is missing required keys: {', '.join(missing_ollama_keys)}")

    return {
        "base_url": ollama_connection["base_url"],
    }


def build_ollama_connection(ollama_secret: dict[str, str], model: str) -> dict[str, str]:
    """Secretの接続情報とモデル名を結合し、生成API呼び出し用設定を返す。

    処理内容:
        検証済みのSecret接続情報にモデル名を追加し、`base_url` と `model` を
        そろえた辞書を生成する。
    入力:
        ollama_secret: `base_url` を含む検証済み接続辞書。
        model: フローごとに設定されたモデル名。
    出力:
        dict[str, str]: `base_url` と `model` を含む接続辞書。
    例外:
        ValueError: モデル名が空または文字列でない場合。
    外部依存リソース:
        なし。
    """
    if not isinstance(model, str) or not model:
        raise ValueError("Ollama model must be a non-empty string")
    return {
        "base_url": ollama_secret["base_url"],
        "model": model,
    }


def load_ollama_connection_secret(secret_block_name: str, logger: logging.Logger | None = None) -> dict[str, str]:
    """Prefect SecretブロックからOllama接続情報を読み出して検証する。

    処理内容:
        指定名の Secret ブロックをロードして値を取得し、必要キーを検証したうえで
        接続辞書として返す。ロガーが指定されていれば読み込みログを出力する。
    入力:
        secret_block_name: Secretブロック名。
        logger: 任意のロガー。
    出力:
        dict[str, str]: 検証済みOllama接続辞書。
    例外:
        ValueError: Secret値の形式が不正な場合。
    外部依存リソース:
        Prefect Secretブロックストレージ。
    """
    ollama_secret_value = Secret.load(secret_block_name).get()
    if logger is not None:
        logger.info("Prefect secret loaded: key=ollama_connection_block name=%s", secret_block_name)
    return validate_ollama_connection(ollama_secret_value)


def invoke_ollama_generate(
    ollama_connection: dict[str, str],
    prompt: str,
    timeout_sec: int = 120,
    response_format: str | dict[str, Any] | None = None,
    logger: logging.Logger | None = None,
) -> str:
    """Ollama の `/api/generate` を呼び出して生成テキストを返す。

    処理内容:
        モデル名・プロンプト・フォーマットからJSONペイロードを作成し、HTTP POSTで
        Ollamaへ送信する。レスポンス本文をJSONとして解析し、`response` を抽出する。
    入力:
        ollama_connection: `base_url` と `model` を含む接続設定。
        prompt: 生成プロンプト文字列。
        timeout_sec: HTTPタイムアウト秒。
        response_format: Ollamaの `format` パラメータ（例: `"json"` やJSONスキーマdict）。
        logger: 任意ロガー。
    出力:
        str: 生成されたテキスト（前後空白除去済み）。
    例外:
        URLError/HTTPError: 通信失敗やHTTPエラー時。
        JSONDecodeError: レスポンスJSONの解析に失敗した場合。
    外部依存リソース:
        Ollama HTTP API。
    """
    if logger is not None:
        logger.info("ollama prompt: %s", prompt)

    payload: dict[str, Any] = {
        "model": ollama_connection["model"],
        "prompt": prompt,
        "stream": False,
    }
    if response_format is not None:
        payload["format"] = response_format

    request = Request(
        f"{ollama_connection['base_url'].rstrip('/')}/api/generate",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=timeout_sec) as response:
        body = response.read().decode("utf-8")

    response_json = json.loads(body)
    response_text = str(response_json.get("response", "")).strip()
    if logger is not None:
        logger.info("ollama response: %s", response_text)
    return response_text


def invoke_ollama_embeddings(
    ollama_connection: dict[str, str],
    text: str,
    timeout_sec: int = 120,
    logger: logging.Logger | None = None,
) -> list[float]:
    """Ollama の `/api/embeddings` を呼び出して埋め込みベクトルを返す。

    処理内容:
        モデル名と入力テキストからJSONペイロードを作成し、HTTP POSTで
        Ollamaへ送信する。レスポンス本文をJSONとして解析し、`embedding`
        を抽出して float 配列として返す。
    入力:
        ollama_connection: `base_url` と `model` を含む接続設定。
        text: 埋め込み対象テキスト。
        timeout_sec: HTTPタイムアウト秒。
        logger: 任意ロガー。
    出力:
        list[float]: 埋め込みベクトル。
    例外:
        ValueError: レスポンスに embedding が存在しない、または不正形式の場合。
        URLError/HTTPError: 通信失敗やHTTPエラー時。
        JSONDecodeError: レスポンスJSONの解析に失敗した場合。
    外部依存リソース:
        Ollama HTTP API。
    """
    payload = {
        "model": ollama_connection["model"],
        "prompt": text,
    }
    request = Request(
        f"{ollama_connection['base_url'].rstrip('/')}/api/embeddings",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urlopen(request, timeout=timeout_sec) as response:
        body = response.read().decode("utf-8")

    response_json = json.loads(body)
    embedding = response_json.get("embedding")
    if not isinstance(embedding, list) or not embedding:
        raise ValueError("Ollama embeddings response must include non-empty embedding array")
    vector = [float(value) for value in embedding]
    if logger is not None:
        logger.info("ollama embedding size=%d", len(vector))
    return vector
