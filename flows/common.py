from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import yaml
from botocore.config import Config as BotocoreConfig
from prefect.blocks.system import Secret
from prefect_aws.credentials import AwsCredentials

SUPPORTED_LLM_CONNECTION_KEYS = (
    "provider",
    "api_base",
    "api_key",
    "api_version",
    "organization",
    "extra_headers",
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


def validate_llm_connection(llm_connection: Any) -> dict[str, Any]:
    """LiteLLM向け接続情報を検証して利用可能な辞書を返す。

    処理内容:
        入力が辞書であることを確認し、LiteLLMへ渡せる接続設定のみを抽出する。
        既存互換のため `base_url` は `api_base` として受け付ける。
        `api_base` のみ指定されている場合は Ollama を推定プロバイダーとして扱う。
    入力:
        llm_connection: Secretから取得した接続情報オブジェクト。
    出力:
        dict[str, Any]: LiteLLMへ渡せる接続辞書。
    例外:
        ValueError: 辞書でない、または接続設定が不正な場合。
    外部依存リソース:
        なし。
    """
    if not isinstance(llm_connection, dict):
        raise ValueError("LLM Secret block value must be a dict")

    normalized: dict[str, Any] = {}
    api_base = llm_connection.get("api_base")
    if api_base is None:
        api_base = llm_connection.get("base_url")

    provider = llm_connection.get("provider")
    if provider is not None:
        if not isinstance(provider, str) or not provider.strip():
            raise ValueError("LLM Secret JSON field 'provider' must be a non-empty string when provided")
        normalized["provider"] = provider.strip()

    if api_base is not None:
        if not isinstance(api_base, str) or not api_base.strip():
            raise ValueError("LLM Secret JSON field 'api_base' must be a non-empty string when provided")
        normalized["api_base"] = api_base.strip()

    for key in ("api_key", "api_version", "organization"):
        value = llm_connection.get(key)
        if value is None:
            continue
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"LLM Secret JSON field '{key}' must be a non-empty string when provided")
        normalized[key] = value.strip()

    extra_headers = llm_connection.get("extra_headers")
    if extra_headers is not None:
        if not isinstance(extra_headers, dict):
            raise ValueError("LLM Secret JSON field 'extra_headers' must be an object when provided")
        normalized["extra_headers"] = extra_headers

    if "provider" not in normalized and "api_base" in normalized:
        normalized["provider"] = "ollama"

    return normalized


def build_llm_connection(llm_secret: dict[str, Any], model: str) -> dict[str, Any]:
    """Secretの接続情報とモデル名を結合し、LiteLLM呼び出し用設定を返す。

    処理内容:
        検証済みのSecret接続情報にモデル名を追加し、必要に応じて
        `provider/model` 形式へ正規化した辞書を生成する。
    入力:
        llm_secret: LiteLLM接続情報を含む検証済み接続辞書。
        model: フローごとに設定されたモデル名。
    出力:
        dict[str, Any]: LiteLLM接続辞書。
    例外:
        ValueError: モデル名が空または文字列でない場合。
    外部依存リソース:
        なし。
    """
    if not isinstance(model, str) or not model:
        raise ValueError("LLM model must be a non-empty string")

    normalized_secret = validate_llm_connection(llm_secret)
    normalized_model = model
    provider = normalized_secret.get("provider")
    if isinstance(provider, str) and provider and "/" not in model:
        normalized_model = f"{provider}/{model}"

    return {
        **normalized_secret,
        "model": normalized_model,
    }


def load_llm_connection_secret(secret_block_name: str, logger: logging.Logger | None = None) -> dict[str, Any]:
    """Prefect SecretブロックからLiteLLM接続情報を読み出して検証する。

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
    llm_secret_value = Secret.load(secret_block_name).get()
    if logger is not None:
        logger.info("Prefect secret loaded: key=llm_connection_block name=%s", secret_block_name)
    return validate_llm_connection(llm_secret_value)


def _get_litellm_functions() -> tuple[Any, Any]:
    """LiteLLMの呼び出し関数を遅延importして返す。"""
    try:
        from litellm import completion, embedding
    except ImportError as exc:
        raise RuntimeError("litellm is required. Install project dependencies with `uv sync`.") from exc
    return completion, embedding


def _extract_completion_text(response: Any) -> str:
    """LiteLLM completionレスポンスから本文文字列を取り出す。"""
    choices = getattr(response, "choices", None)
    if choices is None and isinstance(response, dict):
        choices = response.get("choices")
    if not choices:
        return ""

    first_choice = choices[0]
    message = getattr(first_choice, "message", None)
    if message is None and isinstance(first_choice, dict):
        message = first_choice.get("message")
    if message is None:
        return ""

    content = getattr(message, "content", None)
    if content is None and isinstance(message, dict):
        content = message.get("content")
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        text_parts: list[str] = []
        for item in content:
            if isinstance(item, dict) and isinstance(item.get("text"), str):
                text_parts.append(item["text"])
        return "\n".join(part.strip() for part in text_parts if part.strip()).strip()
    return ""


def _extract_embedding_vector(response: Any) -> list[float]:
    """LiteLLM embeddingレスポンスからベクトルを取り出す。"""
    data = getattr(response, "data", None)
    if data is None and isinstance(response, dict):
        data = response.get("data")
    if not isinstance(data, list) or not data:
        raise ValueError("LiteLLM embeddings response must include non-empty data array")

    first_item = data[0]
    embedding = getattr(first_item, "embedding", None)
    if embedding is None and isinstance(first_item, dict):
        embedding = first_item.get("embedding")
    if not isinstance(embedding, list) or not embedding:
        raise ValueError("LiteLLM embeddings response must include non-empty embedding array")
    return [float(value) for value in embedding]


def invoke_llm_generate(
    llm_connection: dict[str, Any],
    prompt: str,
    timeout_sec: int = 120,
    response_format: str | dict[str, Any] | None = None,
    logger: logging.Logger | None = None,
) -> str:
    """LiteLLM経由で生成テキストを返す。

    処理内容:
        モデル名・プロンプト・フォーマットからLiteLLM `completion()` を呼び出し、
        OpenAI互換レスポンスから本文を抽出する。
    入力:
        llm_connection: LiteLLM接続設定。
        prompt: 生成プロンプト文字列。
        timeout_sec: HTTPタイムアウト秒。
        response_format: 出力フォーマット指定。Ollamaでは `format`、他プロバイダーでは
            可能な範囲で `response_format` に変換して渡す。
        logger: 任意ロガー。
    出力:
        str: 生成されたテキスト（前後空白除去済み）。
    例外:
        RuntimeError: litellm 未インストール時。
        LiteLLM由来例外: 通信・認証・レート制限失敗時。
    外部依存リソース:
        LiteLLM対応LLM API。
    """
    if logger is not None:
        logger.info("llm prompt: %s", prompt)

    completion, _ = _get_litellm_functions()
    request_kwargs: dict[str, Any] = {
        "model": llm_connection["model"],
        "messages": [{"role": "user", "content": prompt}],
        "timeout": timeout_sec,
    }
    for key in SUPPORTED_LLM_CONNECTION_KEYS:
        if key in llm_connection:
            request_kwargs[key] = llm_connection[key]

    provider = llm_connection.get("provider")
    if response_format is not None:
        if provider == "ollama":
            request_kwargs["format"] = response_format
        elif response_format == "json":
            request_kwargs["response_format"] = {"type": "json_object"}
        elif isinstance(response_format, dict):
            request_kwargs["response_format"] = {
                "type": "json_schema",
                "json_schema": {
                    "name": "structured_output",
                    "schema": response_format,
                },
            }
        else:
            request_kwargs["response_format"] = response_format

    response = completion(**request_kwargs)
    response_text = _extract_completion_text(response)
    if logger is not None:
        logger.info("llm response: %s", response_text)
    return response_text


def invoke_llm_embeddings(
    llm_connection: dict[str, Any],
    text: str,
    timeout_sec: int = 120,
    logger: logging.Logger | None = None,
) -> list[float]:
    """LiteLLM経由で埋め込みベクトルを返す。

    処理内容:
        モデル名と入力テキストからLiteLLM `embedding()` を呼び出し、
        OpenAI互換レスポンスの `data[0].embedding` を返す。
    入力:
        llm_connection: LiteLLM接続設定。
        text: 埋め込み対象テキスト。
        timeout_sec: HTTPタイムアウト秒。
        logger: 任意ロガー。
    出力:
        list[float]: 埋め込みベクトル。
    例外:
        ValueError: レスポンスに embedding が存在しない、または不正形式の場合。
        RuntimeError: litellm 未インストール時。
        LiteLLM由来例外: 通信・認証・レート制限失敗時。
    外部依存リソース:
        LiteLLM対応LLM API。
    """
    _, embedding = _get_litellm_functions()
    request_kwargs: dict[str, Any] = {
        "model": llm_connection["model"],
        "input": text,
        "timeout": timeout_sec,
    }
    for key in SUPPORTED_LLM_CONNECTION_KEYS:
        if key in llm_connection:
            request_kwargs[key] = llm_connection[key]

    response = embedding(**request_kwargs)
    vector = _extract_embedding_vector(response)
    if logger is not None:
        logger.info("llm embedding size=%d", len(vector))
    return vector


def validate_ollama_connection(ollama_connection: Any) -> dict[str, Any]:
    """互換用: Ollama接続検証をLiteLLM接続検証へ委譲する。"""
    return validate_llm_connection(ollama_connection)


def build_ollama_connection(ollama_secret: dict[str, Any], model: str) -> dict[str, Any]:
    """互換用: Ollama接続構築をLiteLLM接続構築へ委譲する。"""
    return build_llm_connection(ollama_secret, model)


def load_ollama_connection_secret(secret_block_name: str, logger: logging.Logger | None = None) -> dict[str, Any]:
    """互換用: Ollama Secret読込をLiteLLM Secret読込へ委譲する。"""
    return load_llm_connection_secret(secret_block_name, logger=logger)


def invoke_ollama_generate(
    ollama_connection: dict[str, Any],
    prompt: str,
    timeout_sec: int = 120,
    response_format: str | dict[str, Any] | None = None,
    logger: logging.Logger | None = None,
) -> str:
    """互換用: Ollama生成呼び出しをLiteLLM生成呼び出しへ委譲する。"""
    return invoke_llm_generate(
        llm_connection=ollama_connection,
        prompt=prompt,
        timeout_sec=timeout_sec,
        response_format=response_format,
        logger=logger,
    )


def invoke_ollama_embeddings(
    ollama_connection: dict[str, Any],
    text: str,
    timeout_sec: int = 120,
    logger: logging.Logger | None = None,
) -> list[float]:
    """互換用: Ollama埋め込み呼び出しをLiteLLM埋め込み呼び出しへ委譲する。"""
    return invoke_llm_embeddings(
        llm_connection=ollama_connection,
        text=text,
        timeout_sec=timeout_sec,
        logger=logger,
    )
