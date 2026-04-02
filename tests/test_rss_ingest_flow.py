from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from flows import rss_ingest_flow


VALID_CONFIG_YAML = """
rss_urls:
  - https://example.com/rss.xml
retry:
  max_retries: 3
  initial_delay_sec: 2
  backoff_multiplier: 2
storage:
  s3_prefix: rss
prefect_blocks:
  aws_credentials_block: aws-credentials-prod
  ollama_connection_secret_block: ollama-connection
"""


class TestRssIngestTasks(unittest.TestCase):
    def test_load_config_task_returns_config_when_valid(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.yaml"
            config_path.write_text(VALID_CONFIG_YAML, encoding="utf-8")

            config = rss_ingest_flow.load_config_task.fn(str(config_path))

        self.assertEqual(config["rss_urls"], ["https://example.com/rss.xml"])
        self.assertEqual(config["retry"]["max_retries"], 3)
        self.assertEqual(config["storage"]["s3_prefix"], "rss")
        self.assertEqual(config["prefect_blocks"]["aws_credentials_block"], "aws-credentials-prod")

    def test_load_config_task_raises_when_rss_urls_is_empty(self) -> None:
        invalid_yaml = """
rss_urls: []
retry:
  max_retries: 3
storage:
  s3_prefix: rss
prefect_blocks:
  aws_credentials_block: aws-credentials-prod
  ollama_connection_secret_block: ollama-connection
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "config.yaml"
            config_path.write_text(invalid_yaml, encoding="utf-8")

            with self.assertRaises(ValueError):
                rss_ingest_flow.load_config_task.fn(str(config_path))

    @patch("flows.rss_ingest_flow.get_run_logger")
    @patch("flows.rss_ingest_flow.Secret")
    @patch("flows.rss_ingest_flow.AwsCredentials")
    def test_validate_prerequisites_task_loads_blocks_and_json_secret(
        self,
        mock_aws_credentials: MagicMock,
        mock_secret: MagicMock,
        mock_get_run_logger: MagicMock,
    ) -> None:
        mock_get_run_logger.return_value = MagicMock()
        mock_secret.load.return_value.get.return_value = '{"base_url":"http://localhost:11434","model":"llama3.1:8b"}'

        parsed_config = {
            "rss_urls": ["https://example.com/rss.xml", "https://example.com/rss.xml"],
            "retry": {"max_retries": 3},
            "storage": {"s3_prefix": "rss"},
            "prefect_blocks": {
                "aws_credentials_block": "aws-credentials-prod",
                "ollama_connection_secret_block": "ollama-connection",
            },
        }

        rss_ingest_flow.validate_prerequisites_task.fn(parsed_config)

        mock_aws_credentials.load.assert_called_once_with("aws-credentials-prod")
        mock_secret.load.assert_called_once_with("ollama-connection")

    @patch("flows.rss_ingest_flow.get_run_logger")
    @patch("flows.rss_ingest_flow.Secret")
    @patch("flows.rss_ingest_flow.AwsCredentials")
    def test_validate_prerequisites_task_raises_when_secret_json_is_invalid(
        self,
        mock_aws_credentials: MagicMock,
        mock_secret: MagicMock,
        mock_get_run_logger: MagicMock,
    ) -> None:
        mock_get_run_logger.return_value = MagicMock()
        mock_secret.load.return_value.get.return_value = "not-json"

        parsed_config = {
            "rss_urls": ["https://example.com/rss.xml"],
            "retry": {"max_retries": 3},
            "storage": {"s3_prefix": "rss"},
            "prefect_blocks": {
                "aws_credentials_block": "aws-credentials-prod",
                "ollama_connection_secret_block": "ollama-connection",
            },
        }

        with self.assertRaises(ValueError):
            rss_ingest_flow.validate_prerequisites_task.fn(parsed_config)

        mock_aws_credentials.load.assert_called_once_with("aws-credentials-prod")


if __name__ == "__main__":
    unittest.main()
