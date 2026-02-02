"""Runtime configuration."""

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path


logger = logging.getLogger(__name__)

DEFAULT_MODEL = "anthropic/claude-sonnet-4-20250514"


def _load_preferred_model() -> str:
    """Load preferred model from ~/.hive/configuration.json."""
    config_path = Path.home() / ".hive" / "configuration.json"
    if not config_path.exists():
        return DEFAULT_MODEL

    try:
        with open(config_path) as f:
            config = json.load(f)

        llm = config.get("llm", {})
        provider = llm.get("provider")
        model = llm.get("model")
        if provider and model:
            return f"{provider}/{model}"

        logger.warning(
            "Configuration file %s is missing 'llm.provider' or 'llm.model'. "
            "Falling back to default model: %s",
            config_path,
            DEFAULT_MODEL,
        )
    except json.JSONDecodeError as exc:
        logger.error(
            "Invalid JSON in configuration file %s at line %d, column %d: %s. "
            "Falling back to default model: %s",
            config_path,
            exc.lineno,
            exc.colno,
            exc.msg,
            DEFAULT_MODEL,
        )
    except (OSError, IOError) as exc:
        logger.error(
            "Error reading configuration file %s: %s. Falling back to default model: %s",
            config_path,
            exc,
            DEFAULT_MODEL,
        )
    except Exception:
        logger.exception(
            "Unexpected error loading configuration file %s. Falling back to default model: %s",
            config_path,
            DEFAULT_MODEL,
        )

    return DEFAULT_MODEL


@dataclass
class RuntimeConfig:
    model: str = field(default_factory=_load_preferred_model)
    temperature: float = 0.7
    max_tokens: int = 8192
    api_key: str | None = None
    api_base: str | None = None


default_config = RuntimeConfig()


# Agent metadata
@dataclass
class AgentMetadata:
    name: str = "Online Research Agent"
    version: str = "1.0.0"
    description: str = "Research any topic by searching multiple sources, synthesizing information, and producing a well-structured narrative report with citations."


metadata = AgentMetadata()
