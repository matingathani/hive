"""
Web Search Tool - Search the web using multiple providers.

Supports:
- Google Custom Search API (GOOGLE_API_KEY + GOOGLE_CSE_ID)
- Brave Search API (BRAVE_SEARCH_API_KEY)

Auto-detection: If provider="auto", tries Brave first (backward compatible), then Google.
"""

from __future__ import annotations

import os
import time
from typing import TYPE_CHECKING, Literal

import httpx
from fastmcp import FastMCP

if TYPE_CHECKING:
    from aden_tools.credentials import CredentialStoreAdapter


def _get_env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


CACHE_TTL_SECONDS = _get_env_int("WEB_SEARCH_CACHE_TTL_SECONDS", 300)
CACHE_MAX_SIZE = _get_env_int("WEB_SEARCH_CACHE_MAX_SIZE", 128)
_CACHE: dict[str, tuple[float, dict]] = {}


def _cache_key(
    query: str,
    num_results: int,
    country: str,
    language: str,
    provider: str,
) -> str:
    return f"{provider}|{query}|{num_results}|{country}|{language}"


def _get_cached(key: str) -> dict | None:
    if CACHE_TTL_SECONDS <= 0:
        return None
    cached = _CACHE.get(key)
    if not cached:
        return None
    timestamp, payload = cached
    if time.time() - timestamp > CACHE_TTL_SECONDS:
        _CACHE.pop(key, None)
        return None
    return payload


def _set_cache(key: str, payload: dict) -> None:
    if CACHE_MAX_SIZE <= 0:
        return
    if len(_CACHE) >= CACHE_MAX_SIZE:
        oldest_key = min(_CACHE, key=lambda k: _CACHE[k][0])
        _CACHE.pop(oldest_key, None)
    _CACHE[key] = (time.time(), payload)


def register_tools(
    mcp: FastMCP,
    credentials: CredentialStoreAdapter | None = None,
) -> None:
    """Register web search tools with the MCP server."""

    def _search_google(
        query: str,
        num_results: int,
        country: str,
        language: str,
        api_key: str,
        cse_id: str,
    ) -> dict:
        """Execute search using Google Custom Search API."""
        response = httpx.get(
            "https://www.googleapis.com/customsearch/v1",
            params={
                "key": api_key,
                "cx": cse_id,
                "q": query,
                "num": min(num_results, 10),
                "lr": f"lang_{language}",
                "gl": country,
            },
            timeout=30.0,
        )

        if response.status_code == 401:
            return {"error": "Invalid Google API key"}
        elif response.status_code == 403:
            return {"error": "Google API key not authorized or quota exceeded"}
        elif response.status_code == 429:
            return {"error": "Google rate limit exceeded. Try again later."}
        elif response.status_code != 200:
            return {"error": f"Google API request failed: HTTP {response.status_code}"}

        data = response.json()
        results = []
        for item in data.get("items", [])[:num_results]:
            results.append(
                {
                    "title": item.get("title", ""),
                    "url": item.get("link", ""),
                    "snippet": item.get("snippet", ""),
                }
            )

        return {
            "query": query,
            "results": results,
            "total": len(results),
            "provider": "google",
        }

    def _search_brave(
        query: str,
        num_results: int,
        country: str,
        api_key: str,
    ) -> dict:
        """Execute search using Brave Search API."""
        response = httpx.get(
            "https://api.search.brave.com/res/v1/web/search",
            params={
                "q": query,
                "count": min(num_results, 20),
                "country": country,
            },
            headers={
                "X-Subscription-Token": api_key,
                "Accept": "application/json",
            },
            timeout=30.0,
        )

        if response.status_code == 401:
            return {"error": "Invalid Brave API key"}
        elif response.status_code == 429:
            return {"error": "Brave rate limit exceeded. Try again later."}
        elif response.status_code != 200:
            return {"error": f"Brave API request failed: HTTP {response.status_code}"}

        data = response.json()
        results = []
        for item in data.get("web", {}).get("results", [])[:num_results]:
            results.append(
                {
                    "title": item.get("title", ""),
                    "url": item.get("url", ""),
                    "snippet": item.get("description", ""),
                }
            )

        return {
            "query": query,
            "results": results,
            "total": len(results),
            "provider": "brave",
        }

    def _get_credentials() -> dict:
        """Get available search credentials."""
        if credentials is not None:
            return {
                "google_api_key": credentials.get("google_search"),
                "google_cse_id": credentials.get("google_cse"),
                "brave_api_key": credentials.get("brave_search"),
            }
        return {
            "google_api_key": os.getenv("GOOGLE_API_KEY"),
            "google_cse_id": os.getenv("GOOGLE_CSE_ID"),
            "brave_api_key": os.getenv("BRAVE_SEARCH_API_KEY"),
        }

    @mcp.tool()
    def web_search(
        query: str,
        num_results: int = 10,
        country: str = "us",
        language: str = "en",
        provider: Literal["auto", "google", "brave"] = "auto",
    ) -> dict:
        """
        Search the web for information.

        Supports multiple search providers:
        - "auto": Tries Brave first (backward compatible), then Google
        - "google": Use Google Custom Search API (requires GOOGLE_API_KEY + GOOGLE_CSE_ID)
        - "brave": Use Brave Search API (requires BRAVE_SEARCH_API_KEY)

        Args:
            query: The search query (1-500 chars)
            num_results: Number of results to return (1-20 for Brave, 1-10 for Google)
            country: Country code for localized results (us, id, uk, de, etc.)
            language: Language code for results (en, id, etc.) - Google only
            provider: Search provider to use ("auto", "google", "brave")

        Returns:
            Dict with search results, total count, and provider used
        """
        if not query or len(query) > 500:
            return {"error": "Query must be 1-500 characters"}

        creds = _get_credentials()
        google_available = creds["google_api_key"] and creds["google_cse_id"]
        brave_available = bool(creds["brave_api_key"])

        if provider == "auto":
            if brave_available:
                provider = "brave"
            elif google_available:
                provider = "google"
            else:
                return {
                    "error": "No search credentials configured",
                    "help": "Set either GOOGLE_API_KEY+GOOGLE_CSE_ID or BRAVE_SEARCH_API_KEY",
                }

        if provider == "google" and not google_available:
            return {
                "error": "Google credentials not configured",
                "help": "Set GOOGLE_API_KEY and GOOGLE_CSE_ID environment variables",
            }

        if provider == "brave" and not brave_available:
            return {
                "error": "Brave credentials not configured",
                "help": "Set BRAVE_SEARCH_API_KEY environment variable",
            }

        cache_key = _cache_key(query, num_results, country, language, provider)
        cached = _get_cached(cache_key)
        if cached is not None:
            return cached

        try:
            if provider == "google":
                response = _search_google(
                    query,
                    num_results,
                    country,
                    language,
                    creds["google_api_key"],
                    creds["google_cse_id"],
                )
            else:
                response = _search_brave(query, num_results, country, creds["brave_api_key"])

            if "error" not in response:
                _set_cache(cache_key, response)

            return response

        except httpx.TimeoutException:
            return {"error": "Search request timed out"}
        except httpx.RequestError as e:
            return {"error": f"Network error: {str(e)}"}
        except Exception as e:
            return {"error": f"Search failed: {str(e)}"}
