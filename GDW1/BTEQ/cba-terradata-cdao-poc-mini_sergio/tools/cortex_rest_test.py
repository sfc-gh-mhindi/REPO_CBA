#!/usr/bin/env python3
"""
Snowflake Cortex REST API test module (no warehouse required).

Implements minimal Python helpers for:
- COMPLETE: text generation
- EMBED: vector embeddings

Auth: Bearer token via env var (see docs for obtaining OAuth/JWT/programmatic token).

Environment variables:
- SNOWFLAKE_ACCOUNT: account identifier (e.g., xy12345.us-east-1)
  or SNOWFLAKE_ACCOUNT_URL: full base URL (e.g., https://xy12345.us-east-1.snowflakecomputing.com)
- SNOWFLAKE_BEARER_TOKEN: OAuth/JWT/programmatic access token for Authorization header
- CORTEX_MODEL: default model for COMPLETE (default: snowflake-arctic)
- CORTEX_EMBED_MODEL: default model for EMBED (default: e5-base-v2)

CLI examples:
  python gdw1_xfm_frmw/tools/cortex_rest_test.py complete --prompt "Hello" --temperature 0.2
  python gdw1_xfm_frmw/tools/cortex_rest_test.py embed --text "foo"
"""
from __future__ import annotations

import os
import sys
import json
import argparse
from typing import Any, Dict, List, Optional

import requests


CORTEX_COMPLETE_PATH = "/api/v2/cortex/inference:complete"
CORTEX_EMBED_PATH = "/api/v2/cortex/inference:embed"


def _base_url() -> str:
    url = os.getenv("SNOWFLAKE_ACCOUNT_URL")
    if url:
        return url.rstrip("/")
    acct = os.getenv("SNOWFLAKE_ACCOUNT")
    if not acct:
        raise RuntimeError("Set SNOWFLAKE_ACCOUNT or SNOWFLAKE_ACCOUNT_URL")
    return f"https://{acct}.snowflakecomputing.com"


def _auth_headers() -> Dict[str, str]:
    token = os.getenv("SNOWFLAKE_BEARER_TOKEN")
    if not token:
        raise RuntimeError("Set SNOWFLAKE_BEARER_TOKEN with an OAuth/JWT/programmatic token")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def cortex_complete(
    prompt: str,
    *,
    model: Optional[str] = None,
    temperature: Optional[float] = None,
    max_output_tokens: Optional[int] = None,
    top_p: Optional[float] = None,
    stream: bool = False,
) -> Dict[str, Any]:
    """Invoke Cortex COMPLETE via REST. Returns JSON response dict.

    See: https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-rest-api
    """
    body: Dict[str, Any] = {
        "model": model or os.getenv("CORTEX_MODEL", "snowflake-arctic"),
        "prompt": prompt,
        "temperature": temperature,
        "max_output_tokens": max_output_tokens,
        "top_p": top_p,
        "stream": stream,
    }
    # Drop None values
    body = {k: v for k, v in body.items() if v is not None}

    resp = requests.post(
        _base_url() + CORTEX_COMPLETE_PATH,
        headers=_auth_headers(),
        data=json.dumps(body),
        stream=stream,
        timeout=60,
    )
    resp.raise_for_status()

    if stream:
        # Server-Sent Events; return raw lines as a list
        return {"events": [line.decode("utf-8").rstrip() for line in resp.iter_lines() if line]}

    return resp.json()


def cortex_embed(
    texts: List[str] | str,
    *,
    model: Optional[str] = None,
) -> Dict[str, Any]:
    """Invoke Cortex EMBED via REST. Returns JSON response dict with embeddings.

    Request body uses 'input' as a list of strings.
    """
    inputs = [texts] if isinstance(texts, str) else list(texts)
    body: Dict[str, Any] = {
        "model": model or os.getenv("CORTEX_EMBED_MODEL", "e5-base-v2"),
        "input": inputs,
    }
    resp = requests.post(
        _base_url() + CORTEX_EMBED_PATH,
        headers=_auth_headers(),
        data=json.dumps(body),
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Snowflake Cortex REST test CLI")
    sub = p.add_subparsers(dest="cmd", required=True)

    pc = sub.add_parser("complete", help="Run text generation")
    pc.add_argument("--prompt", required=True)
    pc.add_argument("--model", default=None)
    pc.add_argument("--temperature", type=float, default=None)
    pc.add_argument("--max-output-tokens", type=int, default=None)
    pc.add_argument("--top-p", type=float, default=None)
    pc.add_argument("--stream", action="store_true")

    pe = sub.add_parser("embed", help="Get embeddings for text")
    pe.add_argument("--text", required=True, nargs="+", help="One or more input strings")
    pe.add_argument("--model", default=None)

    return p


def main(argv: Optional[list[str]] = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    p = _parser()
    a = p.parse_args(argv)

    try:
        if a.cmd == "complete":
            out = cortex_complete(
                prompt=a.prompt,
                model=a.model,
                temperature=a.temperature,
                max_output_tokens=a.max_output_tokens,
                top_p=a.top_p,
                stream=a.stream,
            )
            print(json.dumps(out, indent=2))
        elif a.cmd == "embed":
            out = cortex_embed(texts=a.text, model=a.model)
            print(json.dumps(out, indent=2))
        else:
            p.error("unknown command")
            return 2
    except requests.HTTPError as he:
        print(f"HTTP error: {he} -> {getattr(he.response, 'text', '')}")
        return 1
    except Exception as exc:  # noqa: BLE001
        print(f"Error: {exc}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
