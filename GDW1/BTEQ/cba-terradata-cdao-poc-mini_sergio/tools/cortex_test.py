#!/usr/bin/env python3
"""
Snowflake Cortex test module using Snowpark.

Features:
- Text generation via SNOWFLAKE.CORTEX.COMPLETE (called through Snowpark functions)
- Embeddings via SNOWFLAKE.CORTEX.EMBED_TEXT (through Snowpark)
- Env-driven connection config and a small CLI

Environment variables:
- SNOWFLAKE_ACCOUNT
- SNOWFLAKE_USER
- SNOWFLAKE_PASSWORD
- SNOWFLAKE_ROLE
- SNOWFLAKE_WAREHOUSE
- SNOWFLAKE_DATABASE
- SNOWFLAKE_SCHEMA
- CORTEX_MODEL (optional, default: snowflake-arctic)
- CORTEX_EMBED_MODEL (optional, default: e5-base-v2)

Usage examples:
  python gdw1_xfm_frmw/tools/cortex_test.py complete --prompt "Summarize BTEQ to dbt migration principles" --temperature 0.2
  python gdw1_xfm_frmw/tools/cortex_test.py embed --text "Account balance bucket detail"
"""
from __future__ import annotations

import os
import sys
import json
import argparse
from typing import Any, Dict, Optional

from snowflake.snowpark import Session
from snowflake.snowpark import functions as F


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def create_session() -> Session:
    """Create a Snowpark session using environment variables."""
    connection_parameters = {
        "account": _required_env("SNOWFLAKE_ACCOUNT"),
        "user": _required_env("SNOWFLAKE_USER"),
        "password": _required_env("SNOWFLAKE_PASSWORD"),
        "role": _required_env("SNOWFLAKE_ROLE"),
        "warehouse": _required_env("SNOWFLAKE_WAREHOUSE"),
        "database": _required_env("SNOWFLAKE_DATABASE"),
        "schema": _required_env("SNOWFLAKE_SCHEMA"),
    }
    return Session.builder.configs(connection_parameters).create()


def _build_options_object(options: Optional[Dict[str, Any]]) -> F.Column:
    """Return a Snowpark Column representing OBJECT_CONSTRUCT of non-null options.

    If no options are provided, returns an empty OBJECT (OBJECT_CONSTRUCT()).
    """
    kv: list[F.Column] = []
    if options:
        for key, val in options.items():
            if val is None:
                continue
            kv.append(F.lit(str(key)))
            if isinstance(val, bool):
                kv.append(F.lit(bool(val)))
            elif isinstance(val, (int, float)):
                kv.append(F.lit(val))
            else:
                kv.append(F.lit(str(val)))
    if not kv:
        return F.object_construct()
    return F.object_construct(*kv)


def cortex_complete(
    session: Session,
    prompt: str,
    *,
    model: Optional[str] = None,
    temperature: Optional[float] = None,
    max_output_tokens: Optional[int] = None,
    top_p: Optional[float] = None,
) -> str:
    """Call SNOWFLAKE.CORTEX.COMPLETE via Snowpark and return the text result."""
    selected_model = model or os.getenv("CORTEX_MODEL", "snowflake-arctic")
    opts_col = _build_options_object(
        {
            "temperature": temperature,
            "max_output_tokens": max_output_tokens,
            "top_p": top_p,
        }
    )
    df = session.select(
        F.call_function(
            "snowflake.cortex.complete",
            F.lit(selected_model),
            F.lit(prompt),
            opts_col,
        ).alias("output")
    )
    row = df.collect()[0]
    output = row["OUTPUT"] if "OUTPUT" in row.asDict() else row[0]
    if isinstance(output, (dict, list)):
        return json.dumps(output)
    return str(output)


def cortex_embed_text(
    session: Session,
    text: str,
    *,
    model: Optional[str] = None,
) -> list[float]:
    """Call SNOWFLAKE.CORTEX.EMBED_TEXT via Snowpark and return embedding vector."""
    selected_model = model or os.getenv("CORTEX_EMBED_MODEL", "e5-base-v2")
    df = session.select(
        F.call_function(
            "snowflake.cortex.embed_text",
            F.lit(selected_model),
            F.lit(text),
        ).alias("embedding")
    )
    row = df.collect()[0]
    embedding = row["EMBEDDING"] if "EMBEDDING" in row.asDict() else row[0]
    if isinstance(embedding, str):
        try:
            embedding = json.loads(embedding)
        except json.JSONDecodeError:
            pass
    if not isinstance(embedding, list):
        raise RuntimeError("Unexpected embedding return type from Cortex")
    return [float(x) for x in embedding]


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Snowflake Cortex test CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    p_complete = sub.add_parser("complete", help="Run text generation")
    p_complete.add_argument("--prompt", required=True, help="Prompt text")
    p_complete.add_argument("--model", default=None, help="Cortex model name")
    p_complete.add_argument("--temperature", type=float, default=None)
    p_complete.add_argument("--max-output-tokens", type=int, default=None)
    p_complete.add_argument("--top-p", type=float, default=None)

    p_embed = sub.add_parser("embed", help="Get text embeddings")
    p_embed.add_argument("--text", required=True, help="Text to embed")
    p_embed.add_argument("--model", default=None, help="Embedding model name")

    return parser


def main(argv: Optional[list[str]] = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    try:
        session = create_session()
    except Exception as exc:  # noqa: BLE001
        print(f"Failed to create Snowflake session: {exc}")
        return 2

    try:
        if args.command == "complete":
            text = cortex_complete(
                session,
                prompt=args.prompt,
                model=args.model,
                temperature=args.temperature,
                max_output_tokens=args.max_output_tokens,
                top_p=args.top_p,
            )
            print(text)
        elif args.command == "embed":
            vec = cortex_embed_text(session, text=args.text, model=args.model)
            print(json.dumps({"dim": len(vec), "vector": vec[:8] + ["..."]}))
        else:
            parser.error("Unknown command")
            return 2
    except Exception as exc:  # noqa: BLE001
        print(f"Cortex call failed: {exc}")
        return 1
    finally:
        try:
            session.close()
        except Exception:  # noqa: BLE001
            pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
