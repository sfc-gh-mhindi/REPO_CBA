#!/usr/bin/env python3
"""
Native Snowflake Cortex Python demo using snowflake.cortex functions.

Functions demonstrated:
- Complete (LLM text generation)
- ExtractAnswer
- Sentiment
- Summarize
- Translate

Docs:
- AISQL with Python: https://docs.snowflake.com/user-guide/snowflake-cortex/aisql#using-snowflake-cortex-llm-functions-with-python

Auth/Config:
- These functions call Snowflake-hosted models. Ensure your environment is authenticated per Snowflake guidance
  (e.g., Snowflake CLI auth, programmatic token, or configured connection). Your role must have SNOWFLAKE.CORTEX_USER.

CLI examples:
  python bteq_dcf/tools/cortex_native_demo.py complete --prompt "how do snowflakes form?" --model snowflake-arctic --max-tokens 64
  python bteq_dcf/tools/cortex_native_demo.py extract-answer --context-file ctx.txt --question "When was Snowflake founded?"
  python bteq_dcf/tools/cortex_native_demo.py sentiment --text "I really enjoyed this restaurant. Fantastic service!"
  python bteq_dcf/tools/cortex_native_demo.py summarize --file article.txt
  python bteq_dcf/tools/cortex_native_demo.py translate --text "Bonjour" --source fr --target en
"""
from __future__ import annotations

import os
import sys
import argparse
from typing import Optional

from snowflake.cortex import (
    Complete,
    CompleteOptions,
    ExtractAnswer,
    Sentiment,
    Summarize,
    Translate,
)


def _read_text_from_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def cmd_complete(args: argparse.Namespace) -> int:
    model = args.model or os.getenv("CORTEX_MODEL", "snowflake-arctic")
    options = None
    opts_dict = {}
    if args.max_tokens is not None:
        opts_dict["max_tokens"] = int(args.max_tokens)
    if args.temperature is not None:
        opts_dict["temperature"] = float(args.temperature)
    if opts_dict:
        options = CompleteOptions(opts_dict)

    prompt = args.prompt
    if args.file:
        prompt = _read_text_from_file(args.file)
    print(Complete(model, prompt, options=options))
    return 0


def cmd_extract_answer(args: argparse.Namespace) -> int:
    context = args.context
    if args.context_file:
        context = _read_text_from_file(args.context_file)
    print(ExtractAnswer(context, args.question))
    return 0


def cmd_sentiment(args: argparse.Namespace) -> int:
    text = args.text
    if args.file:
        text = _read_text_from_file(args.file)
    print(Sentiment(text))
    return 0


def cmd_summarize(args: argparse.Namespace) -> int:
    text = args.text
    if args.file:
        text = _read_text_from_file(args.file)
    print(Summarize(text))
    return 0


def cmd_translate(args: argparse.Namespace) -> int:
    text = args.text
    if args.file:
        text = _read_text_from_file(args.file)
    print(Translate(text, args.source, args.target))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Native Snowflake Cortex Python demo (AISQL)")
    sub = p.add_subparsers(dest="cmd", required=True)

    pc = sub.add_parser("complete", help="LLM text generation")
    pc.add_argument("--prompt", help="Prompt text")
    pc.add_argument("--file", help="Path to prompt file")
    pc.add_argument("--model", default=None, help="Model name (default from CORTEX_MODEL or snowflake-arctic)")
    pc.add_argument("--max-tokens", type=int, default=None, dest="max_tokens")
    pc.add_argument("--temperature", type=float, default=None)
    pc.set_defaults(func=cmd_complete)

    pea = sub.add_parser("extract-answer", help="Extract answer from context text")
    pea.add_argument("--context", help="Context text")
    pea.add_argument("--context-file", help="Context file path")
    pea.add_argument("--question", required=True, help="Question to answer")
    pea.set_defaults(func=cmd_extract_answer)

    ps = sub.add_parser("sentiment", help="Sentiment score -1..1")
    ps.add_argument("--text", help="Input text")
    ps.add_argument("--file", help="Input file")
    ps.set_defaults(func=cmd_sentiment)

    psu = sub.add_parser("summarize", help="Summarize text")
    psu.add_argument("--text", help="Input text")
    psu.add_argument("--file", help="Input file")
    psu.set_defaults(func=cmd_summarize)

    pt = sub.add_parser("translate", help="Translate text")
    pt.add_argument("--text", help="Input text")
    pt.add_argument("--file", help="Input file")
    pt.add_argument("--source", required=True, help="Source language code (e.g., en)")
    pt.add_argument("--target", required=True, help="Target language code (e.g., fr)")
    pt.set_defaults(func=cmd_translate)

    return p


def main(argv: Optional[list[str]] = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except Exception as exc:  # noqa: BLE001
        print(f"Error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
