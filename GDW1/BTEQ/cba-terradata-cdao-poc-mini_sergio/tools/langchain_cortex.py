#!/usr/bin/env python3
"""
LangChain wrappers for Snowflake Cortex via REST API.

- SnowflakeCortexLLM: LLM wrapper using COMPLETE endpoint
- SnowflakeCortexEmbeddings: Embeddings wrapper using EMBED endpoint

Docs:
- Cortex REST API: https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-rest-api
- AISQL with Python: https://docs.snowflake.com/user-guide/snowflake-cortex/aisql#using-snowflake-cortex-llm-functions-with-python

Auth & Privileges:
- Use OAuth/JWT/programmatic token in SNOWFLAKE_BEARER_TOKEN
- Default role must have SNOWFLAKE.CORTEX_USER

Environment:
- SNOWFLAKE_ACCOUNT or SNOWFLAKE_ACCOUNT_URL
- SNOWFLAKE_BEARER_TOKEN
- CORTEX_MODEL (optional)
- CORTEX_EMBED_MODEL (optional)
"""
from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from langchain_core.language_models.llms import LLM
from langchain_core.callbacks import CallbackManagerForLLMRun
from langchain_core.outputs import GenerationChunk
from langchain_core.embeddings import Embeddings

# Reuse REST helpers
from .cortex_rest_test import cortex_complete, cortex_embed


class SnowflakeCortexLLM(LLM):
    """LangChain LLM wrapper for Snowflake Cortex COMPLETE via REST."""

    model: str = os.getenv("CORTEX_MODEL", "snowflake-arctic")
    temperature: Optional[float] = None
    max_output_tokens: Optional[int] = None
    top_p: Optional[float] = None

    @property
    def _llm_type(self) -> str:
        return "snowflake_cortex_llm"

    @property
    def _identifying_params(self) -> Dict[str, Any]:
        return {
            "model": self.model,
            "temperature": self.temperature,
            "max_output_tokens": self.max_output_tokens,
            "top_p": self.top_p,
        }

    def _call(
        self,
        prompt: str,
        stop: Optional[List[str]] = None,
        run_manager: Optional[CallbackManagerForLLMRun] = None,
    ) -> str:
        # No streaming by default; you can extend to handle Server-Sent Events
        resp = cortex_complete(
            prompt=prompt,
            model=self.model,
            temperature=self.temperature,
            max_output_tokens=self.max_output_tokens,
            top_p=self.top_p,
            stream=False,
        )
        # Try to extract text from common response shapes
        # 1) { "response": "..." } or { "output_text": "..." }
        for key in ("response", "output", "output_text", "text"):
            if key in resp and isinstance(resp[key], str):
                return resp[key]
        # 2) choices-style
        choices = resp.get("choices") or resp.get("outputs")
        if isinstance(choices, list) and choices:
            choice = choices[0]
            if isinstance(choice, dict):
                for key in ("text", "output_text", "response"):
                    if key in choice and isinstance(choice[key], str):
                        return choice[key]
                # OpenAI-like message structure
                msg = choice.get("message")
                if isinstance(msg, dict) and "content" in msg:
                    return str(msg["content"])  # type: ignore[no-any-return]
        # Fallback to stringification
        return str(resp)

    # Optional: basic token streaming hook emulation (not full SSE)
    def stream(self, prompt: str) -> GenerationChunk:  # type: ignore[override]
        events = cortex_complete(
            prompt=prompt,
            model=self.model,
            temperature=self.temperature,
            max_output_tokens=self.max_output_tokens,
            top_p=self.top_p,
            stream=True,
        ).get("events", [])
        text = "\n".join([e for e in events if e.startswith("data:")])
        return GenerationChunk(text=text)


class SnowflakeCortexEmbeddings(Embeddings):
    """LangChain Embeddings wrapper for Snowflake Cortex EMBED via REST."""

    model: str = os.getenv("CORTEX_EMBED_MODEL", "e5-base-v2")

    def _extract_embeddings(self, resp: Dict[str, Any]) -> List[List[float]]:
        # Handle {"data": [{"embedding": [...]}, ...]} or {"embeddings": [[...], ...]}
        if "data" in resp and isinstance(resp["data"], list):
            out: List[List[float]] = []
            for item in resp["data"]:
                emb = item.get("embedding") if isinstance(item, dict) else None
                if isinstance(emb, list):
                    out.append([float(x) for x in emb])
            if out:
                return out
        if "embeddings" in resp and isinstance(resp["embeddings"], list):
            return [[float(x) for x in v] for v in resp["embeddings"]]
        raise ValueError("Unexpected embedding response format from Cortex REST")

    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        resp = cortex_embed(texts=texts, model=self.model)
        return self._extract_embeddings(resp)

    def embed_query(self, text: str) -> List[float]:
        resp = cortex_embed(texts=[text], model=self.model)
        vectors = self._extract_embeddings(resp)
        return vectors[0]


if __name__ == "__main__":
    # Minimal smoke test (requires env vars and token)
    llm = SnowflakeCortexLLM(model=os.getenv("CORTEX_MODEL", "snowflake-arctic"))
    print(llm("Say 'hello' in one word."))
    emb = SnowflakeCortexEmbeddings(model=os.getenv("CORTEX_EMBED_MODEL", "e5-base-v2"))
    print(emb.embed_query("hello world")[:8])
