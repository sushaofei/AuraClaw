#!/usr/bin/env python3
"""Probe the OpenAI-compatible model configured for AuraClaw.

Reads AURACLAW_MODEL_* from .env (or AURACLAW_ENV_FILE / env vars),
then POSTs to /chat/completions. Compatible with Python 3.7+.

Examples:
  python3 scripts/test_model.py
  python3 scripts/test_model.py --stream
  python3 scripts/test_model.py --prompt "用一句话介绍你自己"
  python3 scripts/test_model.py --env-file /path/to/.env
"""

import argparse
import json
import os
import ssl
import sys
import time

try:
    from urllib.error import HTTPError, URLError
    from urllib.request import Request, urlopen
except ImportError:  # pragma: no cover - Python 2 fallback
    from urllib2 import HTTPError, Request, URLError, urlopen  # type: ignore


def _load_env_file(path):
    values = {}
    if not path or not os.path.isfile(path):
        return values
    with open(path) as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("'").strip('"')
            values[key] = value
    return values


def _chat_completions_url(base_url):
    endpoint = (base_url or "").rstrip("/")
    if endpoint.endswith("/chat/completions"):
        return endpoint
    return endpoint + "/chat/completions"


def _build_payload(model, prompt, stream, max_tokens, thinking_enabled):
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "You are a concise assistant."},
            {"role": "user", "content": prompt},
        ],
        "max_tokens": max_tokens,
        "stream": stream,
    }
    if thinking_enabled is not None:
        payload["thinking"] = {
            "type": "enabled" if thinking_enabled else "disabled",
        }
    if stream:
        payload["stream_options"] = {"include_usage": True}
    return payload


def _parse_thinking_flag(raw):
    if raw is None or raw == "":
        return None
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


def _print_non_stream(body_bytes, elapsed):
    text = body_bytes.decode("utf-8", errors="replace")
    try:
        data = json.loads(text)
    except ValueError:
        print("RAW RESPONSE:")
        print(text)
        print(f"elapsed_ms={elapsed * 1000:.0f}")
        return 1

    choices = data.get("choices") or []
    content = ""
    finish_reason = None
    if choices:
        message = choices[0].get("message") or {}
        content = message.get("content") or ""
        finish_reason = choices[0].get("finish_reason")
    usage = data.get("usage") or {}

    print("=== model reply ===")
    print(content if content else "(empty content)")
    print("=== meta ===")
    print(f"finish_reason={finish_reason}")
    print(f"usage={json.dumps(usage, ensure_ascii=False)}")
    print(f"elapsed_ms={elapsed * 1000:.0f}")
    if data.get("id"):
        print("id={}".format(data.get("id")))
    if data.get("model"):
        print("model={}".format(data.get("model")))
    return 0 if content else 2


def _print_stream(resp, elapsed_started):
    content_parts = []
    usage = {}
    finish_reason = None
    model_name = None
    first_token_at = None

    while True:
        line = resp.readline()
        if not line:
            break
        if isinstance(line, bytes):
            line = line.decode("utf-8", errors="replace")
        line = line.strip()
        if not line or line.startswith(":"):
            continue
        if not line.startswith("data:"):
            continue
        data = line[5:].strip()
        if data == "[DONE]":
            break
        try:
            chunk = json.loads(data)
        except ValueError:
            print(f"bad sse chunk: {data}", file=sys.stderr)
            continue
        if chunk.get("model"):
            model_name = chunk.get("model")
        if isinstance(chunk.get("usage"), dict):
            usage = chunk["usage"]
        for choice in chunk.get("choices") or []:
            delta = choice.get("delta") or {}
            piece = delta.get("content") or ""
            if piece:
                if first_token_at is None:
                    first_token_at = time.time()
                content_parts.append(piece)
                sys.stdout.write(piece)
                sys.stdout.flush()
            if choice.get("finish_reason"):
                finish_reason = choice.get("finish_reason")

    elapsed = time.time() - elapsed_started
    content = "".join(content_parts)
    print()
    print("=== meta ===")
    print(f"finish_reason={finish_reason}")
    print(f"usage={json.dumps(usage, ensure_ascii=False)}")
    print(f"elapsed_ms={elapsed * 1000:.0f}")
    if first_token_at is not None:
        print(f"ttft_ms={(first_token_at - elapsed_started) * 1000:.0f}")
    if model_name:
        print(f"model={model_name}")
    return 0 if content else 2


def main(argv=None):
    parser = argparse.ArgumentParser(description="Test AuraClaw configured LLM endpoint")
    default_env = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        ".env",
    )
    parser.add_argument(
        "--env-file",
        default=os.environ.get("AURACLAW_ENV_FILE", default_env),
        help="env file with AURACLAW_MODEL_* (default: ../.env)",
    )
    parser.add_argument("--base-url", default=None, help="override AURACLAW_MODEL_BASE_URL")
    parser.add_argument("--api-key", default=None, help="override AURACLAW_MODEL_API_KEY")
    parser.add_argument("--model", default=None, help="override AURACLAW_MODEL_NAME")
    parser.add_argument("--prompt", default="请用一句话回复：你好，请确认你工作正常。")
    parser.add_argument("--max-tokens", type=int, default=256)
    parser.add_argument("--timeout", type=float, default=None)
    parser.add_argument("--stream", action="store_true", help="use SSE streaming")
    parser.add_argument(
        "--thinking",
        choices=["auto", "on", "off"],
        default="auto",
        help="thinking flag; auto uses AURACLAW_MODEL_THINKING_ENABLED",
    )
    parser.add_argument("--insecure", action="store_true", help="disable TLS cert verify")
    args = parser.parse_args(argv)

    file_env = _load_env_file(args.env_file)

    # Private model endpoints should not go through local HTTP proxies.
    for key in ("NO_PROXY", "no_proxy"):
        value = os.environ.get(key) or file_env.get(key)
        if value:
            os.environ[key] = value
    for key in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "all_proxy"):
        # Keep proxies unset for this process when talking to configured private hosts.
        # Callers can still override by exporting AURACLAW_MODEL_USE_PROXY=1.
        if os.environ.get("AURACLAW_MODEL_USE_PROXY", "").strip().lower() in ("1", "true", "yes"):
            break
        os.environ.pop(key, None)

    def pick(key, override=None, default=None):
        if override is not None:
            return override
        if os.environ.get(key):
            return os.environ.get(key)
        if key in file_env:
            return file_env[key]
        return default

    base_url = pick("AURACLAW_MODEL_BASE_URL", args.base_url)
    api_key = pick("AURACLAW_MODEL_API_KEY", args.api_key)
    model = pick("AURACLAW_MODEL_NAME", args.model)
    timeout = args.timeout
    if timeout is None:
        timeout = float(pick("AURACLAW_MODEL_TIMEOUT_SECONDS", default="120") or 120)

    if args.thinking == "on":
        thinking_enabled = True
    elif args.thinking == "off":
        thinking_enabled = False
    else:
        thinking_enabled = _parse_thinking_flag(pick("AURACLAW_MODEL_THINKING_ENABLED"))

    missing = [
        name
        for name, value in (
            ("AURACLAW_MODEL_BASE_URL", base_url),
            ("AURACLAW_MODEL_API_KEY", api_key),
            ("AURACLAW_MODEL_NAME", model),
        )
        if not value
    ]
    if missing:
        print("missing config: {}".format(", ".join(missing)), file=sys.stderr)
        print(f"env_file={args.env_file}", file=sys.stderr)
        return 1

    url = _chat_completions_url(base_url)
    payload = _build_payload(
        model=model,
        prompt=args.prompt,
        stream=args.stream,
        max_tokens=args.max_tokens,
        thinking_enabled=thinking_enabled,
    )
    body = json.dumps(payload).encode("utf-8")
    req = Request(
        url,
        data=body,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "text/event-stream" if args.stream else "application/json",
        },
    )

    print("=== request ===")
    print(f"url={url}")
    print(f"model={model}")
    print(f"stream={args.stream}")
    print(f"thinking={thinking_enabled}")
    print(f"timeout_s={timeout}")
    print(f"prompt={args.prompt}")
    print(f"env_file={args.env_file}")
    print()

    context = ssl._create_unverified_context() if args.insecure else None

    started = time.time()
    try:
        resp = urlopen(req, timeout=timeout, context=context)
    except HTTPError as exc:
        err_body = exc.read().decode("utf-8", errors="replace")
        print(f"HTTP {exc.code}", file=sys.stderr)
        print(err_body, file=sys.stderr)
        print(f"elapsed_ms={(time.time() - started) * 1000:.0f}", file=sys.stderr)
        return 1
    except URLError as exc:
        print(f"request failed: {exc}", file=sys.stderr)
        print(f"elapsed_ms={(time.time() - started) * 1000:.0f}", file=sys.stderr)
        return 1

    try:
        if args.stream:
            print("=== model reply (stream) ===")
            return _print_stream(resp, started)
        raw = resp.read()
        return _print_non_stream(raw, time.time() - started)
    finally:
        try:
            resp.close()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
