#!/usr/bin/env python3
"""Pi harness plugin for sijba — standalone, no dependency on
sijba-agent-controller's own code, same shape as sijba-cursor-plugin.

"pi" here means the real Pi Coding Agent (https://pi.dev,
npm:@earendil-works/pi-coding-agent) — a full agent CLI with its own
read/bash/edit/write tools, session management and multi-provider model
support. This plugin is a thin wrapper around that CLI (shelling out to
`pi -p --mode json`, one invocation per turn), the same relationship
sijba-cursor-plugin has to the `cursor-agent` CLI — it is NOT a bespoke
from-scratch agent loop.

Runs in Docker (see Dockerfile) rather than a shared venv/PATH, since pi is
a whole Node.js toolchain with its own global install, not a lightweight
Python dependency — isolating it avoids clashing with anything else on the
host and matches how a "real" external tool should be packaged.

Plugin contract (documented in sijba-agent-controller/README.md):
  - read SIJBA_URL / SIJBA_TOKEN / PROJECT_NAME / WORK_DIR / REGISTRY_FILE
    from the environment
  - self-register exactly one sijba channel over its own WebSocket connection
  - handle /archive and /pause locally: write REGISTRY_FILE's own status
    entry directly (no shared import — see _set_registry_status below) and
    exit with the reserved code (90 archived / 91 paused) *before* the
    process actually stops, so the controller can tell "stopped on
    purpose" apart from "crashed" purely from the exit code
  - handle /log [n] locally from an in-memory ring buffer

Environment variables
----------------------
SIJBA_URL       sijba WebSocket URL (default: wss://sijba.edv-bartl.at)
SIJBA_TOKEN     Auth token (required)
PROJECT_NAME    Registry key / trigger word (required) — e.g. "sijba-web"
WORK_DIR        Working directory for pi's tools (required) — mounted at
                /work in the docker catalog entry; pi's own $HOME is also
                pointed here (see Dockerfile) so session state survives a
                container restart
REGISTRY_FILE   Path to the controller's registry.json (required)
AI_API_URL      Unused by this plugin (pi talks to providers directly, not
                a single fixed base URL) — kept required anyway so a
                missing api= on the catalog entry fails loudly, same as
                AI_API_KEY below
AI_API_KEY      API key for AI_API_NAME's provider (required) — mapped to
                the provider-specific env var pi expects, e.g.
                AI_API_NAME=openrouter -> OPENROUTER_API_KEY
AI_API_NAME     Provider name (required) — passed to pi as --provider, and
                used to pick the *_API_KEY env var name from
                docs/providers.md's table (see _provider_env_var)
PI_MODEL        Default model id (default: openai/gpt-4o-mini) — a per-project
                override in WORK_DIR/.pi.json ("model" key) wins if present;
                set/read it live from the channel with /model [name]
PI_CMD          pi CLI command (default: pi)
"""
from __future__ import annotations

import asyncio
import collections
import fcntl
import json
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import websockets

# Exit-code contract with the controller — see sijba-agent-controller/exit_codes.py.
EXIT_ARCHIVED = 90
EXIT_PAUSED = 91

PLUGIN_NAME = "pi"  # this repo *is* the pi plugin — no TOOL switch needed

SIJBA_URL = os.environ.get("SIJBA_URL", "wss://sijba.edv-bartl.at")
SIJBA_TOKEN = os.environ.get("SIJBA_TOKEN", "")
PROJECT_NAME = os.environ.get("PROJECT_NAME", "")
WORK_DIR = Path(os.environ.get("WORK_DIR", ""))
REGISTRY_FILE = os.environ.get("REGISTRY_FILE", "")
AI_API_KEY = os.environ.get("AI_API_KEY", "")
AI_API_NAME = os.environ.get("AI_API_NAME", "")
DEFAULT_MODEL = os.environ.get("PI_MODEL", "openai/gpt-4o-mini")
PI_CMD = os.environ.get("PI_CMD", "pi")

# Per-project model override, read fresh on every turn (see current_model())
# — PI_MODEL is a catalog-level env var, shared by every project running the
# pi plugin. A model choice is naturally per-project/per-channel instead
# (one research project might want a cheap model, another a stronger one),
# so it lives in the project's own WORK_DIR rather than the shared catalog
# entry — a config file, not an env var, is the right shape for "settable
# per channel". JSON kept minimal (just `model` today) but is a dict so it
# can grow (e.g. temperature) without a format change.
CONFIG_PATH = WORK_DIR / ".pi.json"


def _load_config() -> dict:
    if not CONFIG_PATH.exists():
        return {}
    try:
        return json.loads(CONFIG_PATH.read_text())
    except (json.JSONDecodeError, OSError) as exc:
        _log(f"ignoring unreadable {CONFIG_PATH}: {exc}")
        return {}


def _save_config(cfg: dict) -> None:
    CONFIG_PATH.write_text(json.dumps(cfg, indent=2))


def current_model() -> str:
    return _load_config().get("model") or DEFAULT_MODEL


def _provider_env_var(name: str) -> str:
    """Matches the "Environment Variable" column of pi's own
    docs/providers.md table (e.g. openrouter -> OPENROUTER_API_KEY,
    anthropic -> ANTHROPIC_API_KEY) — works for every provider in that
    table since they all follow this exact pattern."""
    return f"{name.upper().replace('-', '_')}_API_KEY"


if not SIJBA_TOKEN:
    print("ERROR: SIJBA_TOKEN environment variable required", file=sys.stderr)
    sys.exit(1)
if not PROJECT_NAME:
    print("ERROR: PROJECT_NAME environment variable required", file=sys.stderr)
    sys.exit(1)
if not WORK_DIR.is_dir():
    print(f"ERROR: WORK_DIR {WORK_DIR} does not exist", file=sys.stderr)
    sys.exit(1)
if not REGISTRY_FILE:
    print("ERROR: REGISTRY_FILE environment variable required", file=sys.stderr)
    sys.exit(1)
if not AI_API_KEY or not AI_API_NAME:
    print(
        "ERROR: AI_API_KEY/AI_API_NAME required — register this plugin in the "
        "controller's catalog with an api= reference (see apis.py / "
        "'controller apis list'), or set AI_API_KEY/AI_API_NAME directly "
        "for standalone runs (AI_API_NAME must be a provider pi knows, e.g. "
        "'openrouter' — see docs/providers.md in the pi package).",
        file=sys.stderr,
    )
    sys.exit(1)

PLUGIN_ID = f"{PLUGIN_NAME}-{PROJECT_NAME}"
CHANNEL_ID = f"{PLUGIN_NAME}:{PROJECT_NAME}"

_LOG_BUFFER: collections.deque[str] = collections.deque(maxlen=200)


def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    line = f"{ts} [{PROJECT_NAME}] {msg}"
    _LOG_BUFFER.append(line)
    print(line, flush=True)


def _set_registry_status(name: str, status: str) -> None:
    """Read-modify-write REGISTRY_FILE directly — same JSON shape as
    sijba-agent-controller/registry.py, but no import of it: this repo has
    to stand on its own, same as any other git-sourced plugin would."""
    path = Path(REGISTRY_FILE)
    with open(path, "r+") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            raw = f.read().strip()
            data = json.loads(raw) if raw else {"roots": [], "projects": {}}
            data.setdefault("projects", {}).setdefault(name, {})["status"] = status
            data["projects"][name]["status_changed_at"] = time.time()
            tmp = path.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(data, indent=2, sort_keys=True))
            os.replace(tmp, path)
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


# ---------------------------------------------------------------------------
# Pi backend — one `pi -p --mode json` invocation per turn, streamed. Same
# "fresh process per turn, resumed via a stable id" shape as
# sijba-cursor-plugin's CursorSession, but pi's own --session-id (rather
# than a session id we'd have to remember from a prior response) lets us
# pick the id ourselves — PROJECT_NAME by default, bumped by /reset.
# ---------------------------------------------------------------------------

MAX_CHARS_TO_UI = 8000


def _truncate(text: str, limit: int = MAX_CHARS_TO_UI) -> str:
    if len(text) <= limit:
        return text
    return text[:limit] + f"\n…[truncated, {len(text) - limit} more chars]"


async def _iter_json_lines(stream: asyncio.StreamReader):
    """Yield complete newline-delimited lines with no hard size limit — see
    sijba-cursor-plugin/main.py's identical helper for why plain
    `readline()` (64KiB buffer, raises on overrun) isn't safe here: a
    tool-call event embedding a large file read/diff can exceed that
    before hitting its own newline."""
    buf = bytearray()
    while True:
        chunk = await stream.read(65536)
        if not chunk:
            if buf:
                yield bytes(buf)
            return
        buf.extend(chunk)
        while True:
            idx = buf.find(b"\n")
            if idx < 0:
                break
            yield bytes(buf[:idx])
            del buf[: idx + 1]


class PiSession:
    def __init__(self, work_dir: Path):
        self.work_dir = work_dir
        self.session_id = f"sijba-{PROJECT_NAME}"
        self.current_proc: asyncio.subprocess.Process | None = None

    async def reset(self) -> None:
        """No API call to "clear" a pi session — start a differently-named
        one instead, so --session-id creates it fresh on the next turn
        while the old session file is simply abandoned (still on disk,
        inspectable, just never --session-id'd again)."""
        self.session_id = f"sijba-{PROJECT_NAME}-{int(time.time())}"
        _log(f"Pi session reset -> {self.session_id}")

    async def kill(self) -> bool:
        """Abort whatever turn is currently running, then reset — same
        effect as /reset, plus actually terminating a still-running pi
        invocation instead of waiting it out. Returns whether anything was
        actually running to kill."""
        proc = self.current_proc
        killed = bool(proc and proc.returncode is None)
        if killed:
            proc.kill()
        await self.reset()
        return killed

    async def send(self, text: str, on_chunk=None) -> dict:
        """Returns {"text", "model", "usage"}. `usage` is pi's own shape
        ({input, output, cacheRead, cacheWrite, totalTokens, cost}), not
        OpenAI's {prompt_tokens, ...} — see the channel_update call site
        below for the field name this maps to."""
        model = current_model()
        cmd = [
            PI_CMD, "-p", "--mode", "json",
            "--provider", AI_API_NAME, "--model", model,
            "--session-id", self.session_id,
            "--", text,
        ]
        env = dict(os.environ)
        env[_provider_env_var(AI_API_NAME)] = AI_API_KEY

        proc = None
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd, cwd=self.work_dir, env=env,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
            self.current_proc = proc
            final_text = ""
            model_used = model
            usage: dict = {}
            stderr_buf = bytearray()

            async def _pump_stdout() -> None:
                nonlocal final_text, model_used, usage
                async for line in _iter_json_lines(proc.stdout):
                    try:
                        event = json.loads(line)
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        continue
                    etype = event.get("type")
                    if etype == "message_update":
                        ev = event.get("assistantMessageEvent") or {}
                        ev_type = ev.get("type")
                        if ev_type == "text_delta" and on_chunk:
                            await on_chunk(_truncate(ev.get("delta", "")))
                        elif ev_type == "thinking_end" and on_chunk:
                            # One notice per reasoning block, not one per
                            # token — thinking_delta used to fall into the
                            # generic branch below and fire on every single
                            # delta (found live: hundreds of "_[thinking_
                            # delta]_" chunks flooding one turn). thinking_end
                            # carries the whole block in `content`, same
                            # shape as text_end, so a short preview here is
                            # free — no need to buffer deltas ourselves.
                            preview = _truncate(ev.get("content", "").strip(), 280)
                            if preview:
                                await on_chunk(f"\n_[thinking: {preview}]_\n")
                        elif ev_type not in (
                            "text_start", "text_delta", "text_end",
                            "thinking_start", "thinking_delta", "thinking_end",
                        ) and on_chunk:
                            # Tool calls and anything else pi's schema adds
                            # later surface here too — better a generic
                            # notice than silently dropping activity the
                            # channel would otherwise show nothing for.
                            await on_chunk(f"\n_[{ev_type or 'event'}]_\n")
                    elif etype == "turn_end":
                        msg = event.get("message") or {}
                        final_text = "".join(
                            b.get("text", "") for b in msg.get("content", []) if b.get("type") == "text"
                        )
                        final_text = _truncate(final_text)
                        model_used = msg.get("model", model_used)
                        usage = msg.get("usage") or {}

            async def _pump_stderr() -> None:
                while True:
                    chunk = await proc.stderr.read(4096)
                    if not chunk:
                        break
                    stderr_buf.extend(chunk)

            await asyncio.wait_for(asyncio.gather(_pump_stdout(), _pump_stderr()), timeout=600)
            await proc.wait()

            stderr_text = bytes(stderr_buf).decode(errors="replace").strip()
            if stderr_text:
                _log(f"stderr: {stderr_text}")
            _log(
                f"usage model={model_used} in={usage.get('input', 0)} "
                f"cacheRead={usage.get('cacheRead', 0)} out={usage.get('output', 0)}"
            )

            if proc.returncode != 0 and not final_text:
                detail = stderr_text or f"exited {proc.returncode}, no output"
                _log(f"invocation failed (exit {proc.returncode})")
                return {"text": f"Error: pi failed — {_truncate(detail, 800)}"}

            return {"text": final_text or "(no output)", "model": model_used, "usage": usage}
        except asyncio.TimeoutError:
            if proc and proc.returncode is None:
                proc.kill()
            return {"text": "Error: pi timeout (600s)"}
        except Exception as exc:
            if proc and proc.returncode is None:
                proc.kill()
            return {"text": f"Error: {exc}"}
        finally:
            self.current_proc = None

    async def close(self) -> None:
        _log("Pi session closed")


# ---------------------------------------------------------------------------
# sijba plugin protocol (single project channel, self-registered) — same
# shape as sijba-cursor-plugin/main.py's PluginClient.
# ---------------------------------------------------------------------------

def _strip_trigger(text: str) -> str:
    for trigger in (PROJECT_NAME, CHANNEL_ID):
        if text.lower().startswith(trigger.lower()):
            return text[len(trigger):].strip()
    return text.strip()


def _is_kill_command(text: str) -> bool:
    stripped = _strip_trigger(text)
    if not stripped.startswith("/"):
        return False
    cmd, _, _ = stripped[1:].partition(" ")
    return cmd.lower().strip() == "kill"


class PluginClient:
    def __init__(self):
        self.ws = None
        self.session = PiSession(WORK_DIR)
        self.shutdown_event = asyncio.Event()
        self.exit_code = 0
        self._reconnect_backoff = 5
        self._msg_queue: asyncio.Queue = asyncio.Queue()
        self._worker_task: asyncio.Task | None = None
        self._inflight: dict[str, asyncio.Event] = {}
        self._completed_replies: dict[str, str] = {}

    async def connect(self):
        ws_url = SIJBA_URL.replace("https://", "wss://").replace("http://", "ws://")
        ws_url = f"{ws_url}/api/plugin/ws?token={SIJBA_TOKEN}"
        _log(f"Connecting to {ws_url} as {PLUGIN_ID}")
        self.ws = await websockets.connect(
            ws_url, ping_interval=30, ping_timeout=60, close_timeout=5,
        )
        _log("Connected!")

        await self.ws.send(json.dumps({
            "type": "register",
            "plugin_id": PLUGIN_ID,
            "capabilities": ["conversation", "tools", "files", "code", "agent"],
            "channels": {
                CHANNEL_ID: {
                    "label": f"Pi: {PROJECT_NAME}",
                    "hint": (
                        f"Pi coding agent (pi.dev) for project '{PROJECT_NAME}' at {WORK_DIR}, "
                        f"model {current_model()} via {AI_API_NAME}. "
                        f"Trigger: '@{PROJECT_NAME} ...'. "
                        f"In-channel commands: /archive, /pause, /reset, /kill, /model [name], /log [n]."
                    ),
                    "triggers": [f"@{PROJECT_NAME}", f"@{CHANNEL_ID}"],
                    "capabilities": ["code", "analysis", "debugging", "files", "terminal", "refactor", "test"],
                    "metadata": {"plugin": PLUGIN_NAME, "project": PROJECT_NAME, "working_dir": str(WORK_DIR)},
                }
            },
        }))
        self._reconnect_backoff = 5
        self._worker_task = asyncio.create_task(self._message_worker())
        try:
            await self.message_loop()
        finally:
            if self._worker_task is not None:
                self._worker_task.cancel()
                try:
                    await self._worker_task
                except asyncio.CancelledError:
                    pass
                self._worker_task = None

    async def _message_worker(self) -> None:
        while True:
            msg = await self._msg_queue.get()
            try:
                await self.handle_message(msg)
            except Exception as exc:
                _log(f"handle_message error: {exc}")
            finally:
                self._msg_queue.task_done()

    async def message_loop(self):
        try:
            async for raw in self.ws:
                msg = json.loads(raw)
                mtype = msg.get("type")
                if mtype == "registered":
                    _log(f"Plugin registered: {msg['plugin_id']}")
                elif mtype == "channel_registered":
                    _log(f"Channel registered: {msg['channel_id']} (ok={msg.get('ok')})")
                elif mtype == "message":
                    if _is_kill_command(msg["payload"]["text"]):
                        # Bypasses _msg_queue on purpose: the one-turn-at-a-
                        # time worker would otherwise queue /kill behind the
                        # very turn it's meant to abort, making it useless
                        # against a stuck/looping agent. Runs concurrently
                        # with whatever the worker is currently doing.
                        asyncio.create_task(self._handle_kill(msg))
                    else:
                        await self._msg_queue.put(msg)
                elif mtype == "ping":
                    await self.ws.send(json.dumps({"type": "pong"}))
                elif mtype == "error":
                    _log(f"Server error: {msg.get('message')}")
        except websockets.ConnectionClosed:
            _log("Connection closed")
        except Exception as exc:
            _log(f"Message loop error: {exc}")

    def _remember_reply(self, envelope_id: str, text: str) -> None:
        self._completed_replies[envelope_id] = text
        while len(self._completed_replies) > 50:
            oldest = next(iter(self._completed_replies))
            self._completed_replies.pop(oldest, None)

    async def _reply(self, channel_id: str, envelope_id: str, text: str) -> None:
        try:
            await self.ws.send(json.dumps({
                "type": "reply", "channel": channel_id, "envelope_id": envelope_id,
                "text": text, "continue": True,
            }))
        except Exception as exc:
            _log(f"Failed to send reply: {exc}")

    async def _handle_kill(self, msg: dict) -> None:
        """See message_loop — runs outside _msg_queue so it takes effect
        immediately against a running turn instead of waiting behind it."""
        channel_id = msg["channel"]
        envelope_id = msg["payload"]["envelope_id"]
        killed = await self.session.kill()
        reply = (
            f"{PROJECT_NAME}: killed running turn, session reset — next message starts fresh."
            if killed else
            f"{PROJECT_NAME}: nothing running, session reset anyway."
        )
        _log(f"killed via /kill (had_running_turn={killed})")
        self._remember_reply(envelope_id, reply)
        await self._reply(channel_id, envelope_id, reply)

    async def _handle_local_command(self, channel_id: str, envelope_id: str, text: str) -> bool:
        cmd, _, rest = text[1:].partition(" ")
        cmd = cmd.lower().strip()

        if cmd == "log":
            n = 10
            if rest.strip().isdigit():
                n = min(int(rest.strip()), len(_LOG_BUFFER) or 10)
            reply = "\n".join(list(_LOG_BUFFER)[-n:]) or "(no logs yet)"
            self._remember_reply(envelope_id, reply)
            await self._reply(channel_id, envelope_id, reply)
            return True

        if cmd == "model":
            new_model = rest.strip()
            if not new_model:
                cfg_model = _load_config().get("model")
                source = f"set for this project ({CONFIG_PATH.name})" if cfg_model else f"default (PI_MODEL={DEFAULT_MODEL})"
                reply = f"Current model: {current_model()} — {source}"
            else:
                cfg = _load_config()
                cfg["model"] = new_model
                _save_config(cfg)
                reply = f"Model for {PROJECT_NAME} set to {new_model} (next turn uses it)."
                _log(f"model changed to {new_model!r} via /model")
            self._remember_reply(envelope_id, reply)
            await self._reply(channel_id, envelope_id, reply)
            return True

        if cmd in ("archive", "pause"):
            status = "archived" if cmd == "archive" else "paused"
            code = EXIT_ARCHIVED if cmd == "archive" else EXIT_PAUSED
            try:
                _set_registry_status(PROJECT_NAME, status)
            except Exception as exc:
                reply = f"Error updating registry: {exc}"
                self._remember_reply(envelope_id, reply)
                await self._reply(channel_id, envelope_id, reply)
                return True
            reply = f"{PROJECT_NAME} {status}. Stopping."
            self._remember_reply(envelope_id, reply)
            await self._reply(channel_id, envelope_id, reply)
            _log(f"{status} via /{cmd} — shutting down (exit {code})")
            self.exit_code = code
            self.shutdown_event.set()
            await self.ws.close()
            return True

        if cmd == "reset":
            await self.session.reset()
            reply = f"{PROJECT_NAME} reset — next message starts a fresh pi session, no prior context."
            self._remember_reply(envelope_id, reply)
            await self._reply(channel_id, envelope_id, reply)
            return True

        if cmd:
            reply = "Unsupported here: /archive, /pause, /reset, /kill, /model [name], /log [n]. Anything else — just plain text."
            self._remember_reply(envelope_id, reply)
            await self._reply(channel_id, envelope_id, reply)
            return True
        return False

    async def handle_message(self, msg: dict):
        channel_id = msg["channel"]
        payload = msg["payload"]
        envelope_id = payload["envelope_id"]
        text = payload["text"]

        if envelope_id in self._completed_replies:
            _log(f"redelivery of completed envelope={envelope_id} — resending reply")
            await self._reply(channel_id, envelope_id, self._completed_replies[envelope_id])
            return
        if envelope_id in self._inflight:
            _log(f"redelivery of in-flight envelope={envelope_id} — waiting")
            await self._inflight[envelope_id].wait()
            if envelope_id in self._completed_replies:
                await self._reply(channel_id, envelope_id, self._completed_replies[envelope_id])
            return

        done = asyncio.Event()
        self._inflight[envelope_id] = done
        try:
            await self._handle_message_inner(channel_id, envelope_id, text)
        finally:
            done.set()
            self._inflight.pop(envelope_id, None)

    async def _handle_message_inner(self, channel_id: str, envelope_id: str, text: str) -> None:
        text = _strip_trigger(text)
        if not text:
            self._remember_reply(envelope_id, "")
            await self._reply(channel_id, envelope_id, "")
            return

        if text.lstrip().startswith("/"):
            if await self._handle_local_command(channel_id, envelope_id, text.lstrip()):
                return

        async def _on_chunk(delta: str) -> None:
            try:
                await self.ws.send(json.dumps({
                    "type": "reply_chunk", "channel": channel_id, "envelope_id": envelope_id, "text": delta,
                }))
            except Exception as exc:
                _log(f"Failed to send reply_chunk: {exc}")

        _log(f"handling envelope={envelope_id} text={text[:120]!r}")
        try:
            response = await self.session.send(text, on_chunk=_on_chunk)
        except Exception as exc:
            response = {"text": f"Error: {exc}"}
        reply_text = response.get("text", "")

        if response.get("model"):
            try:
                await self.ws.send(json.dumps({
                    "type": "channel_update", "channel_id": channel_id,
                    "metadata": {
                        "model": response["model"],
                        "context_tokens": (response.get("usage") or {}).get("input", 0),
                    },
                }))
            except Exception as exc:
                _log(f"Failed to send channel_update: {exc}")

        self._remember_reply(envelope_id, reply_text)
        await self._reply(channel_id, envelope_id, reply_text)

    async def run_forever(self):
        loop = asyncio.get_event_loop()

        def _request_shutdown() -> None:
            self.shutdown_event.set()
            if self.ws is not None:
                asyncio.ensure_future(self.ws.close())

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, _request_shutdown)
            except NotImplementedError:
                pass

        while not self.shutdown_event.is_set():
            try:
                await self.connect()
            except Exception as exc:
                _log(f"Connection failed: {exc}")
            if self.shutdown_event.is_set():
                break
            _log(f"Reconnecting in {self._reconnect_backoff}s...")
            try:
                await asyncio.wait_for(self.shutdown_event.wait(), timeout=self._reconnect_backoff)
            except asyncio.TimeoutError:
                pass
            self._reconnect_backoff = min(self._reconnect_backoff * 2, 300)

        await self.session.close()
        _log("Shutdown complete")


async def main() -> int:
    client = PluginClient()
    await client.run_forever()
    return client.exit_code


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
