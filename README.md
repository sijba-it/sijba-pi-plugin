# sijba-pi-plugin

Sijba wrapper around the real [Pi Coding Agent](https://pi.dev)
(`npm:@earendil-works/pi-coding-agent`) — a full agent CLI with its own
read/bash/edit/write tools, session management, and multi-provider model
support. This repo is a thin `kind: git`/`kind: docker` catalog plugin for
[sijba-agent-controller](https://REDACTED-GIT-HOST/sijba/sijba-agent-controller),
the same relationship `sijba-cursor-plugin` has to `cursor-agent` — it
shells out to the `pi` CLI (`pi -p --mode json`, one invocation per turn),
it does not reimplement an agent loop itself.

## Status

Working: sijba protocol (register/archive/pause/reset/log/model, exit
codes), real tool use (bash/read/edit/write via pi itself, not this
wrapper), per-project model selection, streaming tool/text events relayed
into the channel, session continuity across turns via pi's own
`--session-id`.

## Runs in Docker

Pi is a whole Node.js toolchain (global npm install), not a lightweight
Python dependency — packaging it in its own image avoids clashing with
anything else on the host, unlike running it from a shared venv/PATH.

```bash
cd sijba-pi-plugin
docker build -t sijba-pi:latest .
```

## Quick start (standalone, outside the controller)

```bash
docker run --rm \
  -v /path/to/project:/work \
  -v /path/to/registry.json:/registry.json \
  -e SIJBA_URL=https://sijba.edv-bartl.at \
  -e SIJBA_TOKEN=your-token-here \
  -e PROJECT_NAME=my-project \
  -e WORK_DIR=/work \
  -e REGISTRY_FILE=/registry.json \
  -e AI_API_NAME=openrouter \
  -e AI_API_KEY=your-openrouter-key \
  -e HOME=/work \
  sijba-pi:latest
```

`HOME=/work` matters: it's where pi keeps its own state
(`~/.pi/agent/sessions/`, `models-store.json`) — pointing it at the
already-bind-mounted project directory means that state survives a
container restart (`docker run --rm` otherwise starts a fresh, empty
`$HOME` every time, silently losing all session continuity).

`AI_API_NAME` must be a provider pi itself knows (see
`docs/providers.md` inside the installed npm package, or
[pi.dev](https://pi.dev)) — `openrouter` is the one sijba already has
configured. It's passed to pi as `--provider`, and used to derive the
provider-specific env var pi expects (`openrouter` → `OPENROUTER_API_KEY`,
`anthropic` → `ANTHROPIC_API_KEY`, ...) from `AI_API_KEY`.

## Managed by the controller

```
controller catalog add pi --docker sijba-pi:latest --api openrouter --env HOME=/work
```

(Already registered in this controller's `catalog.json`.) `ensure_fetched`
tries `docker pull` first but falls back to a local image if the pull
fails and the image already exists — so a locally-built, never-pushed
image (as above) works fine on a single-host setup; push it to a registry
instead if you need this on more than one host.

## In-channel commands

- `/archive`, `/pause` — same contract as every other plugin (write
  `REGISTRY_FILE`'s status, exit with the reserved code).
- `/reset` — abandons the current pi session id and starts a fresh one on
  the next turn (no prior context). The old session file stays on disk,
  just never `--session-id`'d again.
- `/model [name]` — no argument shows the current effective model and
  where it came from; with an argument, sets it for *this project only*
  (written to `WORK_DIR/.pi.json`, read fresh every turn, no restart
  needed). See "Changing the model" below.
- `/log [n]` — last `n` (default 10) of this process's own log lines.

## Changing the model

Two levels:

- **Per-project (recommended)** — set it from the channel itself:
  `/model anthropic/claude-3.5-sonnet`. Written to `WORK_DIR/.pi.json`
  (`{"model": "..."}`), read fresh on every turn — no restart needed, and
  it only affects this one project. `/model` with no argument shows the
  current effective model and where it came from. This is the right knob
  for "this research project wants a stronger/cheaper model than the
  others" — an env var can't express that since it's shared by every
  project on the `pi` plugin.
- **Catalog-wide default** — `PI_MODEL` env var (default
  `openai/gpt-4o-mini`), used by any project with no `.pi.json` override:

  ```
  controller catalog set-env pi --env PI_MODEL=anthropic/claude-3.5-sonnet
  ```

  Takes effect on the plugin's next (re)start (`controller archive <name>`
  + `restore <name>`, or its next crash-loop retry).

Provider (`AI_API_NAME`, currently always `openrouter`) is a catalog-level
setting via `api=`, not per-project — switching provider means adding
another entry to `apis.json` and pointing a *different* catalog plugin
(e.g. `pi-anthropic`) at it, since `--provider` and its API key are
resolved once per plugin, not per project.
