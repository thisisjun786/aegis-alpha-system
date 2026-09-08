#!/usr/bin/env bash
# Shared preparation for standalone lanes and CI's setup action.
#
# Interface (source this file from any cwd):
#   export AAS_VERIFY_PROFILE=full   # full (default) or style
#   ensure_verify_toolchain
# This exports AAS_VERIFY_PREPARED, AAS_VERIFY_PROFILE, UV_PROJECT_ENVIRONMENT,
# AAS_VERIFY_PYTHON, UV_PYTHON and UV_OFFLINE=1. CI persists the token and profile
# across steps (also UV_PROJECT_ENVIRONMENT if customized from checkout/.venv).
# Child shells inherit them automatically. Do not cache the token.
# A set token (even empty) requests validation ONLY; missing/stale preparation
# fails without installs or sync. Unset AAS_VERIFY_PREPARED to prepare anew.
# Tokens bind root, environment, interpreter identity/version, profile and inputs.
# They detect stale setup; they are not a security boundary against a hostile job.
# The lock covers setup only. Concurrent standalone lanes sharing an environment
# are unsupported. CI isolates runners; verify prepares before starting children.
AAS_UV_VERSION=0.11.32
_AAS_VERIFY_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"

verify_prepared_fingerprint() {
  [[ -x "$AAS_VERIFY_PYTHON" ]] || {
    echo "verify: prepared interpreter is missing: $AAS_VERIFY_PYTHON" >&2; return 1;
  }
  "$AAS_VERIFY_PYTHON" -I - "$_AAS_VERIFY_ROOT" "$UV_PROJECT_ENVIRONMENT" \
    "$AAS_VERIFY_PROFILE" "$AAS_UV_VERSION" <<'PY'
import hashlib
import json
import os
import sys
from pathlib import Path

root, environment = (Path(arg).resolve() for arg in sys.argv[1:3])
requested = tuple(map(int, (root / ".python-version").read_text().strip().split(".")))
if sys.version_info[:len(requested)] != requested:
    sys.exit("verify: prepared interpreter version does not match .python-version")
executable = environment / "bin/python"
if Path(sys.prefix).resolve() != environment or Path(os.path.abspath(sys.executable)) != executable:
    sys.exit("verify: prepared interpreter path/prefix does not match the environment")
identity = [str(root), str(environment), str(executable.resolve()), sys.version, *sys.argv[3:]]
for path in (executable, executable.resolve(), environment / "pyvenv.cfg"):
    info = path.stat()
    identity.append([info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns])
for name in (".python-version", "pyproject.toml", "uv.lock", "scripts/verify-lib.sh"):
    identity.append([name, hashlib.sha256((root / name).read_bytes()).hexdigest()])
digest = hashlib.sha256(json.dumps(identity).encode()).hexdigest()
print(f"v1:{sys.argv[3]}:{digest}")
PY
}

prepare_verify_environment() (
  # Lock the parent directory across BOTH python install and sync. Its inode
  # survives venv replacement, and no lock file is left in the checkout.
  local setup_fd
  command -v flock >/dev/null || { echo "verify: flock is required" >&2; return 1; }
  exec {setup_fd}<"$(dirname "$UV_PROJECT_ENVIRONMENT")" || return
  flock -n "$setup_fd" || { echo "verify: another setup owns this environment" >&2; return 1; }
  local version
  version="$(cat "$_AAS_VERIFY_ROOT/.python-version")" || return
  uv python install "$version" || return
  if [[ "$AAS_VERIFY_PROFILE" == style ]]; then
    uv sync --project "$_AAS_VERIFY_ROOT" --python "$version" --locked --only-group dev --no-install-project || return
  else
    uv sync --project "$_AAS_VERIFY_ROOT" --python "$version" --locked --dev || return
  fi
)

ensure_verify_toolchain() {
  export AAS_VERIFY_PROFILE="${AAS_VERIFY_PROFILE-full}"
  case "$AAS_VERIFY_PROFILE" in
    style|full) ;;
    *) echo "verify: AAS_VERIFY_PROFILE must be style or full" >&2; return 2 ;;
  esac
  local environment="${UV_PROJECT_ENVIRONMENT-$_AAS_VERIFY_ROOT/.venv}" observed
  [[ "$environment" == /* ]] || environment="$_AAS_VERIFY_ROOT/$environment"
  export UV_PROJECT_ENVIRONMENT="$environment"
  export AAS_VERIFY_PYTHON="$environment/bin/python"
  if [[ ! ${AAS_VERIFY_PREPARED+x} ]] && ! command -v uv >/dev/null 2>&1; then
    python3 -m pip install --quiet --user "uv==$AAS_UV_VERSION" || return
    export PATH="${HOME}/.local/bin:${PATH}"
  fi
  command -v uv >/dev/null || { echo "verify: uv is unavailable" >&2; return 1; }
  observed="$(uv --version)" || return
  [[ "$observed" == "uv $AAS_UV_VERSION" || "$observed" == "uv $AAS_UV_VERSION "* ]] || {
    echo "verify: expected uv $AAS_UV_VERSION; got $observed" >&2; return 1;
  }
  if [[ ${AAS_VERIFY_PREPARED+x} ]]; then
    observed="$(verify_prepared_fingerprint)" || return
    [[ -n "$AAS_VERIFY_PREPARED" && "$AAS_VERIFY_PREPARED" == "$observed" ]] || {
      echo "verify: missing or stale AAS_VERIFY_PREPARED; rerun setup with the token unset" >&2
      return 1
    }
  else
    prepare_verify_environment || return
    AAS_VERIFY_PREPARED="$(verify_prepared_fingerprint)" || return
    export AAS_VERIFY_PREPARED
  fi
  export UV_PYTHON="$AAS_VERIFY_PYTHON" UV_OFFLINE=1
}

# Run an owned command so a signal can interrupt wait and the lane's EXIT trap
# can reap the client. Resource-owning lanes additionally clean up their artifacts.
verify_run() {
  local rc=0
  setsid "$@" &
  AAS_VERIFY_CHILD_PID=$!
  wait "$AAS_VERIFY_CHILD_PID" || rc=$?
  AAS_VERIFY_CHILD_PID=""
  return "$rc"
}

verify_stop_child() {
  if [[ -n "${AAS_VERIFY_CHILD_PID:-}" ]]; then
    local grace_seconds=5 deadline
    deadline=$((SECONDS + grace_seconds))
    kill -TERM -- "-$AAS_VERIFY_CHILD_PID" 2>/dev/null || \
      kill -TERM "$AAS_VERIFY_CHILD_PID" 2>/dev/null || true
    while (( SECONDS < deadline )); do
      if ! kill -0 -- "-$AAS_VERIFY_CHILD_PID" 2>/dev/null && \
          ! kill -0 "$AAS_VERIFY_CHILD_PID" 2>/dev/null; then
        break
      fi
      sleep 0.1
    done
    # Include descendants that outlived the command. The session/process group
    # belongs to verify_run (or the dispatcher's timeout), never to our caller.
    kill -KILL -- "-$AAS_VERIFY_CHILD_PID" 2>/dev/null || \
      kill -KILL "$AAS_VERIFY_CHILD_PID" 2>/dev/null || true
    wait "$AAS_VERIFY_CHILD_PID" 2>/dev/null || true
    AAS_VERIFY_CHILD_PID=""
  fi
}

# Independent hand-calculated oracle for examples/portfolio-preview.json.
# Validate actual CLI output, without importing the application under test.
verify_aas_smoke_json() {
  "$AAS_VERIFY_PYTHON" -I - "$@" <<'PY'
import json
import math
import sys
from pathlib import Path

status, preview = (json.loads(Path(path).read_text()) for path in sys.argv[1:])
if (status["application"] != "aegis-alpha-system"
        or status["mode"] != "standalone-cli"
        or status["capabilities"]["allocation_preview"] is not True
        or status["capabilities"]["live_orders"] is not False
        or status["runtime_dependencies"]["database_for_preview"] is not False):
    sys.exit("verify: unexpected aas status JSON")
expected = [
    {"instrument_id": "asset:equity-basket", "weight": 0.58},
    {"instrument_id": "asset:protective-basket", "weight": 0.1},
    {"instrument_id": "asset:stock-b", "weight": 0.15},
]
if (type(preview["schema_version"]) is not int or preview["schema_version"] != 1
        or preview["as_of"] != "2026-09-05"
        or preview["execution_enabled"] is not False
        or preview["positions"] != expected
        or not math.isclose(preview["cash_weight"], 0.17, rel_tol=0, abs_tol=1e-12)
        or [item["module"] for item in preview["modules"]] != ["aegis", "alpha", "hedge"]):
    sys.exit("verify: unexpected aas preview JSON")
print("aas status/preview JSON: pass")
PY
}
