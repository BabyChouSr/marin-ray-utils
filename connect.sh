#!/usr/bin/env bash
# Simple Ray cluster switcher + helpers
# Source me:   source ./connect.sh
# Usage:       use_cluster marin-us-east1-d-vllm
#              raysubmit -- python your_job.py --arg val
#              raystatus
#              rayattach

# ---- 1) Map cluster name/yaml -> address (edit as you wish) -----------------
# Keys can be with or without .yaml; values can be with or without http://
declare -A CLUSTER_MAP=(
  ["marin-us-east1-d-vllm.yaml"]="localhost:8267"
  ["marin-us-east1-d-vllm"]="localhost:8267"
  ["marin-us-central1.yaml"]="localhost:8266"
  ["marin-us-central1"]="localhost:8266"
  ["marin-us-central2"]="localhost:8268"
  ["marin-us-central2.yaml"]="localhost:8268"
)

# Optional: default cluster if none set
: "${DEFAULT_CLUSTER:=marin-us-east1-d-vllm}"

# ---- 2) Helpers -------------------------------------------------------------
_norm_http() {
  local addr="$1"
  if [[ "$addr" != http* ]]; then
    echo "http://$addr"
  else
    echo "$addr"
  fi
}

_lookup_addr() {
  local key="$1"
  # Try exact, basename, with/without .yaml
  if [[ -n "${CLUSTER_MAP[$key]}" ]]; then
    echo "$(_norm_http "${CLUSTER_MAP[$key]}")"; return 0
  fi
  local base="${key##*/}"
  if [[ -n "${CLUSTER_MAP[$base]}" ]]; then
    echo "$(_norm_http "${CLUSTER_MAP[$base]}")"; return 0
  fi
  local noyaml="${base%.yaml}"
  if [[ -n "${CLUSTER_MAP[$noyaml]}" ]]; then
    echo "$(_norm_http "${CLUSTER_MAP[$noyaml]}")"; return 0
  fi
  return 1
}

# ---- 3) Public functions ----------------------------------------------------

# Select a cluster by logical name or yaml filename
use_cluster() {
  local name="${1:-$DEFAULT_CLUSTER}"
  local addr
  if ! addr="$(_lookup_addr "$name")"; then
    echo "✗ Unknown cluster: $name"
    echo "• Known keys: ${!CLUSTER_MAP[@]}"
    return 1
  fi
  export RAY_CLUSTER="$name"
  export RAY_ADDRESS="$addr"
  echo "✓ RAY_CLUSTER=$RAY_CLUSTER"
  echo "✓ RAY_ADDRESS=$RAY_ADDRESS"
}

# Submit a Ray job to the currently selected cluster.
# Pass args exactly as you'd give to `ray job submit` (we add --address for you).
# Examples:
#   raysubmit -- python train.py --lr 3e-5
#   raysubmit --working-dir . -- python script.py
raysubmit() {
  if [[ -z "$RAY_ADDRESS" ]]; then
    echo "RAY_ADDRESS not set. Run: use_cluster <name>"; return 1
  fi

  # If you keep a runtime_env.yaml next to your code, we’ll auto-apply it.
  local extra=()
  if [[ -n "$RAY_RUNTIME_ENV" && -f "$RAY_RUNTIME_ENV" ]]; then
    # Convert YAML -> JSON on the fly using Python (no internet needed)
    local envjson
    envjson="$(python - <<'PY'
import json, sys
try:
    import yaml
except Exception:
    yaml=None
p=sys.argv[1]
if yaml is None:
    # If PyYAML isn't available, just print empty JSON object.
    print("{}")
else:
    with open(p, "r") as f:
        print(json.dumps(yaml.safe_load(f) or {}))
PY
"$RAY_RUNTIME_ENV" 2>/dev/null)"
    extra+=( --runtime-env-json "$envjson" )
    echo "• Using runtime env: $RAY_RUNTIME_ENV"
  elif [[ -f runtime_env.yaml ]]; then
    local envjson
    envjson="$(python - <<'PY'
import json, sys
try:
    import yaml
except Exception:
    yaml=None
p="runtime_env.yaml"
if yaml is None:
    print("{}")
else:
    with open(p, "r") as f:
        print(json.dumps(yaml.safe_load(f) or {}))
PY
2>/dev/null)"
    extra+=( --runtime-env-json "$envjson" )
    echo "• Using runtime env: runtime_env.yaml"
  fi

  ray job submit --address "$RAY_ADDRESS" "${extra[@]}" "$@"
}

# Quick status
raystatus() {
  if [[ -z "$RAY_ADDRESS" ]]; then
    echo "RAY_ADDRESS not set. Run: use_cluster <name>"; return 1
  fi
  ray status --address "$RAY_ADDRESS"
}

# Attach a Ray dashboard link / print it
raydash() {
  if [[ -z "$RAY_ADDRESS" ]]; then
    echo "RAY_ADDRESS not set. Run: use_cluster <name>"; return 1
  fi
  echo "Dashboard: $RAY_ADDRESS"
}

# Attach shell to the head (if you already have SSH aliasing set up).
# Customize this for your environment if you want fancier behavior.
rayattach() {
  if [[ -z "$RAY_CLUSTER" ]]; then
    echo "RAY_CLUSTER not set. Run: use_cluster <name>"; return 1
  fi
  echo "Tip: customize rayattach() for SSH into the right head node if desired."
}

# On source, auto-select default (edit DEFAULT_CLUSTER above)
if [[ -z "$RAY_ADDRESS" ]]; then
  use_cluster "$DEFAULT_CLUSTER" >/dev/null 2>&1 || true
fi

# Convenience: print a short help
rayhelp() {
  cat <<'EOF'
Commands:
  use_cluster <name>        # set RAY_CLUSTER and RAY_ADDRESS
  raysubmit [args...]       # wrapper for `ray job submit` (adds --address and runtime_env if present)
  raystatus                 # wrapper for `ray status`
  raydash                   # echos dashboard address
  rayattach                 # customize to SSH into cluster head

Env vars:
  RAY_CLUSTER               # logical name, e.g. marin-us-east1-d-vllm
  RAY_ADDRESS               # http://host:port (set by use_cluster)
  RAY_RUNTIME_ENV           # optional path to a runtime_env.yaml

Edit CLUSTER_MAP in connect.sh to add/update mappings.
EOF
}
