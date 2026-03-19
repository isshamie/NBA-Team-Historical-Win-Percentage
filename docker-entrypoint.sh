#!/usr/bin/env bash
set -euo pipefail

build_args_from_env() {
  local build_sources="${BUILD_SOURCES:-nba_api}"
  local normalized="${build_sources//,/ }"
  local -a args=()

  if [[ "${normalized}" != "none" ]]; then
    if [[ "${normalized}" == "all" ]]; then
      args+=(--sources nba_api basketball_reference)
    else
      # shellcheck disable=SC2206
      local sources=( ${normalized} )
      args+=(--sources "${sources[@]}")
    fi
  fi

  if [[ "${FORCE_REFRESH:-0}" == "1" ]]; then
    args+=(--force-refresh)
  elif [[ "${REBUILD_DATASET:-0}" == "1" ]]; then
    args+=(--rebuild-dataset)
  fi

  if [[ "${SKIP_PLOT:-0}" == "1" ]]; then
    args+=(--skip-plot)
  fi

  printf '%s\n' "${args[@]}"
}


run_build_from_env_if_requested() {
  local -a args=()
  mapfile -t args < <(build_args_from_env)
  if [[ ${#args[@]} -eq 0 ]]; then
    return
  fi
  python build_data.py "${args[@]}"
}


command="${1:-app}"
if [[ $# -gt 0 ]]; then
  shift
fi

case "${command}" in
  app)
    run_build_from_env_if_requested
    exec streamlit run app.py \
      --server.address 0.0.0.0 \
      --server.port "${PORT:-8501}" \
      --server.headless true \
      "$@"
    ;;
  build-datasets)
    exec python build_data.py "$@"
    ;;
  *)
    exec "${command}" "$@"
    ;;
esac
