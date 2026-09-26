#!/usr/bin/env bash
# Copyright (C) 2026 EloqData Inc.
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail
umask 077

has_option() {
  local option=$1 argument
  shift
  for argument; do
    [[ $argument == "$option" || $argument == "$option="* ]] && return 0
  done
  return 1
}

if [[ $# == 0 ]]; then set -- lavik; fi
if [[ $1 == -* ]]; then set -- lavik "$@"; fi
case "$1" in
  lavik|lavik-meta)
    for argument in "$@"; do
      case "$argument" in --help|-h|--version) exec "$@" ;; esac
    done
    ;;
esac

if [[ $1 == lavik ]]; then
  # A supplied Redis-style config owns all defaults and storage provisioning.
  if [[ $# -gt 1 && $2 != -* ]]; then exec "$@"; fi
  if ! has_option --data-file "$@"; then
    data_file=${LAVIK_DATA_FILE:-/data/lavik.data}
    mkdir -p "$(dirname "$data_file")"
    if [[ ! -e $data_file && ! -L $data_file ]]; then
      # Publish only a fully allocated file. Never truncate or resize a database
      # on restart, including when LAVIK_DATA_SIZE changes.
      allocation=$(mktemp "${data_file}.allocate.XXXXXX")
      trap 'rm -f "$allocation"' EXIT
      fallocate -l "${LAVIK_DATA_SIZE:-1G}" "$allocation"
      ln "$allocation" "$data_file"
      rm -f "$allocation"
      trap - EXIT
    fi
    set -- "$@" --data-file "$data_file"
  fi
  if ! has_option --bind "$@" && ! has_option -b "$@"; then
    set -- "$@" --bind 0.0.0.0
  fi
  if ! has_option --port "$@" && ! has_option -p "$@"; then
    set -- "$@" --port 6379
  fi
  if ! has_option --threads "$@" && ! has_option -t "$@"; then
    set -- "$@" --threads "${LAVIK_THREADS:-2}"
  fi
  if ! has_option --pin-workers "$@" && ! has_option --no-pin-workers "$@"; then
    set -- "$@" --no-pin-workers
  fi
  if ! has_option --log-dir "$@" && ! has_option --log_dir "$@"; then
    mkdir -p /data/logs
    set -- "$@" --log-dir /data/logs
  fi
  if ! has_option --logtostderr "$@" && ! has_option --nologtostderr "$@" \
      && ! has_option --alsologtostderr "$@" && ! has_option --noalsologtostderr "$@"; then
    set -- "$@" --alsologtostderr
  fi
elif [[ $1 == lavik-meta ]]; then
  meta_dir=${LAVIK_META_DATA_DIR:-/data/meta}
  if has_option --data-dir "$@"; then
    previous=''
    for argument in "$@"; do
      if [[ $previous == --data-dir ]]; then meta_dir=$argument; fi
      case "$argument" in --data-dir=*) meta_dir=${argument#*=} ;; esac
      previous=$argument
    done
  else
    set -- "$@" --data-dir "$meta_dir"
  fi
  mkdir -p "$meta_dir"
  # This is the beta.1 NuRaft bootstrap marker. A recovered voter must load its
  # durable configuration instead of being bootstrapped from the manifest again.
  if [[ -n ${LAVIK_META_MANIFEST:-} && ! -e $meta_dir/cluster_config.dat ]] \
      && ! has_option --initial-cluster-manifest "$@"; then
    set -- "$@" --initial-cluster-manifest "$LAVIK_META_MANIFEST"
  fi
fi
exec "$@"
