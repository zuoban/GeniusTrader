#!/bin/sh
set -eu
task_rust_root="${RUSTUP_HOME:-$HOME/.rustup}/toolchains"
for task_toolchain in "$task_rust_root"/1.98.1-* "$task_rust_root"/stable-*; do
    if [ -x "$task_toolchain/bin/rustc" ] && "$task_toolchain/bin/rustc" --version >/dev/null 2>&1; then
        export PATH="$task_toolchain/bin:$PATH"
        export RUSTC="$task_toolchain/bin/rustc"
        export RUSTDOC="$task_toolchain/bin/rustdoc"
        export RUSTFMT="$task_toolchain/bin/rustfmt"
        exec "$task_toolchain/bin/cargo" "$@"
    fi
done
exec cargo "$@"
