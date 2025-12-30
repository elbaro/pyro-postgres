build:
    cargo build --release --lib
    mv target/release/libpyro_postgres.so pyro_postgres/pyro_postgres.abi3.so

bench:
    # just build
    # PYTHONPATH=. cargo bench --no-default-features
    # mkdir -p benchmark
    for dir in target/criterion/*/report; do name=$(basename $(dirname "$dir")); cp "$dir/violin.svg" "benchmark/${name}.svg"; done
