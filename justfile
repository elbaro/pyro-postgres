build:
    cargo build --release --lib
    mv target/release/libpyro_postgres.so pyro_postgres/pyro_postgres.abi3.so

check:
    cargo fmt
    black .
    PYTHONPATH=. pytest

bench:
    just build
    PYTHONPATH=. cargo bench --no-default-features

publish:
    just check
    maturin build --release
