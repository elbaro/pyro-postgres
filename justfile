build:
    cargo build --release --lib
    mv target/release/libpyro_postgres.so pyro_postgres/pyro_postgres.abi3.so

check:
    cargo fmt
    black .
    PYTHONPATH=. pytest

bench:
    # just build
    # PYTHONPATH=. cargo bench --no-default-features
    # mkdir -p benchmark
    for dir in target/criterion/*/report; do name=$(basename $(dirname "$dir")); cp "$dir/violin.svg" "benchmark/${name}.svg"; done

publish:
    just check
    rm -rf target/wheels
    maturin build --release
    7z e target/wheels/*.whl pyro_postgres/pyro_postgres.abi3.so -otarget/wheels/pyro_postgres
    patchelf --remove-rpath target/wheels/pyro_postgres/pyro_postgres.abi3.so
    cd target/wheels && 7z u *.whl pyro_postgres/pyro_postgres.abi3.so
    maturin upload target/wheels/*.whl
