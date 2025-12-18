{ pkgs ? import <nixpkgs> { } }:

pkgs.mkShell {
  packages = with pkgs; [
    (python313.withPackages (ps: with ps; [
      # runtime dependencies
      greenlet
      (sqlalchemy.overrideAttrs (old: { version = "2.0.44"; }))
      typing-extensions

      # dev tools
      pytest
      pytest-asyncio
      maturin
      pyright

      # comparison drivers
      psycopg
      asyncpg
    ]))

    # system dependencies
    openssl
    pkg-config
    gnuplot
  ];

  shellHook = ''
    export PYTHONPATH=.
    echo "pyro-postgres $(python --version)"
  '';
}
