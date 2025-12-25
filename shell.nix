{
  pkgs ? import <nixpkgs> { },
}:

pkgs.mkShell {
  packages = with pkgs; [
    (python313.withPackages (
      ps: with ps; [
        # runtime dependencies
        greenlet
        sqlalchemy
        typing-extensions

        # dev tools
        pytest
        pytest-asyncio
        pyright
        pip

        # comparison drivers
        psycopg
        asyncpg
      ]
    ))

    # system dependencies
    openssl
    pkg-config
    gnuplot
    maturin
  ];

  shellHook = ''
    export PYTHONPATH=.
    export DATABASE_URL="postgres://test:1234@localhost:5432/test?prefer_unix_socket=false"
  '';
}
