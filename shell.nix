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
        maturin
        pyright

        # comparison drivers
        psycopg
        asyncpg
      ]
    ))

    # system dependencies
    openssl
    pkg-config
    gnuplot
  ];

  shellHook = ''
    export PYTHONPATH=.
    export DATABASE_URL="postgres://test:1234@localhost:5432/test"
    echo "pyro-postgres $(python --version)"
  '';
}
