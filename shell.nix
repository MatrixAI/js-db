{ pkgs ? import ./pkgs.nix {} }:

with pkgs;
pkgs.mkShell {
  nativeBuildInputs = [
    nodejs
    nodePackages.node2nix
  ];
  shellHook = ''
    echo 'Entering js-db'
    set -o allexport
    . ./.env
    set +o allexport
    set -v

    # Enables npm link to work
    export npm_config_prefix=~/.npm

    export PATH="$(pwd)/dist/bin:$(npm bin):$PATH"
    npm install
    mkdir --parents "$(pwd)/tmp"

    set +v
  '';
}
