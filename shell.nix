{ pkgs ? import ./pkgs.nix {} }:

with pkgs;
mkShell {
  nativeBuildInputs = [
    nodejs
    nodejs.python
    clang-tools
  ];
  # Don't set rpath for native addons
  NIX_DONT_SET_RPATH = true;
  NIX_NO_SELF_RPATH = true;
  shellHook = ''
    echo 'Entering js-db'
    set -o allexport
    . ./.env
    set +o allexport
    set -v

    mkdir --parents "$(pwd)/tmp"

    # Built executables and NPM executables
    export PATH="$(pwd)/dist/bin:$(npm bin):$PATH"

    # Enables npm link
    export npm_config_prefix=~/.npm

    # Path to headers used by node-gyp for native addons
    export npm_config_nodedir="${nodejs}"

    # Use all cores during node-gyp compilation
    export npm_config_jobs=max

    # Verbose logging of the Nix compiler wrappers
    export NIX_DEBUG=1

    npm install --ignore-scripts

    set +v
  '';
}
