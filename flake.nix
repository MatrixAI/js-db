{
  inputs = {
    nixpkgs = {
      type = "github";
      owner = "NixOS";
      repo = "nixpkgs";
      rev = "ea5234e7073d5f44728c499192544a84244bf35a";
    };
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { nixpkgs, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        shell = { ci ? false }:
          with pkgs;
          mkShell {
            nativeBuildInputs =
              [ nodejs_20 nodejs_20.python clang-tools shellcheck gitAndTools.gh ];
            # Don't set rpath for native addons
            NIX_DONT_SET_RPATH = true;
            NIX_NO_SELF_RPATH = true;
            shellHook = ''
              echo "Entering $(npm pkg get name)"
              set -o allexport
              . ./.env
              set +o allexport
              set -v
              ${lib.optionalString ci ''
                set -o errexit
                set -o nounset
                set -o pipefail
                shopt -s inherit_errexit
              ''}
              mkdir --parents "$(pwd)/tmp"

              # Built executables and NPM executables
              export PATH="$(pwd)/dist/bin:$(npm root)/.bin:$PATH"

              # Path to headers used by node-gyp for native addons
              export npm_config_nodedir="${nodejs}"

              # Verbose logging of the Nix compiler wrappers
              export NIX_DEBUG=1

              npm install --ignore-scripts

              set +v
            '';
          };
      in {
        devShells = {
          default = shell { ci = false; };
          ci = shell { ci = true; };
        };
      });
}
