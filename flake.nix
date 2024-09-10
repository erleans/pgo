{
  description = "Customize rebar3";

  inputs = {
    nixpkgs.url = "nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = {
    nixpkgs,
    flake-utils,
    ...
  }:
    flake-utils.lib.eachSystem ["x86_64-linux" "x86_64-darwin" "aarch64-darwin"] (system: let
      erlang_26_rebar_overlay = final: prev: {
        rebar3 = prev.rebar3.overrideAttrs (oldAttrs: {
          buildInputs = [ final.erlang_26 ];
        });
      };

      pkgs = import nixpkgs {
        inherit system;
        config.allowBroken = true;
        overlays = [ erlang_26_rebar_overlay ];
      };
    in {
      devShell = import ./shell.nix {
        inherit pkgs;
      };
      packages = flake-utils.lib.flattenTree {
      };
    });
}
