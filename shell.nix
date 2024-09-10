{pkgs, elp, ...}:
pkgs.mkShell {
  buildInputs = with pkgs; [
    # nix tools
    alejandra

    # erlang stuff
    erlang_26
    rebar3
    elp

    # postgresql tools
    dbmate
  ];
}
