{pkgs, ...}:
pkgs.mkShell {
  buildInputs = with pkgs; [
    # erlang stuff
    erlang_26
    rebar3
  ];
}
