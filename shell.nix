with (import <nixpkgs> {config.allowUnfree = true;});

let 
in
  pkgs.mkShell {
    buildInputs = with pkgs; [
      zlib
      haskell.compiler.ghc92
      cabal-install
      haskell-language-server
    ];

    shellHook = ''
      PATH=~/.cabal/bin:$PATH
      LD_LIBRARY_PATH=${pkgs.zlib}/lib/:$LD_LIBRARY_PATH
    '';
  }
