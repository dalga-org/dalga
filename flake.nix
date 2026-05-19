{
  description = "Dalga - Streaming Data Profiler";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            # Rust Toolchain
            cargo
            rustc
            rustfmt
            clippy
            
            # Python Ecosystem
            python311
            maturin
            uv
            ruff
            
            # System Dependencies for compilation
            iconv
          ];

          shellHook = ''
            echo "🌊 Welcome to the Dalga SDK Dev Environment"
            echo "Rust: $(rustc --version)"
            echo "Python: $(python3 --version)"
            
            if [ ! -d ".venv" ]; then
              uv venv
            fi
            source .venv/bin/activate
          '';
        };
      }
    );
}
