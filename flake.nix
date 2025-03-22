{
  description = "Obsrvr Flow Plugin: Contract Events Processor";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
      in
      {
        packages = {
          default = pkgs.buildGoModule {
            pname = "flow-processor-contract-events";
            version = "0.1.0";
            src = ./.;
            
            # Use null to skip vendoring check initially or if using vendored deps
            vendorHash = null;
            
            # Disable hardening which is required for Go plugins
            hardeningDisable = [ "all" ];
            
            # Configure build environment for plugin compilation 
            preBuild = ''
              export CGO_ENABLED=1
            '';
            
            # Build as a shared library/plugin
            buildPhase = ''
              runHook preBuild
              # Use -mod=vendor if you have vendored dependencies
              go build -mod=vendor -buildmode=plugin -o flow-processor-contract-events.so .
              runHook postBuild
            '';

            # Custom install phase for the plugin
            installPhase = ''
              runHook preInstall
              mkdir -p $out/lib
              cp flow-processor-contract-events.so $out/lib/
              # Also install a copy of go.mod for future reference
              mkdir -p $out/share
              cp go.mod $out/share/
              if [ -f go.sum ]; then
                cp go.sum $out/share/
              fi
              runHook postInstall
            '';
            
            # Add dependencies needed for the build
            nativeBuildInputs = [ pkgs.pkg-config ];
            buildInputs = [ 
              # Add C library dependencies here if needed
            ];
          };
        };

        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [ 
            # Using Go 1.23 to match your go.mod requirements
            # Note: If 1.23 isn't available in nixpkgs, you may need to use the closest available version
            go_1_21
            pkg-config
            git
            gopls
            delve
          ];
          
          # Shell setup for development environment
          shellHook = ''
            # Enable CGO which is required for plugin mode
            export CGO_ENABLED=1
            
            # Helper to vendor dependencies - greatly improves build reliability
            if [ ! -d vendor ]; then
              echo "Vendoring dependencies..."
              go mod tidy
              go mod vendor
            fi
            
            echo "Development environment ready!"
            echo "To build the plugin manually: go build -buildmode=plugin -o flow-processor-contract-events.so ."
          '';
        };
      }
    );
} 