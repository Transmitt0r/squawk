{
  description = "FlightTracker dev environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
  };

  outputs = { self, nixpkgs }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
      forAllSystems = f: nixpkgs.lib.genAttrs systems (system: f {
        pkgs = nixpkgs.legacyPackages.${system};
      });
    in
    {
      devShells = forAllSystems ({ pkgs }:
        let
          python = pkgs.python313.withPackages (ps: with ps; [
            aiohttp
            asyncpg
            python-dotenv
            pytest
            pytest-asyncio
          ]);
        in
        {
          default = pkgs.mkShell {
            packages = [
              python
              pkgs.ruff
              pkgs.mypy
              pkgs.postgresql
            ];

            shellHook = ''
              echo "flighttracker dev shell"
            '';
          };
        });
    };
}
