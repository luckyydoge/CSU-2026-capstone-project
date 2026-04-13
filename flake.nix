{
  description = "KubeEdge + Kind Development Environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    utils.url = "github:numtide/flake-utils";
  };

  outputs = {
    self,
    nixpkgs,
    utils,
  }:
    utils.lib.eachDefaultSystem (system: let
      pkgs = import nixpkgs {inherit system;};
    in {
      devShells.default = pkgs.mkShell {
        buildInputs = with pkgs; [
          # 基础集群工具
          kind
          kubectl
          kubernetes-helm

          # KubeEdge 工具
          # keadm

          # 调试工具
          docker       # 主要是为了客户端命令兼容性
          # podman       # 宿主机实际的 provider

	  pyright
	  uv
	  just
	  
        ];

        shellHook = ''
          # 核心：自动切换 Kind 到 Podman 模式
          # export KIND_EXPERIMENTAL_PROVIDER=podman

          # 如果是 Nushell 用户，建议在进入后手动执行:
          # $env.KIND_EXPERIMENTAL_PROVIDER = "podman"

          echo "🚀 KubeEdge + Kind 开发环境已就绪"
          echo "--------------------------------------------------"
          echo "当前 Provider: $KIND_EXPERIMENTAL_PROVIDER"
          echo "Podman 版本: $(podman --version)"
          echo "Kind 版本:   $(kind --version)"
          echo "--------------------------------------------------"
          echo "提示: 如果运行 kind 报错，请确认已在 NixOS 配置中开启 systemd 代理"
        '';
      };
    });
}
