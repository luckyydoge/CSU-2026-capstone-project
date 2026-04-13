# 面向边缘计算的资源管理系统

## 概述
基于KubeRay+Prometheus的边缘推理资源管理原型系统。
Kubernetes (Kind集群) + Python 3.12+ + uv包管理。

## 目录结构
- `src/monitors/` - VictoriaMetrics查询接口
- `tests/` - 单元测试
- `config/` - KubeRay集群配置YAML

## 常用命令
- 同步依赖：`uv sync`
- 查看Kind API Server地址：`kind get kubeconfig --name kind | grep server`
- 查看集群Pod状态：`kubectl get pods -A`

## 环境要点
- Kind集群API Server在127.0.0.1:41303
- KubeRay Operator在default命名空间
- Prometheus+VictoriaMetrics在prometheus-system命名空间
- VM查询地址：`http://localhost:8428`

## 代码规范
- 所有K8s操作使用`kubernetes` Python客户端，禁止调用`kubectl`命令
- 扩缩容操作前必须做上下限边界检查
- 新增功能先写测试

## 架构约束
- 绝对不修改`kube-system`命名空间下的资源
- Ray集群名固定为`raycluster-kuberay`，Worker Group名以实际配置为准