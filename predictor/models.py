"""
预测器抽象接口 + 线性回归实现
支持可插拔：实现 BasePredictor 即可接入新算法
"""
import json
import os
from typing import Dict, List, Optional
from abc import ABC, abstractmethod


class BasePredictor(ABC):
    """预测器抽象接口。所有预测算法实现此接口即可替换。"""

    @abstractmethod
    def fit(self, X: List[List[float]], y: List[float]):
        ...

    @abstractmethod
    def predict(self, X: List[List[float]]) -> List[float]:
        ...

    @abstractmethod
    def to_dict(self) -> Dict:
        ...

    @classmethod
    @abstractmethod
    def from_dict(cls, d: Dict) -> "BasePredictor":
        ...

    def save(self, path: str):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)

    @classmethod
    def load(cls, path: str) -> "BasePredictor":
        with open(path) as f:
            return cls.from_dict(json.load(f))


class LinearRegression(BasePredictor):
    """多元线性回归 — 最小二乘法，零外部依赖"""

    def __init__(self):
        self.coef_: Optional[List[float]] = None
        self.intercept_: Optional[float] = None

    def fit(self, X: List[List[float]], y: List[float]):
        n = len(X)
        if n == 0:
            raise ValueError("Empty training data")

        m = len(X[0]) + 1
        X_aug = [[1.0] + row for row in X]

        XtX = [[0.0] * m for _ in range(m)]
        for i in range(m):
            for j in range(m):
                XtX[i][j] = sum(X_aug[k][i] * X_aug[k][j] for k in range(n))

        Xty = [0.0] * m
        for i in range(m):
            Xty[i] = sum(X_aug[k][i] * y[k] for k in range(n))

        w = self._solve_gaussian(XtX, Xty)
        self.intercept_ = w[0]
        self.coef_ = w[1:]

    def predict(self, X: List[List[float]]) -> List[float]:
        if self.coef_ is None or self.intercept_ is None:
            raise RuntimeError("Model not trained yet")
        result = []
        for row in X:
            val = self.intercept_
            for xi, ci in zip(row, self.coef_):
                val += xi * ci
            result.append(val)
        return result

    def to_dict(self) -> Dict:
        return {"intercept": self.intercept_, "coef": self.coef_}

    @classmethod
    def from_dict(cls, d: Dict) -> "LinearRegression":
        m = cls()
        m.intercept_ = d["intercept"]
        m.coef_ = d["coef"]
        return m

    @staticmethod
    def _solve_gaussian(A: List[List[float]], b: List[float]) -> List[float]:
        n = len(b)
        m = [row[:] for row in A]
        rhs = b[:]
        for i in range(n):
            max_row = max(range(i, n), key=lambda r: abs(m[r][i]))
            if abs(m[max_row][i]) < 1e-12:
                continue
            m[i], m[max_row] = m[max_row], m[i]
            rhs[i], rhs[max_row] = rhs[max_row], rhs[i]
            piv = m[i][i]
            for j in range(i + 1, n):
                factor = m[j][i] / piv
                rhs[j] -= factor * rhs[i]
                for k in range(i, n):
                    m[j][k] -= factor * m[i][k]
        x = [0.0] * n
        for i in range(n - 1, -1, -1):
            s = rhs[i]
            for j in range(i + 1, n):
                s -= m[i][j] * x[j]
            x[i] = s / m[i][i] if abs(m[i][i]) > 1e-12 else 0.0
        return x


class PredictorBundle:
    """
    管理三类数据的 CPU/MEM 模型。

    predictor_cls: 预测器类，默认为 LinearRegression。
                   换算法时传入其他 BasePredictor 实现即可。
    """

    def __init__(self, predictor_cls=LinearRegression):
        self.predictor_cls = predictor_cls
        self.models: Dict[str, Dict[str, BasePredictor]] = {
            dtype: {"cpu": predictor_cls(), "mem": predictor_cls()}
            for dtype in ("image", "video", "data")
        }

    def fit(self, data_type: str, X: List[List[float]], y_cpu: List[float], y_mem: List[float]):
        m = self.models.get(data_type)
        if not m:
            raise ValueError(f"Unknown data type: {data_type}")
        m["cpu"].fit(X, y_cpu)
        m["mem"].fit(X, y_mem)

    def predict(self, data_type: str, features: List[float]) -> Dict[str, float]:
        m = self.models.get(data_type)
        if not m:
            raise ValueError(f"Unknown data type: {data_type}")
        cpu_val = m["cpu"].predict([features])[0]
        mem_val = m["mem"].predict([features])[0]
        return {"cpu_percent": round(max(0, cpu_val), 1),
                "memory_mb": round(max(0, mem_val), 1)}

    def save_all(self, directory: str):
        for dtype in self.models:
            for target in ["cpu", "mem"]:
                path = os.path.join(directory, f"{dtype}_{target}.json")
                self.models[dtype][target].save(path)

    @classmethod
    def load_all(cls, directory: str, predictor_cls=LinearRegression) -> "PredictorBundle":
        bundle = cls(predictor_cls=predictor_cls)
        for dtype in ["image", "video", "data"]:
            for target in ["cpu", "mem"]:
                path = os.path.join(directory, f"{dtype}_{target}.json")
                if os.path.exists(path):
                    bundle.models[dtype][target] = predictor_cls.load(path)
        return bundle
