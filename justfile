dev:
    uv run fastapi dev main.py

# 核心：使用 {{file}} 接收文件名参数
run file:
    uv run python {{file}}

# 如果你想更省力，可以缩写成 r
r file:
    uv run python {{file}}