"""
反向代理模块。

/proxy/{path} → 去掉 /proxy 前缀 → 转发到目标 host + 注入 X-Submit-Time header
"""
import time
import httpx
from fastapi import APIRouter, Request, Response, HTTPException

TARGET_HOST = "http://192.168.31.125:8000"
router = APIRouter(prefix="/proxy", tags=["反向代理"])


@router.api_route("/{rest:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def proxy_request(rest: str, request: Request):
    target_url = f"{TARGET_HOST.rstrip('/')}/{rest.lstrip('/')}"
    if request.query_params:
        target_url += "?" + str(request.query_params)

    headers = dict(request.headers)
    headers.pop("host", None)
    headers["X-Submit-Time"] = str(time.time())

    body = await request.body()
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            resp = await client.request(
                method=request.method,
                url=target_url,
                headers=headers,
                content=body,
            )
        except httpx.RequestError as e:
            raise HTTPException(status_code=502, detail=f"Proxy error: {e}")

    return Response(
        content=resp.content,
        status_code=resp.status_code,
        headers=dict(resp.headers),
    )
