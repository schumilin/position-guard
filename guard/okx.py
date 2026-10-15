"""OKX v5 REST / WebSocket 客户端，只包含本程序用到的接口。

会改变账户的接口只有平仓相关的：追逐委托（只减仓）、撤销追逐委托、市价全平 close-position、只减仓的市价单。没有任何开仓代码。
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import random
import ssl
import time
from datetime import datetime, timezone
from typing import Callable
from urllib.parse import urlencode

import aiohttp
import certifi

log = logging.getLogger(__name__)

REST_HOSTS = ["www.okx.com", "openapi.okx.com"]

AUTH_CODES = {
    "50100": "这把 API Key 已被冻结",
    "50101": "这把 Key 是模拟盘的，本程序只支持实盘 Key",
    "50103": "缺少 API Key",
    "50104": "缺少 Passphrase",
    "50105": "Passphrase 不对",
    "50110": "当前网络的 IP 不在这把 Key 的 IP 白名单里（建 Key 时 IP 应留空）",
    "50111": "API Key 无效，可能填错了",
    "50113": "签名无效，Secret Key 可能填错了",
    "50114": "授权无效",
    "50119": "API Key 不存在：可能填错了，或者 14 天没用被 OKX 自动删除了",
    "50120": "这把 Key 没有需要的权限",
    "60005": "API Key 无效，可能填错了",
    "60007": "签名无效，Secret Key 可能填错了",
    "60009": "登录失败，请检查 Key / Secret / Passphrase",
    "60024": "Passphrase 不对",
}

KNOWN_CODES = {
    "50001": "OKX 服务暂时不可用",
    "50004": "OKX 接口超时",
    "50011": "请求太频繁",
    "50013": "OKX 系统繁忙",
    "50102": "电脑时间和 OKX 不一致",
    "51023": "仓位不存在",
    "51169": "这个方向没有可平的仓位",
    "51008": "保证金不足",
}


class OkxError(Exception):
    def __init__(self, code: str, msg: str, data=None):
        super().__init__(f"[{code}] {msg}")
        self.code = str(code)
        self.msg = msg
        self.data = data

    @property
    def is_auth(self) -> bool:
        return self.code in AUTH_CODES


class NetworkError(Exception):
    pass


def explain(e: Exception) -> str:
    if isinstance(e, OkxError):
        human = AUTH_CODES.get(e.code) or KNOWN_CODES.get(e.code)
        return f"{human}（OKX 错误码 {e.code}：{e.msg}）" if human else f"OKX 返回错误 {e.code}：{e.msg}"
    if isinstance(e, NetworkError):
        return f"网络不通：{e}"
    return f"{type(e).__name__}: {e}"


def sign(secret: str, prehash: str) -> str:
    return base64.b64encode(hmac.new(secret.encode(), prehash.encode(), hashlib.sha256).digest()).decode()


def ssl_context() -> ssl.SSLContext:
    return ssl.create_default_context(cafile=certifi.where())


def ws_urls(kind: str) -> list[str]:
    return [f"{h}/ws/v5/{kind}" for h in ("wss://ws.okx.com:8443", "wss://ws.okx.com")]


class OkxRest:
    def __init__(self, session: aiohttp.ClientSession, *, creds: dict | None,
                 proxy_getter: Callable[[], str | None]):
        self.session = session
        self.creds = creds
        self.proxy_getter = proxy_getter
        self.host_idx = 0
        self.time_offset_ms = 0.0
        self.last_ok = 0.0

    def now_ms(self) -> float:
        return time.time() * 1000 + self.time_offset_ms

    def _iso_ts(self) -> str:
        dt = datetime.fromtimestamp(self.now_ms() / 1000, tz=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"

    async def request(self, method: str, path: str, *, params: dict | None = None, body: dict | list | None = None,
                      auth: bool = False, timeout: float = 10):
        if params:
            params = {k: v for k, v in params.items() if v not in (None, "")}
        req_path = path + ("?" + urlencode(params) if params else "")
        body_str = json.dumps(body, separators=(",", ":")) if body is not None else ""
        last_exc: Exception | None = None
        for _ in range(len(REST_HOSTS)):
            host = REST_HOSTS[self.host_idx]
            headers = {"Content-Type": "application/json", "Accept": "application/json"}
            if auth:
                if not self.creds:
                    raise OkxError("50103", "未设置 API Key")
                ts = self._iso_ts()
                headers.update({
                    "OK-ACCESS-KEY": self.creds["api_key"],
                    "OK-ACCESS-SIGN": sign(self.creds["secret_key"], ts + method.upper() + req_path + body_str),
                    "OK-ACCESS-TIMESTAMP": ts,
                    "OK-ACCESS-PASSPHRASE": self.creds["passphrase"],
                })
            try:
                async with self.session.request(
                    method, f"https://{host}{req_path}", data=body_str or None, headers=headers,
                    proxy=self.proxy_getter(), timeout=aiohttp.ClientTimeout(total=timeout),
                ) as resp:
                    status = resp.status
                    text = await resp.text()
            except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as e:
                last_exc = e
                self.host_idx = (self.host_idx + 1) % len(REST_HOSTS)
                continue
            try:
                payload = json.loads(text)
            except ValueError:
                raise OkxError(f"http{status}", text[:200]) from None
            self.last_ok = time.time()
            code = str(payload.get("code", ""))
            data = payload.get("data")
            if code != "0":
                if isinstance(data, list) and data and isinstance(data[0], dict) and data[0].get("sCode") not in (None, "", "0"):
                    raise OkxError(data[0]["sCode"], data[0].get("sMsg") or payload.get("msg", ""), data)
                raise OkxError(code, payload.get("msg", ""), data)
            return data
        raise NetworkError(f"{type(last_exc).__name__}: {last_exc}" if last_exc else "未知网络错误")

    # ---- 公共接口 ----
    async def sync_time(self) -> float:
        t0 = time.time() * 1000
        data = await self.request("GET", "/api/v5/public/time")
        t1 = time.time() * 1000
        self.time_offset_ms = int(data[0]["ts"]) - (t0 + t1) / 2
        return self.time_offset_ms

    async def instrument(self, inst_id: str) -> dict:
        data = await self.request("GET", "/api/v5/public/instruments", params={"instType": "SWAP", "instId": inst_id})
        return data[0]

    async def mark_candles(self, inst_id: str, limit: int) -> list:
        return await self.request("GET", "/api/v5/market/mark-price-candles",
                                  params={"instId": inst_id, "bar": "1m", "limit": str(limit)})

    # ---- 私有接口（只读）----
    async def account_config(self) -> dict:
        return (await self.request("GET", "/api/v5/account/config", auth=True))[0]

    async def positions(self) -> list[dict]:
        return await self.request("GET", "/api/v5/account/positions", params={"instType": "SWAP"}, auth=True)

    async def algo_pending(self, ord_type: str) -> list[dict]:
        return await self.request("GET", "/api/v5/trade/orders-algo-pending",
                                  params={"ordType": ord_type, "instType": "SWAP"}, auth=True)

    # ---- 私有接口（平仓）----
    async def close_position(self, inst_id: str, mgn_mode: str, pos_side: str) -> list:
        body = {"instId": inst_id, "mgnMode": mgn_mode, "posSide": pos_side, "autoCxl": True}
        return await self.request("POST", "/api/v5/trade/close-position", body=body, auth=True)

    async def place_chase(self, inst_id: str, td_mode: str, side: str, sz: str, pos_side: str, net_mode: bool,
                          max_chase_ratio: float) -> str:
        """追逐委托：挂在买一 / 卖一价，每秒跟随调整；偏离首次挂单价超过 max_chase_ratio 时 OKX 自动撤单。"""
        body = {"instId": inst_id, "tdMode": td_mode, "side": side, "ordType": "chase", "sz": sz,
                "maxChaseType": "ratio", "maxChaseVal": f"{max_chase_ratio:.6f}".rstrip("0").rstrip(".")}
        if net_mode:
            body["reduceOnly"] = True
        else:
            body["posSide"] = pos_side
        data = await self.request("POST", "/api/v5/trade/order-algo", body=body, auth=True)
        return data[0]["algoId"]

    async def algo_detail(self, algo_id: str) -> dict:
        return (await self.request("GET", "/api/v5/trade/order-algo", params={"algoId": algo_id}, auth=True))[0]

    async def cancel_algo(self, inst_id: str, algo_id: str) -> list:
        return await self.request("POST", "/api/v5/trade/cancel-algos", body=[{"algoId": algo_id, "instId": inst_id}],
                                  auth=True)

    async def reduce_market(self, inst_id: str, td_mode: str, side: str, sz: str, pos_side: str, net_mode: bool) -> list:
        body = {"instId": inst_id, "tdMode": td_mode, "side": side, "ordType": "market", "sz": sz}
        if net_mode:
            body["reduceOnly"] = True
        else:
            body["posSide"] = pos_side
        return await self.request("POST", "/api/v5/trade/order", body=body, auth=True)


class AuthFailed(Exception):
    pass


class OkxWs:
    """一条自动重连的 WebSocket 连接。断线后自动重连并恢复订阅。"""

    def __init__(self, name: str, urls: list[str], session: aiohttp.ClientSession,
                 proxy_getter: Callable[[], str | None], on_data: Callable[[dict, list], None],
                 login_args: Callable[[], dict] | None = None):
        self.name = name
        self.urls = urls
        self.session = session
        self.proxy_getter = proxy_getter
        self.on_data = on_data
        self.login_args = login_args
        self.subs: dict[str, dict] = {}
        self.ws: aiohttp.ClientWebSocketResponse | None = None
        self.connected = False
        self.changed_at = time.time()
        self.last_error = ""
        self.last_msg_at = 0.0
        self.auth_error: str | None = None

    def _set_connected(self, v: bool) -> None:
        if v != self.connected:
            self.connected = v
            self.changed_at = time.time()

    async def subscribe(self, args: list[dict]) -> None:
        new = []
        for a in args:
            key = json.dumps(a, sort_keys=True)
            if key not in self.subs:
                self.subs[key] = a
                new.append(a)
        if new and self.ws is not None and self.connected and not self.ws.closed:
            try:
                await self.ws.send_str(json.dumps({"op": "subscribe", "args": new}))
            except Exception as e:  # 发送失败就等重连时统一补订
                log.warning("%s subscribe send failed: %s", self.name, e)

    async def run(self) -> None:
        i = 0
        backoff = 1.0
        while True:
            url = self.urls[i % len(self.urls)]
            try:
                await self._session_once(url)
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except AuthFailed as e:
                self.auth_error = str(e)
                self.last_error = str(e)
                backoff = 30.0
            except Exception as e:
                self.last_error = f"{type(e).__name__}: {e}" if str(e) else type(e).__name__
                log.warning("%s ws error on %s: %s", self.name, url, self.last_error)
                i += 1
            finally:
                self._set_connected(False)
                if self.ws is not None and not self.ws.closed:
                    try:
                        await self.ws.close()
                    except Exception:
                        pass
                self.ws = None
            await asyncio.sleep(backoff + random.random())
            backoff = min(backoff * 2, 15.0)

    async def _recv(self, ws, timeout: float):
        msg = await asyncio.wait_for(ws.receive(), timeout)
        if msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING):
            raise ConnectionError("服务器关闭了连接")
        if msg.type == aiohttp.WSMsgType.ERROR:
            raise ConnectionError(f"连接出错：{ws.exception()}")
        self.last_msg_at = time.time()
        return msg

    async def _session_once(self, url: str) -> None:
        ws = await asyncio.wait_for(
            self.session.ws_connect(url, proxy=self.proxy_getter(), heartbeat=None, autoping=True, max_msg_size=0),
            15,
        )
        self.ws = ws
        if self.login_args:
            await ws.send_str(json.dumps({"op": "login", "args": [self.login_args()]}))
            deadline = time.time() + 10
            while True:
                msg = await self._recv(ws, max(0.5, deadline - time.time()))
                if msg.type != aiohttp.WSMsgType.TEXT or msg.data == "pong":
                    continue
                payload = json.loads(msg.data)
                if payload.get("event") == "login" and str(payload.get("code")) == "0":
                    break
                if payload.get("event") == "error":
                    err = OkxError(str(payload.get("code")), payload.get("msg", ""))
                    if err.is_auth:  # Key 本身有问题才算致命；时间戳过期之类的重连即可
                        raise AuthFailed(explain(err))
                    raise ConnectionError(explain(err))
            self.auth_error = None
        if self.subs:
            await ws.send_str(json.dumps({"op": "subscribe", "args": list(self.subs.values())}))
        self._set_connected(True)
        self.last_error = ""
        while True:
            try:
                msg = await self._recv(ws, 20)
            except asyncio.TimeoutError:
                await ws.send_str("ping")
                msg = await self._recv(ws, 10)  # 10 秒内没有任何回应就重连
            if msg.type != aiohttp.WSMsgType.TEXT or msg.data == "pong":
                continue
            payload = json.loads(msg.data)
            event = payload.get("event")
            if event == "error":
                log.warning("%s ws error event: %s", self.name, payload)
                continue
            if event == "notice":  # 例如服务升级，OKX 要求客户端重连
                raise ConnectionError(payload.get("msg", "服务端通知重连"))
            if event:
                continue
            if "data" in payload:
                try:
                    self.on_data(payload.get("arg") or {}, payload["data"])
                except Exception:
                    log.exception("%s on_data failed", self.name)


def ws_login_args(creds: dict, now_ms: float) -> dict:
    ts = str(int(now_ms / 1000))
    return {
        "apiKey": creds["api_key"],
        "passphrase": creds["passphrase"],
        "timestamp": ts,
        "sign": sign(creds["secret_key"], ts + "GET" + "/users/self/verify"),
    }
