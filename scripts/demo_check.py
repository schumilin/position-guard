"""模拟盘联调：只读检查 Key、持仓、私有 WebSocket 登录。不打印任何密钥。

用法：uv run python scripts/demo_check.py secrets/okx-demo.env
"""
import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
os.environ["PG_CRED_FILE"] = sys.argv[1]
os.environ.setdefault("PG_NO_OS_ALERTS", "1")

import aiohttp  # noqa: E402

from guard import creds as creds_mod  # noqa: E402
from guard.alerts import Alerter  # noqa: E402
from guard.config import ConfigStore  # noqa: E402
from guard.engine import Engine  # noqa: E402
from guard.okx import OkxRest, explain, ssl_context, ws_login_args, ws_urls  # noqa: E402


async def main() -> None:
    creds = creds_mod.load("demo")
    if not creds:
        sys.exit("模拟盘 Key 文件没填完整")
    engine = Engine(ConfigStore(), Alerter())
    res = await engine.verify_key("demo", creds)
    for c in res["checks"].values():
        print(f"[{c['state']}] {c['label']}: {c['msg']}")
    if not res["ok"]:
        sys.exit("Key 检查未通过")
    print("posMode:", res["acct"].get("posMode"), "acctLv:", res["acct"].get("acctLv"))
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context())) as s:
        rest = OkxRest(s, demo=True, creds=creds, proxy_getter=lambda: None)
        await rest.sync_time()
        rows = await rest.positions()
        print(f"持仓 {len(rows)} 个：")
        for r in rows:
            print("  ", r["instId"], r["posSide"], r["pos"], r["mgnMode"], "lever", r.get("lever"), "posId", r.get("posId"),
                  "closeOrderAlgo", len(r.get("closeOrderAlgo") or []))
        algos = await rest.algo_pending("conditional,oco")
        print("挂着的条件单/止盈止损单：", len(algos))
        ws = await s.ws_connect(ws_urls(True, "private")[0])
        await ws.send_str(json.dumps({"op": "login", "args": [ws_login_args(creds, rest.now_ms())]}))
        print("WS login:", (await ws.receive(timeout=10)).data)
        await ws.send_str(json.dumps({"op": "subscribe", "args": [{"channel": "positions", "instType": "SWAP"}]}))
        for _ in range(2):
            msg = json.loads((await ws.receive(timeout=10)).data)
            print("WS:", msg.get("event") or f"positions push x{len(msg.get('data', []))}")
        await ws.close()


try:
    asyncio.run(main())
except Exception as e:
    sys.exit(f"失败：{explain(e)}")
