# 仓位哨兵 PositionGuard

OKX 永续合约「N 分钟涨跌幅」自动平仓的 macOS 菜单栏应用。

- 规则：当前标记价对比 N 分钟前锚点价（每秒采样，锚点取时间 ≥ 当前 − N 且最近的采样点），按「不利 / 有利」方向达到阈值后执行：市价全平 / 按比例平仓 / 只报警。
- 安全：代码里只有平仓、没有开仓；带提现权限的 API Key 拒绝使用；Key 存 macOS 钥匙串；控制面板只监听 127.0.0.1 并校验令牌。

## 开发

```bash
uv sync
uv run pytest -q
uv run python -m guard.app --headless   # 不带菜单栏，终端里打印控制面板地址
```

## 打包

```bash
./scripts/build.sh   # 产出 dist/PositionGuard.app 和 dist/PositionGuard.zip（Apple 芯片，macOS 11+）
```

应用未经 Apple 签名公证，拷到别的 Mac 后需执行 `xattr -cr /Applications/PositionGuard.app` 才能打开。

## 目录

- `guard/engine.py` 监控引擎：采样、规则判断、平仓、健康检查
- `guard/rules.py` / `guard/series.py` 规则计算与价格序列（纯函数）
- `guard/okx.py` OKX REST / WebSocket 客户端
- `guard/web.py` + `guard/static/index.html` 本机控制面板
- `guard/app.py` 入口与菜单栏
- 运行数据：`~/Library/Application Support/PositionGuard/`（配置、事件记录、日志）
