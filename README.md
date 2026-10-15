# 仓位哨兵 PositionGuard

OKX 永续合约「价格条件 + N 分钟涨跌幅」自动平仓的 macOS 菜单栏应用。完整的需求和产品逻辑见 [docs/产品文档.md](docs/产品文档.md)。

- 只盯用户添加的标的；每个标的先设价格条件（标记价高于 / 低于某价，必填），条件当下满足时，规则「和 N 分钟前比涨 / 跌 X%」才生效。
- 触发后平掉这个标的上的所有仓位：先下追逐委托，超时或被撤单就把剩余部分改市价兜底。
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

- `guard/engine.py` 监控引擎：采样、价格条件与规则判断、追逐委托平仓与市价兜底、健康检查
- `guard/rules.py` / `guard/series.py` 规则计算与价格序列（纯函数）
- `guard/okx.py` OKX REST / WebSocket 客户端
- `guard/web.py` + `guard/static/index.html` 本机控制面板
- `guard/app.py` 入口与菜单栏
- 运行数据：`~/Library/Application Support/PositionGuard/`（配置、事件记录、日志）
