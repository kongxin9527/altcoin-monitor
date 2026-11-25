import asyncio
import json
import logging
import os
import sqlite3
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Dict, List, Optional

import aiohttp
import uvicorn
import websockets
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

# ==================== 策略与系统配置 ====================
CONFIG = {
    # Telegram 推送配置（强烈建议通过环境变量传入，也可直接填写）
    "TG_BOT_TOKEN": os.getenv("TG_BOT_TOKEN", ""),
    "TG_CHAT_ID": os.getenv("TG_CHAT_ID", ""),

    # 模拟盘资金配置（无杠杆 1x）
    "INITIAL_BALANCE": 10000.0,       # 初始虚拟本金 (USDT)
    "MAX_POSITIONS": 4,               # 最大同时持仓数
    "POSITION_SIZE_PCT": 0.25,        # 单笔仓位占总净值比例 (25% = 1x无杠杆4份)
    "MAKER_FEE": 0.0002,              # 限价止盈手续费 0.02%
    "TAKER_FEE": 0.0005,              # 市价开仓/止损手续费 0.05%
    "SL_SLIPPAGE": 0.001,             # 市价止损模拟滑点 0.10%

    # 止损 / 止盈 / 冷却（对应模式 A 策略参数）
    "STOP_LOSS_PCT": 0.05,            # 止损 5%
    "TAKE_PROFIT_PCT": 0.20,          # 止盈 20%
    "COOLDOWN_SECONDS": 3600,         # 单币种冷却时间 60 分钟（允许主升浪多次回踩，防连续误触发）

    # 强势币与结构筛选参数
    "MIN_24H_QUOTE_VOL": 30_000_000,  # 24h 成交额门槛：3000万 USDT
    "MIN_24H_CHANGE_PCT": 6.0,        # 初筛 24h 涨幅门槛（减少 REST 请求）
    "MIN_72H_SURGE_PCT": 0.15,        # 近 72h 最低波幅要求：15%
    "MAX_RETRACE_RATIO": 0.50,        # 距 72h 高点最大允许回撤比例（不超过涨幅一半）
    "EMA_TREND_PERIOD": 90,           # 1小时核心趋势均线 (EMA90)
    "EMA_SUPPORT_PERIOD": 18,         # 15分钟回踩支撑均线 (EMA18)

    # 模式 A：左侧触线即入触发区（相对 15m EMA18）
    "TOUCH_UPPER_BUFFER": 0.008,      # 支撑线上方 +0.8% 内即触发
    "TOUCH_LOWER_BUFFER": 0.010,      # 支撑线下方 -1.0% 内仍允许接（跌穿太深不接飞刀）

    # 扫描与持久化
    "SCAN_INTERVAL_SEC": 180,         # 每 3 分钟全市场重扫一次结构
    "MAX_CANDIDATES_PER_SCAN": 35,    # 单轮最多校验 35 个币，严格保护 API 权重
    "DB_PATH": "paper_trading.db",
    "WEB_PORT": int(os.getenv("WEB_PORT", "8000")),
}

BLACKLIST = {"BTCUSDT", "ETHUSDT", "BNBUSDT", "USDCUSDT", "FDUSDUSDT", "TUSDUSDT"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("AltMonitor")


# ==================== 数据库管理 (SQLite) ====================
def db_connect():
    conn = sqlite3.connect(CONFIG["DB_PATH"])
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with db_connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS account (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                cash REAL NOT NULL,
                initial_balance REAL NOT NULL,
                updated_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                entry_price REAL NOT NULL,
                qty REAL NOT NULL,
                notional REAL NOT NULL,
                sl_price REAL NOT NULL,
                tp_price REAL NOT NULL,
                support_ema REAL NOT NULL,
                surge_72h REAL NOT NULL,
                opened_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'OPEN'
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                entry_price REAL NOT NULL,
                exit_price REAL NOT NULL,
                notional REAL NOT NULL,
                pnl REAL NOT NULL,
                pnl_pct REAL NOT NULL,
                reason TEXT NOT NULL,
                opened_at TEXT NOT NULL,
                closed_at TEXT NOT NULL
            )
        """)
        row = conn.execute("SELECT * FROM account WHERE id = 1").fetchone()
        if not row:
            now = datetime.now(timezone.utc).isoformat()
            conn.execute(
                "INSERT INTO account (id, cash, initial_balance, updated_at) VALUES (1, ?, ?, ?)",
                (CONFIG["INITIAL_BALANCE"], CONFIG["INITIAL_BALANCE"], now),
            )
        conn.commit()


# ==================== 核心数学与指标计算 ====================
def calc_ema(values: List[float], period: int) -> List[float]:
    if len(values) < period:
        return []
    k = 2.0 / (period + 1)
    ema = [sum(values[:period]) / period]
    for v in values[period:]:
        ema.append(v * k + ema[-1] * (1.0 - k))
    return ema


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ==================== 全局运行时状态 ====================
class TradingEngine:
    def __init__(self):
        self.tickers: Dict[str, dict] = {}       # WS 实时行情缓存
        self.watchlist: Dict[str, dict] = {}     # 满足强势与结构的观察池
        self.cooldowns: Dict[str, float] = {}    # 单币种冷却时间戳
        self.ws_connected: bool = False
        self.last_scan_time: str = "尚未扫描"
        self.scan_count: int = 0
        self.lock = asyncio.Lock()

    def get_cash(self) -> float:
        with db_connect() as conn:
            row = conn.execute("SELECT cash FROM account WHERE id = 1").fetchone()
            return float(row["cash"]) if row else CONFIG["INITIAL_BALANCE"]

    def get_open_positions(self) -> List[dict]:
        with db_connect() as conn:
            rows = conn.execute("SELECT * FROM positions WHERE status = 'OPEN' ORDER BY id DESC").fetchall()
            return [dict(r) for r in rows]

    def get_recent_trades(self, limit: int = 50) -> List[dict]:
        with db_connect() as conn:
            rows = conn.execute("SELECT * FROM trades ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
            return [dict(r) for r in rows]

    async def send_telegram(self, text: str):
        token = CONFIG["TG_BOT_TOKEN"]
        chat_id = CONFIG["TG_CHAT_ID"]
        if not token or not chat_id:
            return
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=10) as resp:
                    if resp.status != 200:
                        logger.warning(f"Telegram 推送失败: {resp.status} {await resp.text()}")
        except Exception as e:
            logger.error(f"Telegram 请求异常: {e}")

    def calc_total_equity(self) -> float:
        cash = self.get_cash()
        positions = self.get_open_positions()
        pos_val = 0.0
        for p in positions:
            price = self.tickers.get(p["symbol"], {}).get("price", p["entry_price"])
            pos_val += p["qty"] * price
        return cash + pos_val

    async def try_open_position(self, symbol: str, price: float, watch_item: dict):
        async with self.lock:
            now_ts = time.time()
            if now_ts < self.cooldowns.get(symbol, 0):
                return

            open_positions = self.get_open_positions()
            if len(open_positions) >= CONFIG["MAX_POSITIONS"]:
                return
            if any(p["symbol"] == symbol for p in open_positions):
                return

            cash = self.get_cash()
            equity = self.calc_total_equity()
            target_notional = min(cash, equity * CONFIG["POSITION_SIZE_PCT"])
            if target_notional < 50.0:  # 最小模拟开仓额 50 U
                return

            entry_fee = target_notional * CONFIG["TAKER_FEE"]
            qty = (target_notional - entry_fee) / price
            sl_price = price * (1.0 - CONFIG["STOP_LOSS_PCT"])
            tp_price = price * (1.0 + CONFIG["TAKE_PROFIT_PCT"])
            opened_at = now_str()

            with db_connect() as conn:
                conn.execute(
                    "UPDATE account SET cash = ?, updated_at = ? WHERE id = 1",
                    (cash - target_notional, opened_at),
                )
                conn.execute(
                    """
                    INSERT INTO positions
                    (symbol, entry_price, qty, notional, sl_price, tp_price, support_ema, surge_72h, opened_at, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN')
                    """,
                    (
                        symbol,
                        price,
                        qty,
                        target_notional,
                        sl_price,
                        tp_price,
                        watch_item["ema18_15m"],
                        watch_item["surge_72h"],
                        opened_at,
                    ),
                )
                conn.commit()

            self.cooldowns[symbol] = now_ts + CONFIG["COOLDOWN_SECONDS"]
            logger.info(f"[模式A模拟开仓] {symbol} @ {price:.6g} | 仓位 {target_notional:.1f}U | SL {sl_price:.6g} | TP {tp_price:.6g}")

            msg = (
                f"🚀 <b>[模式A 触线入场] {symbol}</b>\n"
                f"━━━━━━━━━━━━━━\n"
                f"• 入场价: <code>{price:.6g}</code>\n"
                f"• 15m EMA18 支撑: <code>{watch_item['ema18_15m']:.6g}</code>\n"
                f"• 模拟仓位: <code>{target_notional:.1f} USDT</code> (1x无杠杆)\n"
                f"• 止损位 (-{CONFIG['STOP_LOSS_PCT']*100:.0f}%): <code>{sl_price:.6g}</code>\n"
                f"• 止盈位 (+{CONFIG['TAKE_PROFIT_PCT']*100:.0f}%): <code>{tp_price:.6g}</code>\n"
                f"• 72h 涨幅: <code>+{watch_item['surge_72h']*100:.1f}%</code>\n"
                f"• 时间: {opened_at}"
            )
            asyncio.create_task(self.send_telegram(msg))

    async def close_position(self, pos_id: int, trigger_price: float, reason: str):
        async with self.lock:
            with db_connect() as conn:
                row = conn.execute("SELECT * FROM positions WHERE id = ? AND status = 'OPEN'", (pos_id,)).fetchone()
                if not row:
                    return False
                pos = dict(row)

                if reason == "TAKE_PROFIT":
                    exec_price = pos["tp_price"]
                    fee_rate = CONFIG["MAKER_FEE"]
                elif reason == "STOP_LOSS":
                    exec_price = trigger_price * (1.0 - CONFIG["SL_SLIPPAGE"])
                    fee_rate = CONFIG["TAKER_FEE"]
                else:
                    exec_price = trigger_price
                    fee_rate = CONFIG["TAKER_FEE"]

                proceeds = pos["qty"] * exec_price * (1.0 - fee_rate)
                pnl = proceeds - pos["notional"]
                pnl_pct = (pnl / pos["notional"]) * 100.0
                closed_at = now_str()

                cash = self.get_cash()
                new_cash = cash + proceeds

                conn.execute("UPDATE positions SET status = 'CLOSED' WHERE id = ?", (pos_id,))
                conn.execute("UPDATE account SET cash = ?, updated_at = ? WHERE id = 1", (new_cash, closed_at))
                conn.execute(
                    """
                    INSERT INTO trades
                    (symbol, entry_price, exit_price, notional, pnl, pnl_pct, reason, opened_at, closed_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        pos["symbol"],
                        pos["entry_price"],
                        exec_price,
                        pos["notional"],
                        pnl,
                        pnl_pct,
                        reason,
                        pos["opened_at"],
                        closed_at,
                    ),
                )
                conn.commit()

            self.cooldowns[pos["symbol"]] = time.time() + CONFIG["COOLDOWN_SECONDS"]
            emoji = "✅" if pnl >= 0 else "🛑"
            equity = self.calc_total_equity()
            logger.info(f"[模拟平仓-{reason}] {pos['symbol']} @ {exec_price:.6g} | PnL: {pnl:+.2f}U ({pnl_pct:+.2f}%)")

            msg = (
                f"{emoji} <b>[模拟平仓 - {reason}] {pos['symbol']}</b>\n"
                f"━━━━━━━━━━━━━━\n"
                f"• 开仓价: <code>{pos['entry_price']:.6g}</code>\n"
                f"• 平仓价: <code>{exec_price:.6g}</code>\n"
                f"• 本笔盈亏: <code>{pnl:+.2f} USDT ({pnl_pct:+.2f}%)</code>\n"
                f"• 账户总净值: <code>{equity:.2f} USDT</code>\n"
                f"• 时间: {closed_at}"
            )
            asyncio.create_task(self.send_telegram(msg))
            return True

    async def on_price_tick(self, symbol: str, price: float):
        # 1. 检查现有持仓是否触发止盈或止损
        for p in self.get_open_positions():
            if p["symbol"] != symbol:
                continue
            if price >= p["tp_price"]:
                await self.close_position(p["id"], price, "TAKE_PROFIT")
            elif price <= p["sl_price"]:
                await self.close_position(p["id"], price, "STOP_LOSS")

        # 2. 模式 A：左侧触线即入检查
        watch = self.watchlist.get(symbol)
        if not watch:
            return
        support = watch["ema18_15m"]
        ema90 = watch["ema90_1h"]
        upper_zone = support * (1.0 + CONFIG["TOUCH_UPPER_BUFFER"])
        lower_zone = support * (1.0 - CONFIG["TOUCH_LOWER_BUFFER"])

        # 价格必须仍在 1h EMA90 宏观强趋势上方，且精准触及 15m EMA18 支撑带
        if price > ema90 and lower_zone <= price <= upper_zone:
            await self.try_open_position(symbol, price, watch)


engine = TradingEngine()


# ==================== 后台任务 1：币安合约 WebSocket ====================
async def ws_market_loop():
    url = "wss://fstream.binance.com/ws/!ticker@arr"
    while True:
        try:
            logger.info("正在连接币安合约 WebSocket...")
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_size=4 * 1024 * 1024) as ws:
                engine.ws_connected = True
                logger.info("币安合约 WebSocket 连接成功。")
                async for raw in ws:
                    data = json.loads(raw)
                    for item in data:
                        sym = item.get("s", "")
                        if not sym.endswith("USDT") or sym in BLACKLIST:
                            continue
                        price = float(item["c"])
                        vol = float(item["q"])
                        chg = float(item["P"])
                        engine.tickers[sym] = {
                            "price": price,
                            "vol_24h": vol,
                            "chg_24h": chg,
                            "ts": time.time(),
                        }
                        # 仅对持仓币或观察池币触发实时评估
                        if sym in engine.watchlist or any(p["symbol"] == sym for p in engine.get_open_positions()):
                            await engine.on_price_tick(sym, price)
        except asyncio.CancelledError:
            engine.ws_connected = False
            break
        except Exception as e:
            engine.ws_connected = False
            logger.warning(f"WebSocket 断开 ({e})，5 秒后自动重连...")
            await asyncio.sleep(5)


# ==================== 后台任务 2：强势币与结构扫描器 ====================
async def fetch_klines(session: aiohttp.ClientSession, symbol: str, interval: str, limit: int) -> List[list]:
    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    async with session.get(url, params=params, timeout=10) as resp:
        if resp.status != 200:
            return []
        return await resp.json()


async def scan_strong_coins_once(session: aiohttp.ClientSession):
    if not engine.tickers:
        return

    # 步骤 1：基于 WS 内存数据零成本初筛（高成交额 + 明显涨幅）
    candidates = []
    for sym, t in engine.tickers.items():
        if t["vol_24h"] >= CONFIG["MIN_24H_QUOTE_VOL"] and (
            t["chg_24h"] >= CONFIG["MIN_24H_CHANGE_PCT"] or sym in engine.watchlist
        ):
            candidates.append((sym, t))

    # 按 24h 涨幅排序，最多检验前 N 个标的，严格控制 API 权重
    candidates.sort(key=lambda x: x[1]["chg_24h"], reverse=True)
    candidates = candidates[: CONFIG["MAX_CANDIDATES_PER_SCAN"]]

    new_watchlist: Dict[str, dict] = {}
    for sym, ticker in candidates:
        try:
            klines_1h = await fetch_klines(session, sym, "1h", 160)
            await asyncio.sleep(0.12)
            klines_15m = await fetch_klines(session, sym, "15m", 80)
            await asyncio.sleep(0.12)

            if len(klines_1h) < 100 or len(klines_15m) < 30:
                continue

            # 仅取已收盘 K 线避免指标闪烁重绘
            closed_1h = klines_1h[:-1]
            closed_15m = klines_15m[:-1]

            closes_1h = [float(k[4]) for k in closed_1h]
            highs_72h = [float(k[2]) for k in closed_1h[-72:]]
            lows_72h = [float(k[3]) for k in closed_1h[-72:]]
            closes_15m = [float(k[4]) for k in closed_15m]

            ema90_list = calc_ema(closes_1h, CONFIG["EMA_TREND_PERIOD"])
            ema18_list = calc_ema(closes_15m, CONFIG["EMA_SUPPORT_PERIOD"])
            if len(ema90_list) < 5 or len(ema18_list) < 5:
                continue

            ema90_now, ema90_prev = ema90_list[-1], ema90_list[-4]
            ema18_now, ema18_prev = ema18_list[-1], ema18_list[-3]
            cur_price = ticker["price"]

            high_72h = max(highs_72h)
            low_72h = min(lows_72h)
            if low_72h <= 0 or high_72h <= low_72h:
                continue

            surge_72h = (high_72h - low_72h) / low_72h
            retrace_ratio = (high_72h - cur_price) / (high_72h - low_72h)

            # 硬性过滤条件：强势 + 清晰上升结构
            is_strong = surge_72h >= CONFIG["MIN_72H_SURGE_PCT"]
            is_macro_up = (cur_price > ema90_now) and (ema90_now > ema90_prev)
            is_micro_up = ema18_now > ema18_prev
            is_healthy_pullback = retrace_ratio <= CONFIG["MAX_RETRACE_RATIO"]

            if is_strong and is_macro_up and is_micro_up and is_healthy_pullback:
                new_watchlist[sym] = {
                    "symbol": sym,
                    "ema18_15m": ema18_now,
                    "ema90_1h": ema90_now,
                    "surge_72h": surge_72h,
                    "retrace_ratio": retrace_ratio,
                    "vol_24h": ticker["vol_24h"],
                    "chg_24h": ticker["chg_24h"],
                    "updated_at": now_str(),
                }
        except Exception as e:
            logger.debug(f"扫描 {sym} 出错: {e}")

    engine.watchlist = new_watchlist
    engine.last_scan_time = now_str()
    engine.scan_count += 1
    logger.info(f"第 {engine.scan_count} 轮扫描完成：初筛 {len(candidates)} 个 -> 符合强势清晰结构 {len(new_watchlist)} 个。")


async def scanner_loop():
    await asyncio.sleep(8)  # 等待 WebSocket 预热行情缓存
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                await scan_strong_coins_once(session)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"扫描循环异常: {e}")
            await asyncio.sleep(CONFIG["SCAN_INTERVAL_SEC"])


# ==================== FastAPI Web 服务端 ====================
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    ws_task = asyncio.create_task(ws_market_loop())
    scan_task = asyncio.create_task(scanner_loop())
    asyncio.create_task(
        engine.send_telegram("🟢 <b>山寨币强势监控 + 模式A模拟盘已启动</b>\nWeb 面板与 WebSocket 实时监控运行中。")
    )
    yield
    ws_task.cancel()
    scan_task.cancel()


app = FastAPI(title="Altcoin Mode-A Paper Trader", lifespan=lifespan)


@app.get("/api/state")
async def api_state():
    cash = engine.get_cash()
    positions = engine.get_open_positions()
    trades = engine.get_recent_trades(50)

    pos_view = []
    unrealized_pnl = 0.0
    for p in positions:
        cur_price = engine.tickers.get(p["symbol"], {}).get("price", p["entry_price"])
        cur_val = p["qty"] * cur_price
        upnl = cur_val - p["notional"]
        upnl_pct = (upnl / p["notional"]) * 100.0
        unrealized_pnl += upnl
        pos_view.append({
            **p,
            "current_price": cur_price,
            "unrealized_pnl": upnl,
            "unrealized_pnl_pct": upnl_pct,
        })

    equity = cash + sum(p["qty"] * p["current_price"] for p in pos_view)
    total_pnl = equity - CONFIG["INITIAL_BALANCE"]
    total_pnl_pct = (total_pnl / CONFIG["INITIAL_BALANCE"]) * 100.0

    wins = [t for t in trades if t["pnl"] > 0]
    win_rate = (len(wins) / len(trades) * 100.0) if trades else 0.0

    now_ts = time.time()
    watch_view = []
    for sym, w in engine.watchlist.items():
        cur_price = engine.tickers.get(sym, {}).get("price", w["ema18_15m"])
        dist_pct = ((cur_price - w["ema18_15m"]) / w["ema18_15m"]) * 100.0
        cd_left = max(0, int(engine.cooldowns.get(sym, 0) - now_ts))
        watch_view.append({
            **w,
            "current_price": cur_price,
            "dist_to_ema18_pct": dist_pct,
            "cooldown_left_min": round(cd_left / 60, 1),
        })
    watch_view.sort(key=lambda x: abs(x["dist_to_ema18_pct"]))

    return JSONResponse({
        "status": {
            "ws_connected": engine.ws_connected,
            "last_scan_time": engine.last_scan_time,
            "tracked_tickers": len(engine.tickers),
            "watchlist_count": len(watch_view),
        },
        "account": {
            "initial_balance": CONFIG["INITIAL_BALANCE"],
            "cash": cash,
            "equity": equity,
            "unrealized_pnl": unrealized_pnl,
            "total_pnl": total_pnl,
            "total_pnl_pct": total_pnl_pct,
            "win_rate": win_rate,
            "trade_count": len(trades),
            "open_count": len(pos_view),
            "max_positions": CONFIG["MAX_POSITIONS"],
        },
        "positions": pos_view,
        "watchlist": watch_view,
        "trades": trades,
        "config": {
            "sl_pct": CONFIG["STOP_LOSS_PCT"] * 100,
            "tp_pct": CONFIG["TAKE_PROFIT_PCT"] * 100,
            "ema_support": CONFIG["EMA_SUPPORT_PERIOD"],
            "ema_trend": CONFIG["EMA_TREND_PERIOD"],
        },
    })


@app.post("/api/close/{pos_id}")
async def api_manual_close(pos_id: int):
    positions = engine.get_open_positions()
    target = next((p for p in positions if p["id"] == pos_id), None)
    if not target:
        return JSONResponse({"ok": False, "msg": "持仓不存在"}, status_code=404)
    cur_price = engine.tickers.get(target["symbol"], {}).get("price", target["entry_price"])
    ok = await engine.close_position(pos_id, cur_price, "MANUAL")
    return JSONResponse({"ok": ok})


@app.post("/api/reset")
async def api_reset_account():
    async with engine.lock:
        now = now_str()
        with db_connect() as conn:
            conn.execute("DELETE FROM positions")
            conn.execute("DELETE FROM trades")
            conn.execute(
                "UPDATE account SET cash = ?, initial_balance = ?, updated_at = ? WHERE id = 1",
                (CONFIG["INITIAL_BALANCE"], CONFIG["INITIAL_BALANCE"], now),
            )
            conn.commit()
        engine.cooldowns.clear()
    return JSONResponse({"ok": True})


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return HTMLResponse(DASHBOARD_HTML)


DASHBOARD_HTML = r"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>山寨币强势监控 · 模式A模拟实盘</title>
<style>
  :root {
    --bg: #0f172a; --card: #1e293b; --border: #334155;
    --text: #f1f5f9; --muted: #94a3b8;
    --green: #22c55e; --red: #ef4444; --blue: #38bdf8; --amber: #f59e0b;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: var(--bg); color: var(--text); font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; padding: 20px; line-height: 1.5; }
  .container { max-width: 1280px; margin: 0 auto; }
  header { display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 12px; margin-bottom: 20px; }
  h1 { font-size: 20px; font-weight: 700; }
  .status-bar { display: flex; gap: 12px; align-items: center; font-size: 13px; color: var(--muted); }
  .dot { display: inline-block; width: 9px; height: 9px; border-radius: 50%; margin-right: 5px; }
  .dot.ok { background: var(--green); box-shadow: 0 0 8px var(--green); }
  .dot.err { background: var(--red); }
  .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); gap: 14px; margin-bottom: 22px; }
  .card { background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 16px; }
  .card-label { font-size: 12px; color: var(--muted); margin-bottom: 6px; }
  .card-val { font-size: 22px; font-weight: 700; }
  .pos { color: var(--green); } .neg { color: var(--red); } .accent { color: var(--blue); }
  .section { background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 16px; margin-bottom: 20px; overflow-x: auto; }
  .section-head { display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; }
  h2 { font-size: 16px; font-weight: 600; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; white-space: nowrap; }
  th, td { text-align: left; padding: 10px 12px; border-bottom: 1px solid var(--border); }
  th { color: var(--muted); font-weight: 600; font-size: 12px; }
  tr:hover td { background: rgba(255,255,255,0.03); }
  .badge { padding: 2px 8px; border-radius: 999px; font-size: 11px; font-weight: 600; }
  .b-near { background: rgba(245,158,11,0.2); color: var(--amber); }
  .b-ok { background: rgba(34,197,94,0.2); color: var(--green); }
  .b-sl { background: rgba(239,68,68,0.2); color: var(--red); }
  .b-cd { background: rgba(148,163,184,0.2); color: var(--muted); }
  button { background: #334155; color: var(--text); border: none; padding: 5px 10px; border-radius: 6px; cursor: pointer; font-size: 12px; }
  button:hover { background: #475569; }
  button.danger { background: rgba(239,68,68,0.2); color: var(--red); }
  button.danger:hover { background: rgba(239,68,68,0.35); }
  .empty { text-align: center; color: var(--muted); padding: 24px; }
</style>
</head>
<body>
<div class="container">
  <header>
    <div>
      <h1>山寨币强势监控 · 模式 A 模拟实盘</h1>
      <div id="cfg-info" style="font-size:12px;color:var(--muted);margin-top:4px;">加载策略参数中...</div>
    </div>
    <div class="status-bar">
      <span><span id="ws-dot" class="dot err"></span><span id="ws-txt">连接中</span></span>
      <span>上次扫描: <strong id="scan-time">-</strong></span>
      <button class="danger" onclick="resetAccount()">重置模拟盘</button>
    </div>
  </header>

  <div class="grid">
    <div class="card"><div class="card-label">账户总净值 (USDT)</div><div id="m-equity" class="card-val">-</div></div>
    <div class="card"><div class="card-label">累计盈亏</div><div id="m-pnl" class="card-val">-</div></div>
    <div class="card"><div class="card-label">当前浮动盈亏</div><div id="m-upnl" class="card-val">-</div></div>
    <div class="card"><div class="card-label">可用现金 (1x 无杠杆)</div><div id="m-cash" class="card-val">-</div></div>
    <div class="card"><div class="card-label">胜率 / 成交笔数</div><div id="m-wr" class="card-val accent">-</div></div>
    <div class="card"><div class="card-label">持仓数 / 强势池</div><div id="m-cnt" class="card-val">-</div></div>
  </div>

  <div class="section">
    <div class="section-head"><h2>当前模拟持仓（实时监控 SL / TP）</h2></div>
    <table>
      <thead>
        <tr>
          <th>币种</th><th>入场价</th><th>最新价</th><th>仓位 (U)</th>
          <th>浮动盈亏</th><th>止损位</th><th>止盈位</th><th>开仓时间</th><th>操作</th>
        </tr>
      </thead>
      <tbody id="tb-pos"></tbody>
    </table>
  </div>

  <div class="section">
    <div class="section-head"><h2>强势币观察池（按距 15m EMA18 支撑线由近到远排序）</h2></div>
    <table>
      <thead>
        <tr>
          <th>币种</th><th>最新价</th><th>15m EMA18 支撑</th><th>距支撑线</th>
          <th>1h EMA90</th><th>72h 涨幅</th><th>24h 成交额</th><th>状态</th>
        </tr>
      </thead>
      <tbody id="tb-watch"></tbody>
    </table>
  </div>

  <div class="section">
    <div class="section-head"><h2>历史模拟平仓记录（最近 50 笔）</h2></div>
    <table>
      <thead>
        <tr>
          <th>币种</th><th>入场价</th><th>平仓价</th><th>仓位 (U)</th>
          <th>净盈亏</th><th>平仓原因</th><th>平仓时间</th>
        </tr>
      </thead>
      <tbody id="tb-trades"></tbody>
    </table>
  </div>
</div>

<script>
const fmtPrice = (v) => v >= 100 ? v.toFixed(2) : v >= 1 ? v.toFixed(4) : v.toFixed(6);
const fmtVol = (v) => (v / 1e6).toFixed(1) + 'M';

async function closePos(id) {
  if (!confirm('确认按当前市价手动平掉该模拟仓位？')) return;
  await fetch('/api/close/' + id, { method: 'POST' });
  refresh();
}

async function resetAccount() {
  if (!confirm('确认清空所有持仓与历史战绩，重置为初始本金？')) return;
  await fetch('/api/reset', { method: 'POST' });
  refresh();
}

async function refresh() {
  try {
    const res = await fetch('/api/state');
    const d = await res.json();
    const s = d.status, a = d.account, c = d.config;

    document.getElementById('cfg-info').textContent =
      `入场: 触及 15m EMA${c.ema_support} | 趋势滤网: 1h EMA${c.ema_trend} | 止损 -${c.sl_pct}% / 止盈 +${c.tp_pct}%`;

    document.getElementById('ws-dot').className = 'dot ' + (s.ws_connected ? 'ok' : 'err');
    document.getElementById('ws-txt').textContent = s.ws_connected ? `WS 实时连接 (${s.tracked_tickers} 币种)` : 'WS 重连中';
    document.getElementById('scan-time').textContent = s.last_scan_time;

    document.getElementById('m-equity').textContent = a.equity.toFixed(2);
    const pnlEl = document.getElementById('m-pnl');
    pnlEl.textContent = `${a.total_pnl >= 0 ? '+' : ''}${a.total_pnl.toFixed(2)} (${a.total_pnl_pct >= 0 ? '+' : ''}${a.total_pnl_pct.toFixed(2)}%)`;
    pnlEl.className = 'card-val ' + (a.total_pnl >= 0 ? 'pos' : 'neg');

    const upnlEl = document.getElementById('m-upnl');
    upnlEl.textContent = `${a.unrealized_pnl >= 0 ? '+' : ''}${a.unrealized_pnl.toFixed(2)}`;
    upnlEl.className = 'card-val ' + (a.unrealized_pnl >= 0 ? 'pos' : 'neg');

    document.getElementById('m-cash').textContent = a.cash.toFixed(2);
    document.getElementById('m-wr').textContent = `${a.win_rate.toFixed(1)}% / ${a.trade_count} 笔`;
    document.getElementById('m-cnt').textContent = `${a.open_count}/${a.max_positions} · 池 ${s.watchlist_count}`;

    // 持仓表
    const tbPos = document.getElementById('tb-pos');
    if (!d.positions.length) {
      tbPos.innerHTML = '<tr><td colspan="9" class="empty">当前空仓，等待强势币回踩 15m EMA18 触发模式 A 入场...</td></tr>';
    } else {
      tbPos.innerHTML = d.positions.map(p => `
        <tr>
          <td><strong>${p.symbol}</strong></td>
          <td>${fmtPrice(p.entry_price)}</td>
          <td>${fmtPrice(p.current_price)}</td>
          <td>${p.notional.toFixed(1)}</td>
          <td class="${p.unrealized_pnl >= 0 ? 'pos' : 'neg'}">
            ${p.unrealized_pnl >= 0 ? '+' : ''}${p.unrealized_pnl.toFixed(2)} (${p.unrealized_pnl_pct >= 0 ? '+' : ''}${p.unrealized_pnl_pct.toFixed(2)}%)
          </td>
          <td class="neg">${fmtPrice(p.sl_price)}</td>
          <td class="pos">${fmtPrice(p.tp_price)}</td>
          <td>${p.opened_at}</td>
          <td><button class="danger" onclick="closePos(${p.id})">市价平仓</button></td>
        </tr>
      `).join('');
    }

    // 观察池表
    const tbWatch = document.getElementById('tb-watch');
    if (!d.watchlist.length) {
      tbWatch.innerHTML = '<tr><td colspan="8" class="empty">暂无符合严格强势结构的币种（或正在进行首次扫描）</td></tr>';
    } else {
      tbWatch.innerHTML = d.watchlist.map(w => {
        let badge = '<span class="badge b-ok">监控中</span>';
        if (w.cooldown_left_min > 0) badge = `<span class="badge b-cd">冷却 ${w.cooldown_left_min}m</span>`;
        else if (Math.abs(w.dist_to_ema18_pct) <= 1.5) badge = '<span class="badge b-near">接近触线</span>';
        return `
          <tr>
            <td><strong>${w.symbol}</strong></td>
            <td>${fmtPrice(w.current_price)}</td>
            <td>${fmtPrice(w.ema18_15m)}</td>
            <td class="${Math.abs(w.dist_to_ema18_pct) <= 1.5 ? 'accent' : ''}">${w.dist_to_ema18_pct >= 0 ? '+' : ''}${w.dist_to_ema18_pct.toFixed(2)}%</td>
            <td>${fmtPrice(w.ema90_1h)}</td>
            <td class="pos">+${(w.surge_72h * 100).toFixed(1)}%</td>
            <td>${fmtVol(w.vol_24h)}</td>
            <td>${badge}</td>
          </tr>
        `;
      }).join('');
    }

    // 历史成交表
    const tbTrades = document.getElementById('tb-trades');
    if (!d.trades.length) {
      tbTrades.innerHTML = '<tr><td colspan="7" class="empty">尚无平仓记录</td></tr>';
    } else {
      tbTrades.innerHTML = d.trades.map(t => `
        <tr>
          <td><strong>${t.symbol}</strong></td>
          <td>${fmtPrice(t.entry_price)}</td>
          <td>${fmtPrice(t.exit_price)}</td>
          <td>${t.notional.toFixed(1)}</td>
          <td class="${t.pnl >= 0 ? 'pos' : 'neg'}">${t.pnl >= 0 ? '+' : ''}${t.pnl.toFixed(2)} (${t.pnl_pct >= 0 ? '+' : ''}${t.pnl_pct.toFixed(2)}%)</td>
          <td><span class="badge ${t.reason === 'TAKE_PROFIT' ? 'b-ok' : 'b-sl'}">${t.reason}</span></td>
          <td>${t.closed_at}</td>
        </tr>
      `).join('');
    }
  } catch (err) {
    console.error(err);
  }
}

refresh();
setInterval(refresh, 3000);
</script>
</body>
</html>
"""

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=CONFIG["WEB_PORT"], log_level="info")
