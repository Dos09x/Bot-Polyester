from __future__ import annotations
import asyncio
import http.server
import inspect
import json
import logging
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from enum import Enum
from pathlib import Path
from uuid import uuid4
from dotenv import load_dotenv


# ═══════════════════════════════════════════════════════════════════
# HEALTH CHECK SERVER (untuk platform PaaS / Inhank Cloud)
# Platform expect HTTP response agar deployment dianggap sehat.
# Bot bukan web app, jadi kita jalankan mini HTTP server di thread.
# ═══════════════════════════════════════════════════════════════════

def _start_health_server() -> None:
    port = int(os.getenv("PORT", "8080"))

    class _Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            body = b"OK"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_):  # suppress access log spam
            pass

    srv = http.server.HTTPServer(("0.0.0.0", port), _Handler)
    srv.serve_forever()


_health_thread = threading.Thread(target=_start_health_server, daemon=True)
_health_thread.start()

# ── Import kompatibel SDK lama (0.1.0a55) dan baru ─────────────────────────
try:
    from polyester.environment import POLYESTER_TESTNET_ENVIRONMENT
except ImportError:
    from polyester import POLYESTER_TESTNET_ENVIRONMENT  # type: ignore

# ── Patch URL testnet (domain lama tidak ada di DNS) ───────────────────────
_URL_PATCH = {
    "api_url":       ("https://api-testnet.polyester.com", "https://api.testnet.polyester.com"),
    "websocket_url": ("wss://api-testnet.polyester.com",   "wss://api.testnet.polyester.com"),
}
for _attr, (_old, _new) in _URL_PATCH.items():
    if getattr(POLYESTER_TESTNET_ENVIRONMENT, _attr, "") == _old:
        try:
            object.__setattr__(POLYESTER_TESTNET_ENVIRONMENT, _attr, _new)
        except (AttributeError, TypeError):
            import polyester.environment as _penv
            _e  = POLYESTER_TESTNET_ENVIRONMENT
            _d  = {k: v for k, v in vars(_e).items()}
            _d[_attr] = _new
            _penv.POLYESTER_TESTNET_ENVIRONMENT = _e.__class__(**_d)
            POLYESTER_TESTNET_ENVIRONMENT = _penv.POLYESTER_TESTNET_ENVIRONMENT

try:
    from polyester.client import AsyncPolyester
    from polyester.codecs.ledger_amounts import format_ledger_u128
    from polyester.models.order_key import ClientOrderId, OrderId
except (ImportError, ModuleNotFoundError):
    from polyester import AsyncPolyester                          # type: ignore[no-redef]
    from polyester.codecs.ledger_amounts import format_ledger_u128  # type: ignore[no-redef]
    from polyester.models.order_key import ClientOrderId, OrderId   # type: ignore[no-redef]


# ═══════════════════════════════════════════════════════════════════
# KONFIGURASI
# ═══════════════════════════════════════════════════════════════════

load_dotenv()

PAIRS = [
    "BTC-USDT", "DOGE-USDT", "ETH-USDT",  "SOL-USDT",
    "BNB-USDT", "AVAX-USDT", "BCH-USDT",  "XRP-USDT",
    "POL-USDT", "ETC-USDT",  "TRX-USDT",  "LTC-USDT",
    "HYPE-USDT",
]

# Presisi qty yang diterima exchange per pair.
# Exchange menegakkan step size yang lebih ketat dari catalog scale.
# Nilai ini didapatkan dari pengujian empiris langsung ke TESTNET.
PAIR_QTY_PRECISION: dict[str, int] = {
    "BTC-USDT":  8,
    "DOGE-USDT": 6,  # catalog=8 → exchange tolak; max 6
    "ETH-USDT":  6,
    "SOL-USDT":  6,
    "BNB-USDT":  6,
    "AVAX-USDT": 6,
    "BCH-USDT":  6,  # catalog=8 → exchange tolak; max 6
    "XRP-USDT":  6,
    "POL-USDT":  2,  # catalog=6 → exchange tolak; max 2
    "ETC-USDT":  6,  # catalog=8 → exchange tolak; max 6
    "TRX-USDT":  6,
    "LTC-USDT":  6,  # catalog=8 → exchange tolak; max 6
    "HYPE-USDT": 4,  # catalog=6 → exchange tolak; max 4
}

D = Decimal

ORDER_SIZE      = D(os.getenv("ORDER_SIZE_USDT",       "100"))
POLL_SECONDS    = int(os.getenv("POLL_SECONDS",        "5"))
PNL_LOG_EVERY   = int(os.getenv("PNL_SUMMARY_INTERVAL","12"))
TRADING_ENABLED = os.getenv("TRADING_ENABLED", "false").lower() == "true"
API_KEY_ID      = os.getenv("POLYESTER_API_KEY_ID",      "").strip()
API_PRIV_KEY    = os.getenv("POLYESTER_API_PRIVATE_KEY", "").strip()
ACCOUNT_ID      = os.getenv("POLYESTER_ACCOUNT_ID",      "").strip()
API_TIMEOUT_SEC = int(os.getenv("API_TIMEOUT_SEC", "30"))
# Setelah market BUY terkirim, tunggu N detik sebelum cek fill (market biasanya langsung fill)
FILL_WAIT_SEC   = int(os.getenv("FILL_WAIT_SEC", "3"))

if ORDER_SIZE <= D("0"):
    raise ValueError("ORDER_SIZE_USDT harus > 0.")
if TRADING_ENABLED and not all([API_KEY_ID, API_PRIV_KEY, ACCOUNT_ID]):
    raise RuntimeError("TRADING_ENABLED=true tapi kredensial belum diisi.")
if TRADING_ENABLED and len(API_PRIV_KEY.removeprefix("0x")) != 64:
    raise RuntimeError("Private key harus 64 karakter hex.")


# ═══════════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════════

Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/bot.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("polyester-bot")


# ═══════════════════════════════════════════════════════════════════
# STATE MACHINE
# ═══════════════════════════════════════════════════════════════════

class Phase(str, Enum):
    BUYING  = "BUYING"   # Tidak ada posisi; akan pasang market BUY
    SELLING = "SELLING"  # Ada posisi; akan pasang market SELL


# ═══════════════════════════════════════════════════════════════════
# DATA MODEL
# ═══════════════════════════════════════════════════════════════════

@dataclass
class Position:
    symbol:      str
    qty:         Decimal
    entry_price: Decimal   # avg fill price saat BUY
    entry_cost:  Decimal
    opened_at:   str


@dataclass
class PendingOrder:
    symbol:     str
    side:       str        # "buy" | "sell"
    order_id:   str
    qty_req:    Decimal
    created_at: str
    key_type:   str = "order"   # "order" = server ID, "client" = client_order_id


# ═══════════════════════════════════════════════════════════════════
# GLOBAL STATE
# ═══════════════════════════════════════════════════════════════════

STATE_FILE = Path(os.getenv("BOT_STATE_FILE", "bot_state.json"))

phase:          dict[str, Phase]                = {s: Phase.BUYING for s in PAIRS}
positions:      dict[str, Position | None]      = {s: None         for s in PAIRS}
pending_orders: dict[str, PendingOrder | None]  = {s: None         for s in PAIRS}

daily_pnl   = D("0")
daily_date  = datetime.now(timezone.utc).date()
cycle_count = 0

# Cache last_price (USDT) dari market overview — diperbarui berkala
_last_price: dict[str, Decimal] = {}

# Throttle log warning agar tidak spam
_warn_ts: dict[str, float] = {}


# ═══════════════════════════════════════════════════════════════════
# STATE PERSISTENCE
# ═══════════════════════════════════════════════════════════════════

def save_state() -> None:
    payload: dict = {
        "phase":      {s: v.value for s, v in phase.items()},
        "positions":  {},
        "pending":    {},
        "daily_pnl":  str(daily_pnl),
        "daily_date": str(daily_date),
    }
    for sym, pos in positions.items():
        if pos is not None:
            payload["positions"][sym] = {
                "symbol":      pos.symbol,
                "qty":         str(pos.qty),
                "entry_price": str(pos.entry_price),
                "entry_cost":  str(pos.entry_cost),
                "opened_at":   pos.opened_at,
            }
    for sym, pnd in pending_orders.items():
        if pnd is not None:
            payload["pending"][sym] = {
                "symbol":     pnd.symbol,
                "side":       pnd.side,
                "order_id":   pnd.order_id,
                "qty_req":    str(pnd.qty_req),
                "created_at": pnd.created_at,
                "key_type":   pnd.key_type,
            }
    tmp = STATE_FILE.with_suffix(STATE_FILE.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp.replace(STATE_FILE)


def load_state() -> None:
    global daily_pnl, daily_date
    if not STATE_FILE.exists():
        return
    try:
        data = json.loads(STATE_FILE.read_text(encoding="utf-8"))

        for sym, v in data.get("phase", {}).items():
            if sym in phase:
                try:
                    phase[sym] = Phase(v)
                except ValueError:
                    phase[sym] = Phase.BUYING

        for sym, p in data.get("positions", {}).items():
            if sym in positions:
                positions[sym] = Position(
                    symbol=p["symbol"],
                    qty=D(p["qty"]),
                    entry_price=D(p["entry_price"]),
                    entry_cost=D(p["entry_cost"]),
                    opened_at=p["opened_at"],
                )

        for sym, p in data.get("pending", {}).items():
            if sym in pending_orders:
                pending_orders[sym] = PendingOrder(
                    symbol=p["symbol"],
                    side=p["side"],
                    order_id=p["order_id"],
                    qty_req=D(p["qty_req"]),
                    created_at=p["created_at"],
                    key_type=p.get("key_type", "order"),
                )

        today = datetime.now(timezone.utc).date()
        saved_str = data.get("daily_date", "")
        if saved_str:
            from datetime import date as _dt
            try:
                saved_d = _dt.fromisoformat(saved_str)
                if saved_d < today:
                    daily_pnl  = D("0")
                    daily_date = today
                    log.info("Hari baru; PnL harian direset.")
                else:
                    daily_date = saved_d
                    daily_pnl  = D(data.get("daily_pnl", "0"))
            except ValueError:
                pass

        n_pos = sum(1 for p in positions.values() if p is not None)
        n_pnd = sum(1 for p in pending_orders.values() if p is not None)
        log.warning("State dipulihkan: %d posisi, %d pending. PnL=%s", n_pos, n_pnd, daily_pnl)
    except Exception as exc:
        log.error("State file error (%s) — mulai state kosong.", exc)


# ═══════════════════════════════════════════════════════════════════
# UTILITIES
# ═══════════════════════════════════════════════════════════════════

def new_cid(prefix: str) -> str:
    return f"{prefix}-{uuid4().hex[:24]}"

def fmt(v: Decimal) -> str:
    """Format Decimal sebagai string fixed-point (tanpa 'E+' notation)."""
    return format(v, "f")

def to_decimal(value) -> Decimal:
    """
    Konversi Price object dari SDK ke Decimal USDT.
    SDK: Price(ticks=N) → N / 10^9 USDT  (nanosecond precision).
    Fallback: coba parse sebagai string/Decimal biasa.
    """
    if value is None:
        raise ValueError("Nilai None tidak bisa dikonversi.")
    ticks = getattr(value, "ticks", None)
    if ticks is not None:
        return D(str(ticks)) / D("1000000000")
    try:
        r = D(str(value))
        if not r.is_finite():
            raise ValueError(f"Tidak finite: {value!r}")
        return r
    except Exception as exc:
        raise ValueError(f"Tidak bisa konversi {type(value).__name__}({value!r})") from exc

def warn_throttled(key: str, msg: str, *args, every: int = 60) -> None:
    """Log warning maksimal 1x per `every` detik untuk key yang sama."""
    now = time.monotonic()
    if now - _warn_ts.get(key, 0.0) >= every:
        _warn_ts[key] = now
        log.warning(msg, *args)


# ═══════════════════════════════════════════════════════════════════
# MARKET DATA
# ═══════════════════════════════════════════════════════════════════

async def refresh_prices(client) -> None:
    """
    Ambil last_price semua pair dari market overview.
    Price dari SDK = Price(ticks=N) → N / 10^9 USDT (sudah benar).
    """
    try:
        overview = await asyncio.wait_for(
            client.market_overview.list(
                symbols=PAIRS, limit=len(PAIRS), include_sparklines=False,
            ),
            API_TIMEOUT_SEC,
        )
        updated = 0
        for m in overview.markets:
            if m.symbol in PAIRS and m.last_price is not None:
                try:
                    _last_price[m.symbol] = to_decimal(m.last_price)
                    updated += 1
                except Exception:
                    pass
        missing = [s for s in PAIRS if s not in _last_price]
        log.info("Harga diperbarui: %d/%d pair.%s",
                 updated, len(PAIRS),
                 f" Belum ada data: {missing}" if missing else "")
    except Exception as exc:
        log.warning("Gagal refresh harga pasar: %r", exc)


async def get_usdt_balance(client) -> Decimal:
    balances = await asyncio.wait_for(client.balances.list(), API_TIMEOUT_SEC)
    lid = client.catalogs.ledger_id_for_asset("USDT")
    if lid is None:
        raise RuntimeError("Ledger ID USDT tidak ditemukan.")
    rows = [r for r in balances.balances if r.asset_id == lid]
    if not rows:
        raise RuntimeError("Saldo USDT tidak ditemukan di balances.")
    available = D(format_ledger_u128(rows[0].available))
    log.info("Saldo USDT tersedia: %s", available)
    return available


# ═══════════════════════════════════════════════════════════════════
# KIRIM MARKET ORDER
# ═══════════════════════════════════════════════════════════════════

async def send_market_order(
    client, symbol: str, side: str, qty: Decimal
) -> str | None:
    """
    Kirim market order. Return server order_id, atau None jika gagal.

    Tidak ada field 'price' → tidak ada masalah tick size sama sekali.
    Market order dieksekusi langsung di harga terbaik yang tersedia.
    """
    cid = new_cid(side)
    req = {
        "symbol":          symbol,
        "side":            side,
        "order_type":      "market",
        "qty":             fmt(qty),
        "client_order_id": cid,
    }

    last = _last_price.get(symbol, D("1"))
    notional = float(qty * last)
    log.info("%s MARKET %s qty=%s (~%.2f USDT) [cid=%s]",
             symbol, side.upper(), qty, notional, cid[-8:])

    try:
        result = await asyncio.wait_for(client.orders.create(**req), API_TIMEOUT_SEC)
        oid = str(result.order_id)
        log.info("%s %s order diterima: server_id=%s status=%s",
                 symbol, side.upper(), oid, result.status)
        return oid
    except Exception as exc:
        log.error("%s MARKET %s gagal (cid=%s): %s", symbol, side.upper(), cid[-8:], exc)
        return None


# ═══════════════════════════════════════════════════════════════════
# CEK STATUS ORDER
# ═══════════════════════════════════════════════════════════════════

_TERMINAL = frozenset({
    "filled", "rejected", "cancelled", "canceled",
    "expired", "done", "complete", "completed",
})

async def check_order_status(client, pnd: PendingOrder) -> tuple[str, Decimal, Decimal] | None:
    """
    Ambil status order dari exchange.
    Return (status, filled_qty, avg_price) atau None jika error/tidak bisa lookup.
    status = "open" | "filled" | "rejected" | ... (string dari exchange)
    """
    get_fn = client.orders.get
    try:
        params = inspect.signature(get_fn).parameters
    except Exception:
        params = {}

    order = None
    try:
        if "key" in params:
            key = (ClientOrderId(pnd.order_id) if pnd.key_type == "client"
                   else OrderId(pnd.order_id))
            result = await asyncio.wait_for(get_fn(key=key), API_TIMEOUT_SEC)
            order  = getattr(result, "order", result)
        elif "order_id" in params and pnd.key_type != "client":
            result = await asyncio.wait_for(get_fn(order_id=pnd.order_id), API_TIMEOUT_SEC)
            order  = getattr(result, "order", result)
        else:
            log.error("%s: orders.get tidak support lookup; reset pending.", pnd.symbol)
            return ("cancelled", D("0"), D("0"))
    except asyncio.TimeoutError:
        log.warning("%s: lookup order timeout; coba lagi.", pnd.symbol)
        return None
    except Exception as exc:
        warn_throttled(f"{pnd.symbol}:lookup",
                       "%s: lookup order %s gagal: %r", pnd.symbol, pnd.order_id, exc)
        return None

    if order is None:
        # Order belum muncul di read model — tunggu sebentar lagi
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(pnd.created_at)).total_seconds()
        if age > 300:
            log.warning("%s: order %s tidak muncul >5 menit; anggap cancelled.",
                        pnd.symbol, pnd.order_id)
            return ("cancelled", D("0"), D("0"))
        return None

    status_raw = str(order.status).strip().lower().rsplit(".", 1)[-1]
    status = status_raw.removeprefix("order_status_").removeprefix("status_")

    if status not in _TERMINAL:
        return ("open", D("0"), D("0"))

    # Parse fill qty & avg price
    try:
        scale = client.catalogs.base_quantity_scale_for_symbol(pnd.symbol)
        if scale is None:
            log.error("%s: base_quantity_scale tidak tersedia.", pnd.symbol)
            return (status, D("0"), D("0"))

        if order.cum_qty is None or order.avg_px is None:
            # Rejected / cancelled tanpa fill
            return (status, D("0"), D("0"))

        filled_qty = D(order.cum_qty.format(scale=scale))
        avg_price  = to_decimal(order.avg_px)
        return (status, filled_qty, avg_price)

    except Exception as exc:
        log.error("%s: parse fill gagal: %r", pnd.symbol, exc)
        return (status, D("0"), D("0"))


# ═══════════════════════════════════════════════════════════════════
# PROSES TIAP PAIR
# ═══════════════════════════════════════════════════════════════════

async def process_pair(client, symbol: str) -> None:
    """
    State machine per pair:

    BUYING  → Kirim market BUY ($100). Setelah fill → simpan posisi → SELLING.
    SELLING → Kirim market SELL (qty dari posisi). Setelah fill → catat PnL → BUYING.

    Bot tidak bisa open BUY lagi sebelum SELL posisi lama selesai.
    """
    global daily_pnl, daily_date

    # Reset PnL jika hari berganti
    today = datetime.now(timezone.utc).date()
    if today != daily_date:
        daily_date = today
        daily_pnl  = D("0")
        log.info("Hari UTC baru; PnL harian direset.")

    cur_phase = phase.get(symbol, Phase.BUYING)
    cur_pos   = positions.get(symbol)
    cur_pnd   = pending_orders.get(symbol)

    try:
        # ═══ FASE BUYING ════════════════════════════════════════════
        if cur_phase == Phase.BUYING:

            # Ada pending BUY → cek apakah sudah fill
            if cur_pnd is not None and cur_pnd.side == "buy":
                info = await check_order_status(client, cur_pnd)
                if info is None:
                    return  # Coba lagi cycle berikutnya

                status, filled_qty, avg_price = info

                if status == "open":
                    # Market order harusnya fill cepat; tunggu saja
                    return

                if filled_qty > 0 and avg_price > 0:
                    # BUY fill (penuh atau parsial)
                    positions[symbol] = Position(
                        symbol=symbol,
                        qty=filled_qty,
                        entry_price=avg_price,
                        entry_cost=filled_qty * avg_price,
                        opened_at=cur_pnd.created_at,
                    )
                    pending_orders[symbol] = None
                    phase[symbol]          = Phase.SELLING
                    save_state()
                    log.info("%s BUY filled: qty=%s avg=%s cost=~%.2f USDT → SELLING",
                             symbol, filled_qty, avg_price, float(filled_qty * avg_price))
                else:
                    # Rejected / cancelled tanpa fill → ulangi BUY
                    pending_orders[symbol] = None
                    phase[symbol]          = Phase.BUYING
                    save_state()
                    log.warning("%s BUY tidak fill (status=%s); akan retry.", symbol, status)
                return

            # Posisi sudah ada (restore dari state) → langsung SELLING
            if cur_pos is not None:
                phase[symbol] = Phase.SELLING
                save_state()
                log.info("%s: posisi dari state restore → switch SELLING.", symbol)
                return

            # Tidak ada pending, tidak ada posisi → kirim BUY baru
            if not TRADING_ENABLED:
                last = _last_price.get(symbol, D("0"))
                log.info("%s [DRY-RUN] akan BUY ~%.2f USDT @ ~%s", symbol, float(ORDER_SIZE), last)
                return

            last = _last_price.get(symbol)
            if last is None or last <= 0:
                warn_throttled(f"{symbol}:noprice",
                               "%s: harga belum tersedia; skip BUY.", symbol)
                return

            # Gunakan presisi yang sudah diuji ke exchange (lebih ketat dari catalog scale)
            scale = client.catalogs.base_quantity_scale_for_symbol(symbol)
            precision = PAIR_QTY_PRECISION.get(symbol, min(scale or 6, 6))
            qty = (ORDER_SIZE / last).quantize(D(1).scaleb(-precision), rounding=ROUND_DOWN)
            if qty <= 0:
                warn_throttled(f"{symbol}:zeroqty",
                               "%s: qty=0 (last=%s ORDER=%s); skip.", symbol, last, ORDER_SIZE)
                return

            oid = await send_market_order(client, symbol, "buy", qty)
            if oid is None:
                return  # Error sudah dilog; coba lagi cycle berikutnya

            pending_orders[symbol] = PendingOrder(
                symbol=symbol, side="buy", order_id=oid,
                qty_req=qty,
                created_at=datetime.now(timezone.utc).isoformat(),
                key_type="order",
            )
            save_state()

        # ═══ FASE SELLING ════════════════════════════════════════════
        elif cur_phase == Phase.SELLING:

            # Ada pending SELL → cek status
            if cur_pnd is not None and cur_pnd.side == "sell":
                info = await check_order_status(client, cur_pnd)
                if info is None:
                    return

                status, filled_qty, avg_price = info

                if status == "open":
                    return

                if filled_qty > 0 and avg_price > 0 and cur_pos is not None:
                    sold     = min(filled_qty, cur_pos.qty)
                    realized = (avg_price - cur_pos.entry_price) * sold
                    daily_pnl += realized
                    log.info("%s SELL filled: qty=%s avg=%s pnl=%+.6f USDT (PnL_hari=%s)",
                             symbol, sold, avg_price, float(realized), daily_pnl)
                else:
                    if status not in ("filled",):
                        log.warning("%s SELL status=%s qty=%s; lanjut ke BUYING.",
                                    symbol, status, filled_qty)

                positions[symbol]      = None
                pending_orders[symbol] = None
                phase[symbol]          = Phase.BUYING
                save_state()
                log.info("%s: posisi ditutup → BUYING.", symbol)
                return

            # Posisi tidak ada → seharusnya tidak terjadi; perbaiki state
            if cur_pos is None:
                log.warning("%s: SELLING tapi posisi kosong; reset ke BUYING.", symbol)
                pending_orders[symbol] = None
                phase[symbol]          = Phase.BUYING
                save_state()
                return

            # Ada posisi, belum ada pending SELL → kirim SELL
            if not TRADING_ENABLED:
                log.info("%s [DRY-RUN] akan SELL qty=%s", symbol, cur_pos.qty)
                return

            # Gunakan presisi yang sudah diuji ke exchange (sama dengan BUY)
            scale = client.catalogs.base_quantity_scale_for_symbol(symbol)
            precision = PAIR_QTY_PRECISION.get(symbol, min(scale or 6, 6))
            qty = cur_pos.qty.quantize(D(1).scaleb(-precision), rounding=ROUND_DOWN)
            if qty <= 0:
                log.warning("%s: qty posisi tidak valid (%s); reset.", symbol, cur_pos.qty)
                positions[symbol] = None
                phase[symbol]     = Phase.BUYING
                save_state()
                return

            oid = await send_market_order(client, symbol, "sell", qty)
            if oid is None:
                return

            pending_orders[symbol] = PendingOrder(
                symbol=symbol, side="sell", order_id=oid,
                qty_req=qty,
                created_at=datetime.now(timezone.utc).isoformat(),
                key_type="order",
            )
            save_state()

    except Exception:
        log.exception("%s: error tidak terduga di process_pair.", symbol)


# ═══════════════════════════════════════════════════════════════════
# PNL SUMMARY
# ═══════════════════════════════════════════════════════════════════

def log_pnl_summary() -> None:
    n_pos = sum(1 for p in positions.values() if p is not None)
    n_pnd = sum(1 for p in pending_orders.values() if p is not None)
    log.info("=== PnL SUMMARY === PnL_harian=%s | posisi=%d | pending=%d | pairs=%d",
             daily_pnl, n_pos, n_pnd, len(PAIRS))
    for sym in PAIRS:
        pos = positions.get(sym)
        pnd = pending_orders.get(sym)
        ph  = phase.get(sym, "?")
        if pos or pnd:
            log.info("  [%s] phase=%-8s pos=%s pnd=%s",
                     sym, ph,
                     f"qty={pos.qty} entry={pos.entry_price}" if pos else "-",
                     f"{pnd.side.upper()} id=...{pnd.order_id[-8:]}" if pnd else "-")


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════

async def main() -> None:
    global cycle_count
    load_state()

    log.info("=" * 65)
    log.info("Polyester Testnet Volume Bot — Market Order | BUY→SELL→BUY")
    log.info("ORDER_SIZE=%.2f USDT | PAIRS=%d", float(ORDER_SIZE), len(PAIRS))
    log.info("PAIRS: %s", PAIRS)
    log.info("Aturan: BUY dulu → harus SELL sebelum BUY lagi (per pair)")
    if TRADING_ENABLED:
        log.warning("MODE TRADING AKTIF — order MARKET dikirim ke TESTNET.")
    else:
        log.warning("MODE DRY-RUN — tidak ada order dikirim.")
    log.info("=" * 65)

    async with AsyncPolyester(
        environment=POLYESTER_TESTNET_ENVIRONMENT,
        api_key_id=API_KEY_ID or None,
        api_private_key=API_PRIV_KEY or None,
        default_account_id=ACCOUNT_ID or None,
    ) as client:

        await client.wait_for_catalogs()
        log.info("Katalog pasar dimuat.")

        # Ambil harga awal
        await refresh_prices(client)

        if TRADING_ENABLED:
            try:
                bal = await get_usdt_balance(client)
                if bal < ORDER_SIZE:
                    raise RuntimeError(f"Saldo {bal} USDT < ORDER_SIZE {ORDER_SIZE} USDT")
            except Exception as exc:
                log.error("Cek saldo gagal: %s", exc)

        # Main loop
        while True:
            try:
                cycle_count += 1

                # Refresh harga tiap 60 siklus (~5 menit dengan POLL=5)
                if cycle_count % 60 == 1:
                    await refresh_prices(client)

                # Jalankan semua 13 pair secara concurrent
                tasks   = [process_pair(client, sym) for sym in PAIRS]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for sym, res in zip(PAIRS, results):
                    if isinstance(res, Exception):
                        log.error("%s: exception di gather: %r", sym, res)

                if cycle_count % PNL_LOG_EVERY == 0:
                    log_pnl_summary()

                await asyncio.sleep(max(POLL_SECONDS, 2))

            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Kesalahan loop utama; lanjut dalam 5 detik.")
                await asyncio.sleep(5)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Bot dihentikan oleh user.")
    except RuntimeError as exc:
        log.error("Bot tidak bisa start: %s", exc)