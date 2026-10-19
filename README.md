# Polyester Testnet Volume Bot

Bot trading otomatis untuk **Polyester Testnet** — membuat volume dengan strategi **Market Order BUY → SELL → BUY** per pair.

## Cara Kerja

- Bot membuka posisi **BUY market order** senilai `$100` per pair
- Setelah BUY terisi (filled), bot langsung pasang **SELL market order** untuk menutup posisi
- Setelah SELL terisi, siklus BUY berikutnya dimulai
- **Aturan:** Bot tidak bisa BUY lagi sebelum posisi sebelumnya di-SELL
- Berjalan pada **13 pair** secara bersamaan (concurrent)

## Pair yang Didukung

`BTC-USDT` `DOGE-USDT` `ETH-USDT` `SOL-USDT` `BNB-USDT` `AVAX-USDT`  
`BCH-USDT` `XRP-USDT` `POL-USDT` `ETC-USDT` `TRX-USDT` `LTC-USDT` `HYPE-USDT`

---

## Setup di VPS (Wispbyte / Panel)

### 1. Clone repo

```bash
git clone https://github.com/<username>/<repo>.git /home/container
cd /home/container
```

### 2. Buat file `.env` dari contoh

```bash
cp .env.example .env
nano .env
```

Isi nilai berikut di `.env`:

```env
POLYESTER_API_KEY_ID=ak_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
POLYESTER_API_PRIVATE_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
POLYESTER_ACCOUNT_ID=xxxxxxxxxxx
TRADING_ENABLED=true
ORDER_SIZE_USDT=100
```

> **⚠️ Penting:** File `.env` **tidak** ada di GitHub (sudah di `.gitignore`).  
> Harus dibuat manual di VPS setiap kali pertama deploy.

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Jalankan bot

```bash
python bot.py
```

Atau gunakan panel Wispbyte untuk set startup command:

```
python /home/container/bot.py
```

---

## Update dari GitHub (setelah ada perubahan)

```bash
cd /home/container
git pull
python bot.py
```

File `.env` dan `bot_state.json` **tidak akan tertimpa** oleh `git pull` karena ada di `.gitignore`.

---

## Struktur File

```
polyester_bot/
├── bot.py              # Bot utama
├── requirements.txt    # Dependensi Python
├── .env.example        # Template konfigurasi (aman di GitHub)
├── .env                # ← Buat sendiri di VPS, TIDAK di-commit
├── bot_state.json      # State runtime (dibuat otomatis, TIDAK di-commit)
└── logs/               # Log runtime (dibuat otomatis, TIDAK di-commit)
```

---

## Variabel Konfigurasi

| Variabel | Default | Keterangan |
|---|---|---|
| `POLYESTER_API_KEY_ID` | *(wajib)* | API Key ID dari dashboard Polyester |
| `POLYESTER_API_PRIVATE_KEY` | *(wajib)* | Private key 64 karakter hex |
| `POLYESTER_ACCOUNT_ID` | *(wajib)* | Account ID dari dashboard Polyester |
| `TRADING_ENABLED` | `false` | Set `true` untuk kirim order nyata ke testnet |
| `ORDER_SIZE_USDT` | `100` | Nilai order per pair (USDT) |
| `POLL_SECONDS` | `5` | Jarak antar siklus (detik) |
| `PNL_SUMMARY_INTERVAL` | `12` | Cetak ringkasan PnL setiap N siklus |
| `API_TIMEOUT_SEC` | `30` | Timeout per API call (detik) |
| `BOT_STATE_FILE` | `bot_state.json` | Lokasi file state |
