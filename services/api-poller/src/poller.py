import time
import uuid
import logging
import httpx

log = logging.getLogger(__name__)

_COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"
_SYMBOLS = ["bitcoin", "ethereum"]
_TICKER_MAP = {"bitcoin": "BTC", "ethereum": "ETH"}


def fetch_prices(client: httpx.Client) -> list[dict]:
    response = client.get(
        _COINGECKO_URL,
        params={
            "ids": ",".join(_SYMBOLS),
            "vs_currencies": "usd",
            "include_market_cap": "true",
            "include_24hr_vol": "true",
            "include_24hr_change": "true",
        },
        timeout=10,
    )
    response.raise_for_status()
    data = response.json()

    now_ms = int(time.time() * 1000)
    now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    events = []
    for coin_id, ticker in _TICKER_MAP.items():
        if coin_id not in data:
            log.warning("CoinGecko response missing %s", coin_id)
            continue
        coin = data[coin_id]
        events.append({
            "event_id": str(uuid.uuid4()),
            "timestamp": now_ms,
            "symbol": ticker,
            "price_usd": float(coin.get("usd", 0)),
            "market_cap_usd": float(coin.get("usd_market_cap", 0)),
            "volume_24h_usd": float(coin.get("usd_24h_vol", 0)),
            "pct_change_24h": float(coin.get("usd_24h_change", 0)),
            "sampled_at": now_iso,
        })
    return events
