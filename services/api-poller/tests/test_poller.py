from unittest.mock import Mock
from src.poller import fetch_prices


def test_fetch_prices_returns_two_events_for_full_response():
    mock_data = {
        "bitcoin": {
            "usd": 68000.0, "usd_market_cap": 1.34e12,
            "usd_24h_vol": 2.8e10, "usd_24h_change": 2.5,
        },
        "ethereum": {
            "usd": 3800.0, "usd_market_cap": 4.56e11,
            "usd_24h_vol": 1.5e10, "usd_24h_change": -1.2,
        },
    }
    mock_resp = Mock()
    mock_resp.json.return_value = mock_data
    mock_resp.raise_for_status.return_value = None
    mock_client = Mock()
    mock_client.get.return_value = mock_resp

    events = fetch_prices(mock_client)

    assert len(events) == 2
    btc = next(e for e in events if e["symbol"] == "BTC")
    assert btc["price_usd"] == 68000.0
    assert btc["pct_change_24h"] == 2.5
    assert "event_id" in btc
    assert "timestamp" in btc
    assert "sampled_at" in btc


def test_fetch_prices_skips_missing_coin():
    mock_data = {
        "bitcoin": {
            "usd": 68000.0, "usd_market_cap": 1.34e12,
            "usd_24h_vol": 2.8e10, "usd_24h_change": 2.5,
        }
    }
    mock_resp = Mock()
    mock_resp.json.return_value = mock_data
    mock_resp.raise_for_status.return_value = None
    mock_client = Mock()
    mock_client.get.return_value = mock_resp

    events = fetch_prices(mock_client)

    assert len(events) == 1
    assert events[0]["symbol"] == "BTC"


def test_fetch_prices_event_has_required_schema_fields():
    mock_data = {
        "bitcoin": {
            "usd": 68000.0, "usd_market_cap": 1.34e12,
            "usd_24h_vol": 2.8e10, "usd_24h_change": 2.5,
        }
    }
    mock_resp = Mock()
    mock_resp.json.return_value = mock_data
    mock_resp.raise_for_status.return_value = None
    mock_client = Mock()
    mock_client.get.return_value = mock_resp

    events = fetch_prices(mock_client)
    e = events[0]
    required = {"event_id", "timestamp", "symbol", "price_usd",
                "market_cap_usd", "volume_24h_usd", "pct_change_24h", "sampled_at"}
    assert required <= e.keys()
