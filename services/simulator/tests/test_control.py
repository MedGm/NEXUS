import pytest
from fastapi.testclient import TestClient


def _make_client():
    from src.main import control_app, _config
    # Reset to known defaults before each test
    _config["base_rate"] = 10.0
    _config["burst_probability"] = 0.05
    _config["burst_size"] = 500.0
    return TestClient(control_app)


def test_get_control_returns_all_fields():
    client = _make_client()
    resp = client.get("/control")
    assert resp.status_code == 200
    data = resp.json()
    assert "base_rate" in data
    assert "burst_probability" in data
    assert "burst_size" in data


def test_get_control_returns_defaults():
    client = _make_client()
    data = client.get("/control").json()
    assert data["base_rate"] == 10.0
    assert data["burst_probability"] == 0.05
    assert data["burst_size"] == 500.0


def test_post_control_updates_base_rate():
    client = _make_client()
    resp = client.post("/control", json={"base_rate": 50.0})
    assert resp.status_code == 200
    assert resp.json()["base_rate"] == 50.0


def test_post_control_partial_update_preserves_others():
    client = _make_client()
    client.post("/control", json={"burst_probability": 0.8})
    data = client.get("/control").json()
    assert data["burst_probability"] == 0.8
    assert data["base_rate"] == 10.0
    assert data["burst_size"] == 500.0


def test_post_control_updates_burst_size():
    client = _make_client()
    resp = client.post("/control", json={"burst_size": 200.0})
    assert resp.json()["burst_size"] == 200.0


def test_post_control_all_fields():
    client = _make_client()
    resp = client.post("/control", json={
        "base_rate": 100.0,
        "burst_probability": 0.9,
        "burst_size": 800.0,
    })
    data = resp.json()
    assert data["base_rate"] == 100.0
    assert data["burst_probability"] == 0.9
    assert data["burst_size"] == 800.0
