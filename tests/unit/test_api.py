from __future__ import annotations

from decimal import Decimal
from typing import Any

import httpx
import pytest

from api.app import create_app


SYMBOL = "BTCUSDT"


def execution_payload(
    *,
    target_position: str = "0.010",
    target_price_lower: str = "94000",
    target_price_upper: str = "97000",
    parameters: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "environment": "simulation",
        "symbol": SYMBOL,
        "algorithm": "CHASE",
        "target_position": target_position,
        "target_price_lower": target_price_lower,
        "target_price_upper": target_price_upper,
        "target_duration_seconds": 300,
        "deadline_policy": "AGGRESSIVE_WITHIN_RANGE",
    }
    if parameters is not None:
        payload["parameters"] = parameters
    return payload


async def post_json(app: Any, url: str, payload: dict[str, Any] | None = None) -> httpx.Response:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.post(url, json=payload)


async def get_json(app: Any, url: str) -> httpx.Response:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.get(url)


def assert_decimal_field(body: dict[str, Any], field: str, expected: str) -> None:
    assert isinstance(body[field], str)
    assert Decimal(body[field]) == Decimal(expected)


@pytest.mark.asyncio
async def test_create_execution_no_action_completes() -> None:
    app = create_app(simulator_position="0.010")

    response = await post_json(app, "/executions", execution_payload())

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "COMPLETED"
    assert body["final_reason"] == "NO_ACTION_TARGET_ALREADY_REACHED"
    assert body["child_orders"] == []
    assert_decimal_field(body, "raw_required_quantity", "0")
    assert_decimal_field(body, "required_quantity", "0")
    assert_decimal_field(body, "target_dust_quantity", "0")
    assert_decimal_field(body, "unfilled_quantity", "0")
    assert_decimal_field(body, "initial_position", "0.010")
    assert body["side"] == "NO_ACTION"
    assert body["request"] == {
        "environment": "simulation",
        "symbol": SYMBOL,
        "algorithm": "CHASE",
        "target_position": "0.010",
        "target_price_lower": "94000",
        "target_price_upper": "97000",
        "target_duration_seconds": 300,
        "deadline_policy": "AGGRESSIVE_WITHIN_RANGE",
        "parameters": {
            "reprice_threshold_bps": "2.0",
            "minimum_reprice_interval_ms": 500,
            "number_of_slices": 10,
            "child_order_timeout_seconds": 20,
            "repricing_mode": "ADVERSE_ONLY",
        },
    }
    assert body["summary_final_status"] == "COMPLETED"
    assert body["summary_final_reason"] == "NO_ACTION_TARGET_ALREADY_REACHED"
    summary_metrics = body["summary_metrics"]
    assert summary_metrics["target_position"] == "0.010"
    assert summary_metrics["side"] == "NO_ACTION"
    assert summary_metrics["child_order_count"] == 0
    assert_decimal_field(summary_metrics, "initial_position", "0.010")
    assert_decimal_field(summary_metrics, "required_quantity", "0")
    assert body["started_monotonic"] is None
    assert body["last_reprice_monotonic"] is None


@pytest.mark.asyncio
async def test_cancel_terminal_execution_is_idempotent_and_preserves_reason() -> None:
    app = create_app(simulator_position="0.010")
    created = (await post_json(app, "/executions", execution_payload())).json()

    first = await post_json(app, f"/executions/{created['execution_id']}/cancel")
    second = await post_json(app, f"/executions/{created['execution_id']}/cancel")

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["status"] == "COMPLETED"
    assert second.json()["status"] == "COMPLETED"
    assert first.json()["final_reason"] == "NO_ACTION_TARGET_ALREADY_REACHED"
    assert second.json()["final_reason"] == "NO_ACTION_TARGET_ALREADY_REACHED"


@pytest.mark.asyncio
async def test_create_nonzero_execution_then_get_returns_running_buy() -> None:
    app = create_app(simulator_position="0")
    created_response = await post_json(app, "/executions", execution_payload())
    created = created_response.json()

    fetched_response = await get_json(app, f"/executions/{created['execution_id']}")

    assert created_response.status_code == 200
    assert fetched_response.status_code == 200
    fetched = fetched_response.json()
    assert fetched["execution_id"] == created["execution_id"]
    assert fetched["status"] == "RUNNING"
    assert fetched["side"] == "BUY"
    assert_decimal_field(fetched, "raw_required_quantity", "0.010")
    assert_decimal_field(fetched, "required_quantity", "0.010")
    assert_decimal_field(fetched, "target_dust_quantity", "0")
    assert_decimal_field(fetched, "unfilled_quantity", "0.010")
    assert fetched["child_orders"] == []


@pytest.mark.asyncio
async def test_create_execution_below_step_returns_untradeable_dust_fields() -> None:
    app = create_app(simulator_position="0")

    response = await post_json(app, "/executions", execution_payload(target_position="0.0005"))

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "COMPLETED"
    assert body["final_reason"] == "UNTRADEABLE_TARGET_DUST"
    assert_decimal_field(body, "raw_required_quantity", "0.0005")
    assert_decimal_field(body, "required_quantity", "0")
    assert_decimal_field(body, "target_dust_quantity", "0.0005")
    assert_decimal_field(body, "unfilled_quantity", "0")
    assert body["child_orders"] == []


@pytest.mark.asyncio
async def test_run_once_creates_child_order_when_market_data_is_present() -> None:
    app = create_app(simulator_position="0")
    created = (await post_json(app, "/executions", execution_payload())).json()
    await app.state.adapter.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), 10)

    response = await post_json(app, f"/executions/{created['execution_id']}/run-once")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "RUNNING"
    assert len(body["child_orders"]) == 1
    child = body["child_orders"][0]
    assert child["child_order_id"] == "child_0001"
    assert child["status"] == "OPEN"
    assert child["side"] == "BUY"
    assert child["submitted_quantity"] == "0.010"
    assert child["filled_quantity"] == "0"
    assert child["remaining_quantity"] == "0.010"
    assert child["price"] == "95000.00"
    assert child["terminal_reason"] is None


@pytest.mark.asyncio
async def test_terminal_response_serializes_rich_summary_metrics() -> None:
    app = create_app(simulator_position="0")
    created = (await post_json(app, "/executions", execution_payload(target_position="0.004"))).json()
    await app.state.adapter.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), 10)
    opened = (await post_json(app, f"/executions/{created['execution_id']}/run-once")).json()
    child = opened["child_orders"][0]
    await app.state.adapter.push_fill(child["client_order_id"], Decimal("0.004"), Decimal("95010.00"))

    response = await post_json(app, f"/executions/{created['execution_id']}/reconcile")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "COMPLETED"
    metrics = body["summary_metrics"]
    assert metrics["final_status"] == "COMPLETED"
    assert metrics["raw_required_quantity"] == "0.004"
    assert metrics["required_quantity"] == "0.004"
    assert Decimal(metrics["target_dust_quantity"]) == Decimal("0")
    assert metrics["filled_quantity"] == "0.004"
    assert metrics["unfilled_quantity"] == "0"
    assert metrics["completion_rate"] == "1"
    assert metrics["arrival_bid"] == "95000"
    assert metrics["arrival_ask"] == "95001"
    assert metrics["arrival_mid"] == "95000.5"
    assert metrics["execution_vwap"] == "95010"
    assert Decimal(metrics["slippage_bps"]) > Decimal("0")
    assert metrics["requested_duration_seconds"] == 300
    assert metrics["actual_duration_seconds"] == "0"
    assert metrics["max_reserved_exposure"] == "0.004"
    assert metrics["overfill_quantity"] == "0"


@pytest.mark.asyncio
async def test_reconcile_endpoint_returns_current_state() -> None:
    app = create_app(simulator_position="0")
    created = (await post_json(app, "/executions", execution_payload())).json()

    response = await post_json(app, f"/executions/{created['execution_id']}/reconcile")

    assert response.status_code == 200
    body = response.json()
    assert body["execution_id"] == created["execution_id"]
    assert body["status"] == "RUNNING"
    assert body["child_orders"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method,path",
    [
        ("GET", "/executions/missing"),
        ("POST", "/executions/missing/cancel"),
        ("POST", "/executions/missing/run-once"),
        ("POST", "/executions/missing/reconcile"),
    ],
)
async def test_unknown_execution_id_returns_404(method: str, path: str) -> None:
    app = create_app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.request(method, path)

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_internal_key_error_is_not_converted_to_404(monkeypatch: pytest.MonkeyPatch) -> None:
    app = create_app()

    async def raise_key_error(execution_id: str) -> None:
        raise KeyError("internal state bug")

    monkeypatch.setattr(app.state.service, "get_execution", raise_key_error)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        with pytest.raises(KeyError, match="internal state bug"):
            await client.get("/executions/anything")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        execution_payload(target_position=0.010),  # type: ignore[arg-type]
        execution_payload(target_price_lower=94000),  # type: ignore[arg-type]
        execution_payload(target_price_upper=97000),  # type: ignore[arg-type]
        execution_payload(parameters={"reprice_threshold_bps": 2.0}),
    ],
)
async def test_json_float_decimal_fields_are_rejected(payload: dict[str, Any]) -> None:
    app = create_app()

    response = await post_json(app, "/executions", payload)

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_lower_greater_than_upper_is_rejected() -> None:
    app = create_app()

    response = await post_json(
        app,
        "/executions",
        execution_payload(target_price_lower="97000", target_price_upper="94000"),
    )

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_empty_parameters_default_and_repricing_mode_accepts_enum() -> None:
    app = create_app(simulator_position="0")
    created = await post_json(
        app,
        "/executions",
        execution_payload(parameters={"repricing_mode": "TWO_SIDED"}),
    )
    empty = await post_json(app, "/executions", execution_payload(parameters={}))

    assert created.status_code == 200
    assert empty.status_code == 200
    assert created.json()["status"] == "RUNNING"
    assert empty.json()["status"] == "RUNNING"


@pytest.mark.asyncio
async def test_response_child_and_exposure_decimal_fields_are_strings_after_run_once() -> None:
    app = create_app(simulator_position="0")
    created = (await post_json(app, "/executions", execution_payload())).json()
    await app.state.adapter.push_market_data(SYMBOL, Decimal("95000.00"), Decimal("95001.00"), 10)

    response = await post_json(app, f"/executions/{created['execution_id']}/run-once")

    assert response.status_code == 200
    body = response.json()
    for field in [
        "raw_required_quantity",
        "required_quantity",
        "target_dust_quantity",
        "unfilled_quantity",
        "confirmed_filled_quantity",
        "live_open_quantity",
        "pending_submit_quantity",
        "pending_cancel_quantity",
        "unknown_order_quantity",
        "reserved_exposure",
    ]:
        assert isinstance(body[field], str)

    child = body["child_orders"][0]
    for field in ["submitted_quantity", "filled_quantity", "remaining_quantity", "price"]:
        assert isinstance(child[field], str)
    assert body["request"]["target_position"] == "0.010"
    assert body["request"]["target_price_lower"] == "94000"
    assert body["request"]["target_price_upper"] == "97000"
    assert body["request"]["parameters"]["reprice_threshold_bps"] == "2.0"
    assert body["summary_final_status"] is None
    assert body["summary_final_reason"] is None
    assert body["summary_metrics"] is None
    assert body["started_monotonic"] == "0.0"
    assert body["last_reprice_monotonic"] is None
