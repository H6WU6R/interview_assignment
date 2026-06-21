from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from execution.engine import ExecutionRecord
from execution.models import (
    Algorithm,
    DeadlinePolicy,
    Environment,
    ExecutionParameters,
    ExecutionRequest,
    RepricingMode,
)


DECIMAL_STRING_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$")


def parse_decimal_string(value: object) -> Decimal:
    if not isinstance(value, str):
        raise ValueError("decimal fields must be JSON strings")
    if not DECIMAL_STRING_RE.fullmatch(value):
        raise ValueError("decimal fields must be plain decimal strings")
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError("invalid decimal string") from exc
    if not parsed.is_finite():
        raise ValueError("decimal fields must be finite")
    return parsed


def decimal_to_string(value: Decimal) -> str:
    return str(value)


class ExecutionParametersCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reprice_threshold_bps: Decimal = Decimal("2.0")
    minimum_reprice_interval_ms: int = 500
    number_of_slices: int = 10
    child_order_timeout_seconds: int = 20
    repricing_mode: RepricingMode = RepricingMode.ADVERSE_ONLY

    @field_validator("reprice_threshold_bps", mode="before")
    @classmethod
    def validate_reprice_threshold_bps(cls, value: object) -> Decimal:
        parsed = parse_decimal_string(value)
        if parsed < Decimal("0"):
            raise ValueError("reprice_threshold_bps must be greater than or equal to 0")
        return parsed

    @field_validator("minimum_reprice_interval_ms")
    @classmethod
    def validate_minimum_reprice_interval_ms(cls, value: int) -> int:
        if value < 0:
            raise ValueError("minimum_reprice_interval_ms must be greater than or equal to 0")
        return value

    @field_validator("number_of_slices")
    @classmethod
    def validate_number_of_slices(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("number_of_slices must be greater than 0")
        return value

    @field_validator("child_order_timeout_seconds")
    @classmethod
    def validate_child_order_timeout_seconds(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("child_order_timeout_seconds must be greater than 0")
        return value

    def to_domain(self) -> ExecutionParameters:
        return ExecutionParameters(
            reprice_threshold_bps=self.reprice_threshold_bps,
            minimum_reprice_interval_ms=self.minimum_reprice_interval_ms,
            number_of_slices=self.number_of_slices,
            child_order_timeout_seconds=self.child_order_timeout_seconds,
            repricing_mode=self.repricing_mode,
        )


class ExecutionCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    environment: Environment
    symbol: str
    algorithm: Algorithm
    target_position: Decimal
    target_price_lower: Decimal
    target_price_upper: Decimal
    target_duration_seconds: int
    deadline_policy: DeadlinePolicy
    parameters: ExecutionParametersCreate = Field(default_factory=ExecutionParametersCreate)

    @field_validator("target_position", "target_price_lower", "target_price_upper", mode="before")
    @classmethod
    def validate_decimal_string(cls, value: object) -> Decimal:
        return parse_decimal_string(value)

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, value: str) -> str:
        symbol = value.upper()
        if symbol != "BTCUSDT":
            raise ValueError("only BTCUSDT is supported")
        return symbol

    @field_validator("target_duration_seconds")
    @classmethod
    def validate_target_duration_seconds(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("target_duration_seconds must be greater than 0")
        return value

    @model_validator(mode="after")
    def validate_price_range(self) -> ExecutionCreateRequest:
        if self.target_price_lower <= Decimal("0"):
            raise ValueError("target_price_lower must be greater than 0")
        if self.target_price_upper <= Decimal("0"):
            raise ValueError("target_price_upper must be greater than 0")
        if self.target_price_lower > self.target_price_upper:
            raise ValueError("target_price_lower must be less than or equal to target_price_upper")
        return self

    def to_domain(self) -> ExecutionRequest:
        return ExecutionRequest(
            environment=self.environment,
            symbol=self.symbol,
            algorithm=self.algorithm,
            target_position=self.target_position,
            target_price_lower=self.target_price_lower,
            target_price_upper=self.target_price_upper,
            target_duration_seconds=self.target_duration_seconds,
            deadline_policy=self.deadline_policy,
            parameters=self.parameters.to_domain(),
        )


class ChildOrderResponse(BaseModel):
    child_order_id: str
    client_order_id: str
    status: str
    side: str
    submitted_quantity: str
    filled_quantity: str
    remaining_quantity: str
    price: str
    terminal_reason: str | None


class ExecutionParametersResponse(BaseModel):
    reprice_threshold_bps: str
    minimum_reprice_interval_ms: int
    number_of_slices: int
    child_order_timeout_seconds: int
    repricing_mode: str


class ExecutionRequestResponse(BaseModel):
    environment: str
    symbol: str
    algorithm: str
    target_position: str
    target_price_lower: str
    target_price_upper: str
    target_duration_seconds: int
    deadline_policy: str
    parameters: ExecutionParametersResponse


class ExecutionResponse(BaseModel):
    execution_id: str
    status: str
    final_reason: str | None
    side: str
    raw_required_quantity: str
    required_quantity: str
    target_dust_quantity: str
    unfilled_quantity: str
    initial_position: str
    confirmed_filled_quantity: str
    live_open_quantity: str
    pending_submit_quantity: str
    pending_cancel_quantity: str
    unknown_order_quantity: str
    reserved_exposure: str
    child_orders: list[ChildOrderResponse]
    request: ExecutionRequestResponse
    summary_final_status: str | None
    summary_final_reason: str | None
    summary_metrics: dict[str, Any] | None
    started_monotonic: str | None
    last_reprice_monotonic: str | None


def execution_response(record: ExecutionRecord) -> ExecutionResponse:
    exposure = record.exposure
    summary = record.summary
    return ExecutionResponse(
        execution_id=record.execution_id,
        status=record.status.value,
        final_reason=record.final_reason,
        side=record.side.value,
        raw_required_quantity=decimal_to_string(record.raw_required_quantity),
        required_quantity=decimal_to_string(record.required_quantity),
        target_dust_quantity=decimal_to_string(record.target_dust_quantity),
        unfilled_quantity=decimal_to_string(unfilled_quantity(record)),
        initial_position=decimal_to_string(record.initial_position.position),
        confirmed_filled_quantity=decimal_to_string(exposure.confirmed_filled_quantity),
        live_open_quantity=decimal_to_string(exposure.live_open_quantity),
        pending_submit_quantity=decimal_to_string(exposure.pending_submit_quantity),
        pending_cancel_quantity=decimal_to_string(exposure.pending_cancel_quantity),
        unknown_order_quantity=decimal_to_string(exposure.unknown_order_quantity),
        reserved_exposure=decimal_to_string(exposure.reserved_exposure),
        child_orders=[
            ChildOrderResponse(
                child_order_id=child.child_order_id,
                client_order_id=child.client_order_id,
                status=child.status.value,
                side=child.side.value,
                submitted_quantity=decimal_to_string(child.submitted_quantity),
                filled_quantity=decimal_to_string(child.confirmed_filled_quantity),
                remaining_quantity=decimal_to_string(child.remaining_quantity),
                price=decimal_to_string(child.price),
                terminal_reason=child.terminal_reason,
            )
            for child in record.child_orders
        ],
        request=request_response(record),
        summary_final_status=summary.final_status.value if summary is not None else None,
        summary_final_reason=summary.final_reason if summary is not None else None,
        summary_metrics=json_safe(summary.metrics) if summary is not None else None,
        started_monotonic=started_monotonic(record),
        last_reprice_monotonic=(
            decimal_to_string(record.last_reprice_monotonic)
            if record.last_reprice_monotonic is not None
            else None
        ),
    )


def unfilled_quantity(record: ExecutionRecord) -> Decimal:
    remaining = record.required_quantity - record.exposure.confirmed_filled_quantity
    return remaining if remaining > Decimal("0") else Decimal("0")


def request_response(record: ExecutionRecord) -> ExecutionRequestResponse:
    request = record.request
    parameters = request.parameters
    return ExecutionRequestResponse(
        environment=request.environment.value,
        symbol=request.symbol,
        algorithm=request.algorithm.value,
        target_position=decimal_to_string(request.target_position),
        target_price_lower=decimal_to_string(request.target_price_lower),
        target_price_upper=decimal_to_string(request.target_price_upper),
        target_duration_seconds=request.target_duration_seconds,
        deadline_policy=request.deadline_policy.value,
        parameters=ExecutionParametersResponse(
            reprice_threshold_bps=decimal_to_string(parameters.reprice_threshold_bps),
            minimum_reprice_interval_ms=parameters.minimum_reprice_interval_ms,
            number_of_slices=parameters.number_of_slices,
            child_order_timeout_seconds=parameters.child_order_timeout_seconds,
            repricing_mode=parameters.repricing_mode.value,
        ),
    )


def started_monotonic(record: ExecutionRecord) -> str | None:
    if record.exposure_tracker is None:
        return None
    return decimal_to_string(record.started_monotonic)


def json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return decimal_to_string(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {key: json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [json_safe(item) for item in value]
    return value
