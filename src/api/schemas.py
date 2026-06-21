from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation

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
        return Decimal(value)
    except InvalidOperation as exc:
        raise ValueError("invalid decimal string") from exc


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
        return parse_decimal_string(value)

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

    @field_validator("target_duration_seconds")
    @classmethod
    def validate_target_duration_seconds(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("target_duration_seconds must be greater than 0")
        return value

    @model_validator(mode="after")
    def validate_price_range(self) -> ExecutionCreateRequest:
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


class ExecutionResponse(BaseModel):
    execution_id: str
    status: str
    final_reason: str | None
    side: str
    required_quantity: str
    initial_position: str
    confirmed_filled_quantity: str
    live_open_quantity: str
    pending_submit_quantity: str
    pending_cancel_quantity: str
    unknown_order_quantity: str
    reserved_exposure: str
    child_orders: list[ChildOrderResponse]


def execution_response(record: ExecutionRecord) -> ExecutionResponse:
    exposure = record.exposure
    return ExecutionResponse(
        execution_id=record.execution_id,
        status=record.status.value,
        final_reason=record.final_reason,
        side=record.side.value,
        required_quantity=decimal_to_string(record.required_quantity),
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
    )
