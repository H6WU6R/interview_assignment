"""FastAPI application factory for execution service endpoints."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

from api.runtime import (
    ActiveExecutionConflict,
    ExecutionRuntime,
    RuntimeConfigurationError,
    RuntimeUnavailableError,
)
from api.schemas import ExecutionCreateRequest, ExecutionResponse, execution_response
from exchanges.base import VenueBanHardStop
from execution.engine import ExecutionRecord, UnknownExecution


def create_app(
    simulator_position: str = "0",
    *,
    background_tick_interval_seconds: float = 0.25,
) -> FastAPI:
    """Create the FastAPI application and wire the execution runtime."""

    runtime = ExecutionRuntime(
        simulator_position=simulator_position,
        background_tick_interval_seconds=background_tick_interval_seconds,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await runtime.start()
        try:
            yield
        finally:
            await runtime.stop()

    app = FastAPI(title="Calais Execution API", lifespan=lifespan)
    app.state.runtime = runtime
    app.state.clock = runtime.simulation_clock
    app.state.adapter = runtime.simulation_adapter
    app.state.service = runtime.simulation_service

    @app.post("/executions", response_model=ExecutionResponse)
    async def create_execution(request: ExecutionCreateRequest) -> ExecutionResponse:
        try:
            record = await runtime.create_execution(request.to_domain())
        except ActiveExecutionConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except RuntimeConfigurationError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except RuntimeUnavailableError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        return execution_response(record)

    @app.get("/executions/{execution_id}", response_model=ExecutionResponse)
    async def get_execution(execution_id: str) -> ExecutionResponse:
        record = await _get_or_404(runtime.get_execution, execution_id)
        return execution_response(record)

    @app.post("/executions/{execution_id}/cancel", response_model=ExecutionResponse)
    async def cancel_execution(execution_id: str) -> ExecutionResponse:
        record = await _get_or_404(runtime.cancel_execution, execution_id)
        return execution_response(record)

    @app.post("/executions/{execution_id}/run-once", response_model=ExecutionResponse)
    async def run_once(execution_id: str) -> ExecutionResponse:
        record = await _get_or_404(runtime.run_once, execution_id)
        return execution_response(record)

    @app.post("/executions/{execution_id}/reconcile", response_model=ExecutionResponse)
    async def reconcile_execution(execution_id: str) -> ExecutionResponse:
        record = await _get_or_404(runtime.reconcile_execution, execution_id)
        return execution_response(record)

    return app


async def _get_or_404(
    operation: Callable[[str], Awaitable[ExecutionRecord]],
    execution_id: str,
) -> ExecutionRecord:
    try:
        return await operation(execution_id)
    except UnknownExecution as exc:
        raise HTTPException(status_code=404, detail="execution not found") from exc
    except RuntimeUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except VenueBanHardStop as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
