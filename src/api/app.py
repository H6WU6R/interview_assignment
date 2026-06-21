from __future__ import annotations

from decimal import Decimal

from fastapi import FastAPI, HTTPException

from api.schemas import ExecutionCreateRequest, ExecutionResponse, execution_response
from exchanges.simulator import DeterministicSimulator
from execution.clock import ManualClock
from execution.engine import ExecutionRecord
from execution.service import ExecutionService


def create_app(simulator_position: str = "0") -> FastAPI:
    clock = ManualClock()
    adapter = DeterministicSimulator(clock=clock, position=Decimal(simulator_position))
    service = ExecutionService(adapter, clock=clock)

    app = FastAPI(title="Calais Execution API")
    app.state.clock = clock
    app.state.adapter = adapter
    app.state.service = service

    @app.post("/executions", response_model=ExecutionResponse)
    async def create_execution(request: ExecutionCreateRequest) -> ExecutionResponse:
        record = await service.create_execution(request.to_domain())
        return execution_response(record)

    @app.get("/executions/{execution_id}", response_model=ExecutionResponse)
    async def get_execution(execution_id: str) -> ExecutionResponse:
        record = await _get_or_404(service.get_execution, execution_id)
        return execution_response(record)

    @app.post("/executions/{execution_id}/cancel", response_model=ExecutionResponse)
    async def cancel_execution(execution_id: str) -> ExecutionResponse:
        record = await _get_or_404(service.cancel_execution, execution_id)
        return execution_response(record)

    @app.post("/executions/{execution_id}/run-once", response_model=ExecutionResponse)
    async def run_once(execution_id: str) -> ExecutionResponse:
        record = await _get_or_404(service.run_once, execution_id)
        return execution_response(record)

    @app.post("/executions/{execution_id}/reconcile", response_model=ExecutionResponse)
    async def reconcile_execution(execution_id: str) -> ExecutionResponse:
        record = await _get_or_404(service.reconcile_execution, execution_id)
        return execution_response(record)

    return app


async def _get_or_404(operation, execution_id: str) -> ExecutionRecord:
    try:
        return await operation(execution_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="execution not found") from exc
