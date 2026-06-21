import re

from execution.ids import child_order_id, client_order_id, execution_id, make_client_order_id, make_client_order_prefix


def test_client_order_id_is_binance_compatible() -> None:
    execution = execution_id()
    child = child_order_id(1)
    client = client_order_id(execution, child)

    assert len(client) <= 36
    assert re.fullmatch(r"^[\.A-Z\:/a-z0-9_-]{1,36}$", client)
    assert client.startswith("ce_")


def test_client_order_id_changes_by_child_sequence() -> None:
    execution = "exec_0123456789abcdef"
    assert client_order_id(execution, child_order_id(1)) != client_order_id(execution, child_order_id(2))


def test_client_order_prefix_matches_derived_order_ids() -> None:
    execution = "exec_0123456789abcdef"
    client = make_client_order_id(execution, 1)

    assert client.startswith(make_client_order_prefix(execution))


def test_client_order_prefix_is_execution_specific() -> None:
    first_execution = "exec_0123456789abcdef"
    second_execution = "exec_fedcba9876543210"

    first_prefix = make_client_order_prefix(first_execution)
    second_prefix = make_client_order_prefix(second_execution)
    first_client = make_client_order_id(first_execution, 1)

    assert first_prefix != second_prefix
    assert first_client.startswith(first_prefix)
    assert not first_client.startswith(second_prefix)
