from decimal import getcontext


def pytest_configure() -> None:
    getcontext().prec = 28
