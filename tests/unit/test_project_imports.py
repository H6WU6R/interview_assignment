def test_project_packages_importable() -> None:
    import api
    import algorithms
    import exchanges
    import execution
    import risk
    import observability

    assert api is not None
    assert algorithms is not None
    assert exchanges is not None
    assert execution is not None
    assert risk is not None
    assert observability is not None
