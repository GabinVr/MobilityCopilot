def test_main_module_importable() -> None:
    import main

    assert main is not None
