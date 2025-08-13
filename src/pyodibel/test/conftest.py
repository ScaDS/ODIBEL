import pytest

def pytest_addoption(parser):
    parser.addoption(
        "--run-slow", action="store_true", default=False,
        help="Only run tests marked as slow"
    )

def pytest_collection_modifyitems(config, items):
    run_slow = config.getoption("--run-slow")

    selected = []
    deselected = []

    for item in items:
        is_slow = "slow" in item.keywords
        if run_slow:
            # Keep only slow tests
            if is_slow:
                selected.append(item)
            else:
                deselected.append(item)
        else:
            # Keep only fast (i.e., non-slow) tests
            if is_slow:
                deselected.append(item)
            else:
                selected.append(item)

    items[:] = selected
    config.hook.pytest_deselected(items=deselected)