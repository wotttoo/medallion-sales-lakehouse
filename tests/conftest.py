"""Fixture dung chung cho test."""

import pytest

from common.spark import get_spark


@pytest.fixture(scope="session")
def spark():
    """Mot SparkSession cho ca phien test."""
    session = get_spark("sales_lakehouse_tests")
    yield session
    session.stop()
