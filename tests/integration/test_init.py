import pytest
from in_n_out_sdk import health_check

@pytest.mark.dependency(name='health_check', scope='session')
def test_health_check():
    assert health_check()