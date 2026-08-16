import pytest

from collector.repository import SearchRepository


@pytest.fixture
async def repo(tmp_path):
    repository = SearchRepository(str(tmp_path / "state.db"))
    await repository.open()
    yield repository
    await repository.close()
