from unittest.mock import AsyncMock, MagicMock

from iceprod.materialization.service import MaterializationService

def test_materialization_service_init(mocker):
    mat_mock = mocker.patch('iceprod.materialization.service.Materialize', autospec=True)
    MaterializationService(MagicMock(), MagicMock())
    mat_mock.assert_called()

async def test_materialization_service_run_no_work(mocker):
    db = AsyncMock()
    mat_mock = mocker.patch('iceprod.materialization.service.Materialize', autospec=True).return_value
    ms = MaterializationService(db, MagicMock())

    db.materialization.find_one_and_update.return_value = None

    await ms._run_once()

    mat_mock.run_once.assert_not_called()
    assert ms.last_cleanup_time != None
    assert ms.last_success_time == None

async def test_materialization_service_run(mocker):
    db = AsyncMock()
    mat_mock = mocker.patch('iceprod.materialization.service.Materialize', autospec=True).return_value
    ms = MaterializationService(db, MagicMock())

    db.materialization.find_one_and_update.return_value = {
        'materialization_id': '0123',
        'dataset_id': 'foo',
        'num': 10,
        'set_status': 'waiting',
    }

    await ms._run_once()

    mat_mock.run_once.assert_called()
    assert ms.last_success_time != None
    