from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock


from iceprod.materialization.service import MaterializationService, TimeoutException


def make_fake_context(return_value):
    exception_mock = MagicMock()

    @asynccontextmanager
    async def test(*args, **kwargs):
        try:
            yield return_value
        except Exception as e:
            exception_mock(e)
            raise

    return test, exception_mock


def test_materialization_service_init(mocker):
    mat_mock = mocker.patch('iceprod.materialization.service.Materialize', autospec=True)
    MaterializationService(MagicMock(), MagicMock())
    mat_mock.assert_called()

async def test_materialization_service_run_no_work(mocker):
    db = AsyncMock()
    mat_mock = mocker.patch('iceprod.materialization.service.Materialize', autospec=True).return_value
    ms = MaterializationService(message_queue=db, rest_client=MagicMock())

    db.process_next, process_next_exception = make_fake_context(None)

    await ms.run(loop=False)

    mat_mock.run_once.assert_not_called()
    assert process_next_exception.called == False


async def test_materialization_service_run(mocker):
    db = AsyncMock()
    mat_mock = mocker.patch('iceprod.materialization.service.Materialize', autospec=True).return_value
    ms = MaterializationService(message_queue=db, rest_client=MagicMock())

    db.process_next, process_next_exception = make_fake_context({
        'dataset_id': 'foo',
        'num': 10,
        'set_status': 'waiting',
    })

    await ms.run(loop=False)

    assert mat_mock.run_once.called
    assert process_next_exception.called == False


async def test_materialization_service_run_no_datasets(mocker):
    db = AsyncMock()
    mat_mock = mocker.patch('iceprod.materialization.service.Materialize', autospec=True).return_value
    ms = MaterializationService(message_queue=db, rest_client=MagicMock())

    db.process_next, process_next_exception = make_fake_context({
        'dataset_id': 'foo',
        'num': 10,
        'set_status': 'waiting',
    })

    mat_mock.run_once = AsyncMock(return_value=True)

    await ms.run(loop=False)

    assert mat_mock.run_once.called
    assert process_next_exception.called == False


async def test_materialization_service_run_error(mocker):
    db = AsyncMock()
    mat_mock = mocker.patch('iceprod.materialization.service.Materialize', autospec=True).return_value
    e = Exception()
    mat_mock.run_once = AsyncMock(side_effect=e)
    ms = MaterializationService(message_queue=db, rest_client=MagicMock())

    db.process_next, process_next_exception = make_fake_context({
        'dataset_id': 'foo',
        'num': 10,
        'set_status': 'waiting',
    })

    await ms.run(loop=False)

    assert mat_mock.run_once.called
    assert process_next_exception.call_args[0][0] == e


async def test_materialization_service_run_too_long(mocker):
    db = AsyncMock()
    mat_mock = mocker.patch('iceprod.materialization.service.Materialize', autospec=True).return_value
    ms = MaterializationService(message_queue=db, rest_client=MagicMock())

    db.process_next, process_next_exception = make_fake_context({
        'dataset_id': 'foo',
        'num': 10,
        'set_status': 'waiting',
    })

    mat_mock.run_once = AsyncMock(return_value=False)

    await ms.run(loop=False)

    assert mat_mock.run_once.called
    assert mat_mock.run_once.call_args.kwargs == {
        'only_dataset': 'foo',
        'num': 10,
        'set_status': 'waiting',
    }
    assert type(process_next_exception.call_args[0][0]) == TimeoutException
