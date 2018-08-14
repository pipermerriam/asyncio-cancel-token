import asyncio
import functools
import tempfile
import threading
import pytest

import multiprocessing

from cancel_token import (
    BaseCancelTokenManager,
    CancelToken,
    CancelTokenClient,
    CancelTokenProxy,
    OperationCancelled,
)


# @pytest.fixture(params=('tcp', 'ipc'))
@pytest.fixture(params=('ipc',))
def manager_address(request, unused_tcp_port_factory):
    if request.param == 'tcp':
        yield ('', unused_tcp_port_factory())
    elif request.param == 'ipc':
        with tempfile.NamedTemporaryFile(suffix='.ipc') as tmp_ipc_path:
            # we want a unique temporary file path but we also want it to not
            # exist yet so that the Manager class can create the IPC socket in
            # this location.  Thus, we wait till we exit the context so that
            # the filename gets cleaned up.
            pass
        yield tmp_ipc_path.name
    else:
        raise AssertionError('Invariant: must be one of tcp or ipc')


class Transport:
    def __init__(self):
        # internal event used to signal that the subprocess has booted
        self.ready = multiprocessing.Event()

        # signals which side of the process the assertion about triggering is
        # being made.  If set, then the assertion happens in the subprocess on
        # the main token.  If not set, then the assertion happens on the proxy
        # token.
        self.reverse_trigger_check = multiprocessing.Event()

        # Signals in either direction that the other pricess should go ahead
        # and trigger.
        self.should_trigger = multiprocessing.Event()

        # Signals in ether direction that the token has now been triggered.
        self.was_triggered = multiprocessing.Event()

        # Allows the main process to check whether the main token running in
        # the subprocess was indeed triggered.
        self.is_triggered = multiprocessing.Event()

        # internal event used to signal that the subprocess exited
        # successfully.
        self.done = multiprocessing.Event()

    def trigger(self, timeout=1):
        self.should_trigger.set()
        assert self.was_triggered.wait(timeout=1)


@pytest.fixture
def transport():
    return Transport()


@pytest.fixture
def reverse_trigger_check(transport):
    transport.reverse_trigger_check.set()
    return transport


@pytest.fixture(autouse=True)
def base_token_proc(manager_address, transport):
    proc = multiprocessing.Process(
        target=run_base_token_process,
        args=(manager_address, transport),
    )
    proc.start()

    try:
        assert transport.ready.wait(timeout=1)
        yield proc
        transport.done.set()
    finally:
        proc.terminate()
        proc.join()


def run_base_token_process(address, transport):
    class TokenManager(BaseCancelTokenManager):
        pass

    token = CancelToken('base')

    TokenManager.register(
        'get_cancel_token',
        callable=lambda: token,
        proxytype=CancelTokenProxy,
    )

    manager = TokenManager(address=address)
    server = manager.get_server()

    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    try:
        # signal that the manager server has started
        transport.ready.set()

        if not transport.reverse_trigger_check.wait(timeout=0.01):
            # In this case the triggering happens here in this process.
            try:
                # wait for the test process to finish any pre-checks prior to triggering
                # the cancel token.
                assert transport.should_trigger.wait(timeout=1)
            except TimeoutError:
                pass
            else:
                token.trigger()
                # signal that the token has now been triggered.
                transport.was_triggered.set()
        else:
            # In this case the triggering happens in the other process on the
            # proxy token.
            transport.should_trigger.set()

            assert transport.was_triggered.wait(timeout=1)

            if token.triggered:
                # communicate back to the other process that the token has been
                # triggered.
                transport.is_triggered.set()

        assert transport.done.wait(timeout=1)
    except TimeoutError:
        pass
    finally:
        server.stop_event.set()

    server_thread.join()


@pytest.fixture
def token(manager_address, transport):
    manager = CancelTokenClient(manager_address)
    manager.connect()

    _token = manager.get_cancel_token()

    # sanity checks
    assert _token.name == 'base'
    assert _token.check_triggered() is False
    assert _token.triggered is False
    assert _token.get_triggered_token() is None
    assert _token.triggered_token is None

    return _token


def test_proxy_token_remote_cancellation(token, transport):
    transport.trigger()

    assert token.triggered
    assert token.triggered_token.name == 'base'


def test_proxy_token_local_cancellation(token, transport, reverse_trigger_check):
    token.trigger()

    transport.was_triggered.set()

    assert transport.is_triggered.wait(timeout=1)

    assert token.triggered_token.name == 'base'


def test_local_token_can_chain_with_proxy_token(token, transport):
    local_token = CancelToken('local').chain(token)

    assert not local_token.triggered

    transport.trigger()

    assert local_token.triggered
    assert local_token.triggered_token.name == 'base'


@pytest.mark.asyncio
async def test_proxy_token_wait(token, transport):
    async def wait_till_triggered():
        await token.wait()
        return True, token.triggered_token.name

    task = asyncio.ensure_future(wait_till_triggered())

    transport.trigger()

    was_triggered, triggered_token_name = await task
    assert was_triggered
    assert triggered_token_name == 'base'


@pytest.mark.asyncio
async def test_proxy_token_cancellable_wait(token, transport):
    async def wait_for_future_or_timeout():
        loop = token.get_event_loop()
        fut = asyncio.Future()
        loop.call_soon(functools.partial(fut.set_result, 'result'))
        result = await token.cancellable_wait(fut, timeout=1)
        return result == 'result'

    task = asyncio.ensure_future(wait_for_future_or_timeout())

    was_successful = await task
    assert was_successful


@pytest.mark.asyncio
async def test_proxy_token_cancellable_wait_with_timeout(token, transport):
    async def wait_for_future_or_timeout():
        loop = token.get_event_loop()
        fut = asyncio.Future()
        loop.call_soon(functools.partial(asyncio.sleep, 1))
        try:
            await token.cancellable_wait(fut, timeout=0.01)
        except TimeoutError:
            return True
        else:
            return False

    task = asyncio.ensure_future(wait_for_future_or_timeout())

    was_successful = await task
    assert was_successful


@pytest.mark.asyncio
async def test_proxy_token_cancellable_wait_operation_cancelled_from_remote(token, transport):
    transport.trigger()

    with pytest.raises(OperationCancelled):
        await token.cancellable_wait(asyncio.sleep(0.02))


@pytest.mark.asyncio
async def test_proxy_token_cancellable_wait_operation_cancelled_from_local(token):
    token.trigger()

    with pytest.raises(OperationCancelled):
        await token.cancellable_wait(asyncio.sleep(0.02))
