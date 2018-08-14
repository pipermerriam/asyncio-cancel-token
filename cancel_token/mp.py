import asyncio
from multiprocessing.managers import (
    BaseManager,
    BaseProxy,
)
from typing import (
    Any,
    Callable
)

from .token import AbstractCancelToken


def sync_method(method_name: str) -> Callable[..., Any]:
    def method(self: Any, *args: Any, **kwargs: Any) -> Any:
        return self._callmethod(method_name, args, kwargs)
    return method


class CancelTokenProxy(BaseProxy, AbstractCancelToken):
    get_name = sync_method('get_name')

    is_proxy = True

    def get_event_loop(self):
        return asyncio.get_event_loop()

    def chain(self, token: 'CancelToken') -> 'CancelToken':
        raise NotImplementedError(
            "The `CancelToken.chain` API is not available to proxy tokens"
        )

    trigger = sync_method('trigger')

    _get_triggered_token = sync_method('get_triggered_token')

    def get_triggered_token(self):
        if not self.triggered:
            return None
        return self._get_triggered_token()

    check_triggered = sync_method('check_triggered')
    raise_if_triggered = sync_method('raise_if_triggered')

    async def wait(self) -> None:
        condition = asyncio.Condition()
        async with condition:
            await condition.wait_for(self.check_triggered)

    _method_to_typeid_ = {
        'get_triggered_token': 'CancelToken',
        'chain': 'CancelToken',
    }


class BaseCancelTokenManager(BaseManager):
    pass


BaseCancelTokenManager.register('CancelToken', proxytype=CancelTokenProxy, create_method=False)


class CancelTokenClient(BaseManager):
    get_cancel_token: AbstractCancelToken


CancelTokenClient.register('CancelToken', proxytype=CancelTokenProxy, create_method=False)
CancelTokenClient.register(  # type: ignore
    'get_cancel_token',
    proxytype=CancelTokenProxy,
)
