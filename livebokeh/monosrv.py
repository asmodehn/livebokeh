"""
A minimalist async server for visualization
"""
import asyncio
import typing

from bokeh.document import Document
from bokeh.models import PreText
from bokeh.server.server import Server as BokehServer


async def monosrv(applications: typing.Dict[str, typing.Callable[[Document], typing.Any]]):
    """ Async server runner, to force the eventloop -same as the server loop- to be already running..."""
    print(f"Starting Tornado Server...")
    # Server will take current running asyncio loop as his own.
    server = BokehServer(applications=applications, io_loop=None, num_procs=1)
    # ioloop must remain to none, num_procs must be default (1)
    # TODO : maybe better to explicitely set io_loop to current loop here...

    server.start()
    # TODO : how to handle exceptions here ??
    #  we would like to except, trigger some user-defined behavior and restart what needs to be.
    print('Serving Bokeh application on http://localhost:5006/')

    await asyncio.sleep(3600)  # running for one hour.
    # TODO : scheduling restart (crontab ? cli params ?) -> GOAL: ensure resilience (erlang-style)


def _internal_bokeh(doc):
    import inspect
    doc.add_root(
        PreText(text=inspect.getsource(monosrv))  # TODO : niceties like pygments ??
    )


if __name__ == '__main__':
    try:
        asyncio.run(monosrv({'/': _internal_bokeh}))
    except KeyboardInterrupt:
        print("Exiting...")


