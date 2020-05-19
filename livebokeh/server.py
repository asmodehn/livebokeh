
# THREADED


# Note : use bind_sockets to create the sockets
import asyncio
import threading
from datetime import datetime, timedelta
import random

import pandas

from aiokraken.domain.models.figures.bokehliveview import BokehLiveView
from bokeh.server.server import BaseServer, HTTPServer, Server

from bokeh.application import Application
from bokeh.application.handlers import FunctionHandler
from bokeh.layouts import column, row
from bokeh.server.tornado import BokehTornado
from bokeh.server.util import bind_sockets
from tornado.ioloop import IOLoop

from aiokraken.domain.models.figures.ddbokeh import DDSharedDataSource


def bk_server_thread(sockets, host, port, applications, extra_websocket_origins=None, bg_task=None):

    if not threading.current_thread() is threading.main_thread():
        # we need to manually create an event loop when we are not in the main thread
        asyncio.set_event_loop(asyncio.new_event_loop())

    if extra_websocket_origins is None:
        extra_websocket_origins = []

    # appending direct connection for development/debugging puposes
    extra_websocket_origins.append(f"{host}:{port}")

    # building tornado server for bokeh
    bokeh_tornado = BokehTornado(applications, extra_websocket_origins=extra_websocket_origins)
    bokeh_http = HTTPServer(bokeh_tornado)
    bokeh_http.add_sockets(sockets)

    # creating eventloop and instantiating bokeh server
    server = BaseServer(IOLoop.current(), bokeh_tornado, bokeh_http)

    print(f"Bokeh listening on http://{host}:{port}...")
    print(f"WS Connection expected from {[e for e in extra_websocket_origins]}")

    if bg_task:
        server.io_loop.spawn_callback(bg_task, -10, 10)

    server.start()
    server.io_loop.start()


if __name__ == '__main__':

    sockets, port = bind_sockets("localhost", 0)


    def test_page(doc):

        doc.add_root(
            column(
                row(...),
            )
        )

    # can't use shortcuts here, since we are passing to low level BokehTornado
    bkapp = Application(FunctionHandler(test_page))

    try:
        bk_server_thread(sockets, host="localhost", port=port, applications={'/bkapp': bkapp}, bg_task=compute_random)
    except KeyboardInterrupt as ke:
        print(f"KeyboardInterrupt: Exiting.")

# ASYNC
def start_tornado(bkapp):
    # Server will take current running asyncio loop as his own.
    server = Server({'/': bkapp})  # iolopp must remain to none, num_procs must be default (1)
    server.start()
    return server

async def main():

    print(f"Starting Tornado Server...")
    server = start_tornado(bkapp=test_page)
    # Note : the bkapp is run for each request to the url...

    # bg async task...TMP deactivate, DEBUG problem on first render
    asyncio.create_task(compute_random(-10, 10))

    print('Serving Bokeh application on http://localhost:5006/')
    # server.io_loop.add_callback(server.show, "/")

    # THIS is already the loop that is currently running !!!
    assert server.io_loop.asyncio_loop == asyncio.get_running_loop(), f"{server.io_loop.asyncio_loop} != {asyncio.get_running_loop()}"
    # server.io_loop.start()  # DONT NEED !

    await asyncio.sleep(3600)  # running for one hour.
    # TODO : scheduling restart (crontab ? cli params ?) -> GOAL: ensure resilience (erlang-style)

asyncio.run(main())

