

# Note : use bind_sockets to create the sockets
import asyncio
import threading
from datetime import datetime, timedelta
import random

import pandas

from aiokraken.domain.models.figures.bokehliveview import BokehLiveView
from bokeh.server.server import BaseServer, HTTPServer

from bokeh.application import Application
from bokeh.application.handlers import FunctionHandler
from bokeh.layouts import column, row
from bokeh.server.tornado import BokehTornado
from bokeh.server.util import bind_sockets
from tornado.ioloop import IOLoop

from aiokraken.domain.models.figures.ddbokeh import DDSharedDataSource


def bk_server(sockets, host, port, applications, extra_websocket_origins=None, bg_task=None):

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

    # Note : This is "created" before a document output is known
    # and before a request is sent to the server
    start = datetime.now()
    ddsource1 = DDSharedDataSource(name="ddsource1", data=pandas.DataFrame(data=[
            [random.randint(-10, 10), random.randint(-10, 10)],
            [random.randint(-10, 10), random.randint(-10, 10)],
        ],
            columns=["random1", "random2"],
            index=[start, start+timedelta(milliseconds=1)]
    ))

    # Note : This is "created" before a document output is known and before a request is sent to the server
    view = BokehLiveView(source=ddsource1, title="Random Test", plot_height=480,
                    tools='xpan, xwheel_zoom, reset',
                    toolbar_location="left", y_axis_location="right",
                    x_axis_type='datetime', sizing_mode="scale_width")

    # Producer as a background task
    async def compute_random(m, M):
        tick = ddsource1.data.index.to_list()  # to help with full data generation
        while True:
            now = datetime.now()
            tick.append(now)
            print(now)  # print in console for explicitness

            new_data = {
                    "random1": [
                        random.randint(m, M)  # change everything to trigger patch
                        for t in range(len(tick))  # + 1 extra element to stream
                    ],
                    "random2": ddsource1.data["random2"].to_list() + [
                        random.randint(m, M)  # only add one element to stream
                    ]
                }

            # push FULL data updates !
            # Note some derivative computation may require more than you think
            ddsource1.data = pandas.DataFrame(
                columns=["random1", "random2"],
                data = new_data,
                index = tick
            )

            await asyncio.sleep(1)

    def test_page(doc):

        doc.add_root(
            column(
                row(view.figure, ddsource1.table),
            )
        )

    # can't use shortcuts here, since we are passing to low level BokehTornado
    bkapp = Application(FunctionHandler(test_page))

    try:
        bk_server(sockets, host="localhost", port=port, applications={'/bkapp': bkapp}, bg_task=compute_random)
    except KeyboardInterrupt as ke:
        print(f"KeyboardInterrupt: Exiting.")