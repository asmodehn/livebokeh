import typing

import bokeh
import pandas
from bokeh.layouts import column, row
from bokeh.palettes import viridis

from livebokeh.datamodel import DataModel
from bokeh.models import ColumnDataSource, DataTable, DateFormatter, Model, TableColumn
from bokeh.plotting import Figure


class DataView:

    model: DataModel
    _figure_kwargs: typing.Dict

    # TODO : some plot visualisation in terminal ...

    def __init__(self, model: DataModel, **figure_kwargs):
        self.model = model
        self._figure_kwargs = figure_kwargs

    def __call__(self):
        """ To call everything that needs to be done on request (and not on init)"""

        figure = Figure(**self._figure_kwargs)

        palette = viridis(len(self.model.data.columns))
        color_index = {c: palette[i] for i, c in enumerate(self.model.data.columns)}

        # by default : lines
        for c in self.model.data.columns:
            # TODO : maybe only pass the necessary column of source ? how about updates ??
            figure.line(source=self.model.source, x="index", y=c, color=color_index[c], legend_label=c)

        return figure


if __name__ == '__main__':
    # Minimal server test
    import random
    import asyncio
    from datetime import datetime, timedelta

    from bokeh.server.server import Server as BokehServer

    # Note : This is "created" before a document output is known
    # and before a request is sent to the server
    start = datetime.now()
    ddsource1 = DataModel(name="ddsource1", data=pandas.DataFrame(data=[
            [random.randint(-10, 10), random.randint(-10, 10)],
            [random.randint(-10, 10), random.randint(-10, 10)],
        ],
            columns=["random1", "random2"],
            index=[start, start+timedelta(milliseconds=1)]
    ))

    # Note : This is "created" before a document output is known and before a request is sent to the server
    view = DataView(model=ddsource1, title="Random Test", plot_height=480,
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
            ddsource1(pandas.DataFrame(
                columns=["random1", "random2"],
                data = new_data,
                index = tick
            ))

            await asyncio.sleep(1)

    def test_page(doc):

        doc.add_root(
            column(
                row(view(), ddsource1.table, view()),
            )
        )

    async def main():

        from livebokeh.monosrv import monosrv
        # bg async task...
        asyncio.create_task(compute_random(-10, 10))

        await monosrv({'/': test_page})


    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Exiting...")


