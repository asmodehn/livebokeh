import datetime

import pandas
import typing

from bokeh.layouts import row
from bokeh.models import ColumnDataSource, DataTable, TableColumn
from bokeh.plotting import Figure


class Clock:

    data: pandas.DataFrame

    _cds: typing.List[ColumnDataSource]

    @property
    def source(self) -> ColumnDataSource:
        cds = ColumnDataSource(self.data)
        self._cds.append(cds)
        return cds

    @staticmethod
    def _clock2df(dt: datetime) -> pandas.DataFrame:
        return pandas.DataFrame(data=[
            [dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second,]
        ], columns=["year", "month", "day", "hour", "minute", "second"]
        )

    def __init__(self):
        dt = datetime.now()
        self.data = self._clock2df(dt)
        self._cds = list()

    async def __call__(self):
        while True:
            await asyncio.sleep(1)
            dt = datetime.now()
            update_df = self._clock2df(dt)

            self.data.append(update_df)

            # attempt datadriven stream to view
            for c in self._cds:
                if c.document is not None:
                    c.document.add_next_tick_callback(
                        # lambda: c.stream(update_df)
                        lambda: c.stream(update_df.reset_index().to_dict('series'))
                    )


if __name__ == '__main__':
    # Minimal server test
    import asyncio
    from datetime import datetime

    from bokeh.server.server import Server as BokehServer

    clock = Clock()

    def test_page(doc):
        # Debug Figure
        debug_fig = Figure(title="Random Test", plot_height=480,
                        tools='pan, xwheel_zoom, reset',
                        toolbar_location="left", y_axis_location="right",
                        sizing_mode="scale_width")

        # dynamic datasource plots as simply as possible
        plot_secs = debug_fig.line(x="index", y="second", color="blue", source=clock.source, legend_label="Seconds")
        plot_mins = debug_fig.line(x="index", y="minute", color="red", source=clock.source, legend_label="Minutes")

        table = DataTable(source=clock.source, columns=[
            TableColumn(field=f, title=f) for f in clock.data.columns
        ], width=320, height=480)

        doc.add_root(
                # to help compare / visually debug
                row(debug_fig, table)
        )

        # quick and easy dynamic update
        # doc.add_periodic_callback(
        #     lambda: (
        #         # replacing data in datasource directly trigger simple dynamic update in plots.
        #         setattr(plot_secs.data_source, 'data', clock.data),
        #         setattr(plot_mins.data_source, 'data', clock.data),
        #         setattr(table.source, 'data', clock.data)
        #     ),
        #     period_milliseconds=1000
        # )


    async def main():

        print(f"Starting Tornado Server...")
        server = BokehServer({'/': test_page})  # iolopp must remain to none, num_procs must be default (1)
        server.start()
        # Note : the bkapp is run for each request to the url...

        # bg clock task...
        asyncio.create_task(clock())

        print('Serving Bokeh application on http://localhost:5006/')

        await asyncio.sleep(3600)  # running for one hour.

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Exiting...")

