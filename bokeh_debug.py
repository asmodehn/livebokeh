from datetime import datetime

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

    @property
    def table(self):  # a very simple view of the data.
        src = self.source
        # Note : index position is None, as that index (not a column) seems not usable in plots...)
        table = DataTable(
            source=src,
            columns=[TableColumn(field=f, title=f) for f in src.column_names],
            sortable=False,
            reorderable=False,
            index_position=None,
        )  # , width=320, height=480)
        return table

    @staticmethod
    def df_from_datetimes(
        dts: typing.List[datetime], indexes: typing.Optional[typing.List[int]] = None
    ) -> pandas.DataFrame:
        return pandas.DataFrame(
            data=[
                [dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second] for dt in dts
            ],
            columns=["year", "month", "day", "hour", "minute", "second"],
            # this is useful to set a specific index for proper appending later on
            index=indexes if indexes is not None else [i for i in range(len(dts))],
        )

    def __init__(self):
        dt = datetime.now()
        self.data = self.df_from_datetimes([dt])
        self._cds = list()

    def __call__(self, new_data):

        # attempt datadriven stream to view
        for c in self._cds:
            if c.document is not None:
                c.document.add_next_tick_callback(
                    lambda cds=c: cds.stream(new_data)
                    # lambda cds=c: cds.stream(update_df.reset_index().to_dict('series'))
                )


if __name__ == "__main__":
    # Minimal server test
    import asyncio
    from datetime import datetime

    from bokeh.server.server import Server as BokehServer

    clock = Clock()

    async def ticking():
        tick = 0
        while True:
            tick += 1
            await asyncio.sleep(1)
            df_update = clock.df_from_datetimes([datetime.now()], indexes=[tick])
            clock.data.append(df_update, verify_integrity=True)
            clock(df_update)  # this will trigger all the needed callbacks

    def test_page(doc):
        # Debug Figure
        debug_fig = Figure(
            title="Random Test",
            plot_height=480,
            tools="pan, xwheel_zoom, reset",
            toolbar_location="left",
            y_axis_location="right",
            sizing_mode="scale_width",
        )

        # dynamic datasource plots as simply as possible
        plot_secs = debug_fig.line(
            x="index",
            y="second",
            color="blue",
            source=clock.source,
            legend_label="Seconds",
        )
        plot_mins = debug_fig.line(
            x="index",
            y="minute",
            color="red",
            source=clock.source,
            legend_label="Minutes",
        )

        doc.add_root(
            # to help compare / visually debug
            row(debug_fig, clock.table)
        )

    async def main():

        print(f"Starting Tornado Server...")
        server = BokehServer(
            {"/": test_page}
        )  # ioloop must remain to none, num_procs must be default (1)
        server.start()
        # Note : the bkapp is run for each request to the url...

        # bg clock task...
        asyncio.create_task(ticking())

        print("Serving Bokeh application on http://localhost:5006/")

        await asyncio.sleep(3600)  # running for one hour.

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Exiting...")
