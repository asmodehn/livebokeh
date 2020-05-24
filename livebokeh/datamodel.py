"""
Data Driven Bokeh
"""
from __future__ import annotations

from collections import namedtuple

import pandas
import typing
from bokeh.document import Document
from bokeh.layouts import column, row
from bokeh.plotting import Figure
from bokeh.models import ColumnDataSource, GlyphRenderer, Plot, DataTable, PreText, TableColumn, DateFormatter
from bokeh.util.serialization import convert_datetime_array, convert_datetime_type


class DataModel:  # rename ? "LiveFrame"

    _data: pandas.DataFrame

    _rendered_datasources: typing.List[ColumnDataSource]
    # REMINDER : document is a property of bokeh's datasource

    @property
    def columns(self):
        return self._data.columns

    def _stream(self, compared_to: pandas.DataFrame) -> typing.Optional[pandas.DataFrame]:

        # Attempting to discover appends based on index...
        data_index = self._data.index
        appended_index = compared_to.index.difference(data_index)
        # TODO : double check, seems buggy...
        streamable = compared_to.loc[appended_index]

        if streamable.any(axis='columns').any():
            # TODO: log stream detected properly
            if self._debug:
                print(f"Stream update: \n{streamable}")

        return streamable

    def _patch(self, compared_to: pandas.DataFrame) -> typing.Dict[str, list]:

        # Attempting to discover patches based on index...
        data_index = self._data.index
        patched_index = compared_to.index.intersection(data_index)
        # TODO : double check, seems buggy...
        available_patches = compared_to.loc[patched_index]

        # if we have some potential patches
        patches = dict()
        if available_patches.any(axis='columns').any():
            # only patch data differences
            patchfilter = available_patches.isin(self._data)  #TODO : prevent "ValueError: cannot compute isin with a duplicate axis."
            patchable_indices = patchfilter[~patchfilter.all(axis='columns')].index
            patchable = available_patches.loc[patchable_indices]

            if patchable.any(axis='columns').any():
                # TODO: log patch detected properly
                if self._debug:
                    print(f"Patch update: \n{patchable}")

                # Note : seems we need the integer index for patch, not the timestamp integer...
                for col, pseries in patchable.reset_index(drop=True).items():
                    patches[col] = [t for t in pseries.items()]

        return patches

    @property
    def data(self):  # to mark it read-only. use __call__ for update.
        return self._data

    @property
    def source(self):
        src = ColumnDataSource(data=self._data, name=self._name)
        self._rendered_datasources.append(src)
        # TODO : how to prune this list ? shall we ever ?
        # note that _detach_document seems to be called properly by bokeh and datasource's document is set to None.
        return src

    def view(self, callable: typing.Callable[[pandas.DataFrame], pandas.DataFrame]):
        # TODO: a preprocess that always trigger on source creation or patch or stream...
        #  OR between data models ?? like a compute dependency graph ?
        raise NotImplementedError

    @property
    def table(self) -> DataTable:  # TODO : Table is actually another kind of DataView...
        """ Simplest model to visually help debug interactive update.
            This does NOT require us to call stream or patch.
        """

        ds = self.source
        ds.name = f"{self._name}_tableview"
        dt = DataTable(source=ds, columns=[

            TableColumn(field=f, title=f, formatter=DateFormatter(format="%m/%d/%Y %H:%M:%S"))
            if pandas.api.types.is_datetime64_any_dtype(self.data.dtypes[i-1]) else  # CAREFUL with index

            TableColumn(field=f, title=f)
            for i, f in enumerate(ds.column_names)
        ], sortable=False, reorderable=False, index_position=None)
        return dt

    """ class representing one viewplot - potentially rendered in multiple documents """
    def __init__(self, data: pandas.DataFrame, name:str, debug=True):
        self._debug = debug
        self._name = name

        # make sure index values are unique (set semantics, or some operations might fail later on...)
        if not data.index.is_unique:
            raise TypeError(f"{data.index} has to be unique to guarantee proper behavior during later computations."
                            "If in doubt, keep pandas' default index.")

        self._data = data
        self._rendered_datasources = list()
        # a set here is fine, it is never included in the bokeh document

    def __call__(self, new_data: typing.Optional[pandas.DataFrame] = None, name=None, debug = None) -> DataModel:
        """ To schedule optimal push of detected data changes. """

        if name is not None:
            self._name = name
        if debug is not None:
            self._debug = debug

        # make sure index values are unique (set semantics, or some operations might fail later on...)
        if not new_data.index.is_unique:
            raise TypeError(f"{new_data.index} has to be unique to guarantee proper behavior during later computations."
                            "If in doubt, keep pandas' default index.")

        patches = self._patch(new_data)

        if patches:
            for r in self._rendered_datasources:
                if r.document is not None:
                    r.document.add_next_tick_callback(
                        lambda ds=r: ds.patch(patches)
                    )

        streamable = self._stream(new_data)

        if not streamable.empty:
            for r in self._rendered_datasources:
                if r.document is not None:
                    r.document.add_next_tick_callback(
                        lambda ds=r: ds.stream(streamable),
                    )

        # Replace data here and in existing datasources.
        # BUT it will NOT trigger redraw of the various plots, we rely on stream or patch for that.
        self._data = new_data
        # Note this is necessary for further patches to work correctly and not get out of bounds.
        for rds in self._rendered_datasources:
            if rds.document is not None:
                rds.document.add_next_tick_callback(
                    lambda ds=rds: setattr(ds, 'data', new_data)
                )

        return self  # to be able to chain updates.


def _internal_bokeh(doc):
    doc.add_root(
        PreText(text="LiveBokeh DataModel is working !")  # TODO : render THIS source code example ??
    )


if __name__ == '__main__':
    # Minimal server test
    import random
    import asyncio
    from datetime import datetime, timedelta

    from bokeh.server.server import Server as BokehServer

    # Note : This is "created" before a document output is known
    # and before a request is sent to the server
    start = datetime.now()
    ddsource1 = DataModel(name="ddsource1", data=pandas.DataFrame(data=[random.randint(-10, 10), random.randint(-10, 10)], columns=["random1"],
                                                                  index=[start, start+timedelta(milliseconds=1)]),
                          debug=False)
    ddsource2 = DataModel(name="ddsource2", data=pandas.DataFrame(data=[random.randint(-10, 10), random.randint(-10, 10)], columns=["random2"],
                                                                  index=[start, start+timedelta(milliseconds=1)]),
                          debug=False)
    # Note we add some initial data to have bokeh center the plot properly on the time axis TODO : fix it !

    # Producer as a background task
    async def compute_random(m, M):
        tick = ddsource1.data.index.to_list()  # to help with full data generation
        while True:
            now = datetime.now()
            tick.append(now)
            print(now)  # print in console for explicitness

            # push FULL data updates !
            # Note some derivative computation may require more than you think
            ddsource1(pandas.DataFrame(
                columns=["random1"],
                data = {
                    "random1": [
                    random.randint(m, M)
                    for t in range(len(tick))
                ]},
                index = tick
            ))

            # Note : we can trigger only a stream update by calling with the same data + some appended...
            ddsource2(ddsource2.data.append(
                pandas.DataFrame(data = {"random2": [random.randint(m, M)]}, index=[now])
            ))

            await asyncio.sleep(1)

    def test_page(doc):
        # Debug Figure
        debug_fig = Figure(title="Random Test", plot_height=480,
                        tools='pan, xwheel_zoom, reset',
                        toolbar_location="left", y_axis_location="right",
                        x_axis_type='datetime', sizing_mode="scale_width")

        # dynamic datasource plots as simply as possible
        debug_fig.line(x="index", y="random1", color="blue", source=ddsource1.source, legend_label="Patch+Stream")
        debug_fig.line(x="index", y="random2", color="red", source=ddsource2.source, legend_label="Stream")

        doc.add_root(
                # to help compare / visually debug
                row(debug_fig, ddsource1.table, ddsource2.table)
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






