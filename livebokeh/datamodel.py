"""
Data Driven Bokeh
"""
from __future__ import annotations

import functools
import inspect
import sys
from collections import namedtuple

import pandas
import typing
from bokeh.document import Document
from bokeh.layouts import column, grid, layout, row
from bokeh.plotting import Figure
from bokeh.models import (
    ColumnDataSource,
    GlyphRenderer,
    Plot,
    DataTable,
    PreText,
    TableColumn,
    DateFormatter,
)
from bokeh.util.serialization import convert_datetime_array, convert_datetime_type


class DataModel:  # rename ? "LiveFrame"
    # TODO : leverage github.com/asmodehn/framable package to implement some way of "processing datamodel into another"
    #        GOAL : a compute network fo dataframes would allows to implement "functions" between dataframes, as usual code...
    _data: pandas.DataFrame

    _rendered_datasources: typing.List[ColumnDataSource]
    # REMINDER : document is a property of bokeh's datasource

    # in a sense, the compute graph of models indexed from this one (one level only)...
    _related_models: typing.Dict[typing.List[str], functools.partial]

    @property
    def columns(self):
        return self._data.columns

    def _stream(
        self, compared_to: pandas.DataFrame
    ) -> typing.Optional[pandas.DataFrame]:

        # Attempting to discover appends based on index...
        data_index = self._data.index
        appended_index = compared_to.index.difference(data_index)
        # TODO : double check, seems buggy...
        streamable = compared_to.loc[appended_index]

        if streamable.any(axis="columns").any():
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
        if available_patches.any(axis="columns").any():
            # only patch data differences
            patchfilter = available_patches.isin(
                self._data
            )  # TODO : prevent "ValueError: cannot compute isin with a duplicate axis."
            patchable_indices = patchfilter[~patchfilter.all(axis="columns")].index
            patchable = available_patches.loc[patchable_indices]

            if patchable.any(axis="columns").any():
                # TODO: log patch detected properly
                if self._debug:
                    print(f"Patch update: \n{patchable}")

                # Note : seems we need the integer index for patch, not the timestamp integer...
                for col, pseries in patchable.reset_index(drop=True).items():
                    patches[col] = [t for t in pseries.items()]

        # asserting the quality of patches (to have a chance to break early and debug)
        # for c, p in patches.items():
        #     assert c in self._data.columns, f"{c} not in {self._data.columns}"
        #     for pe in p:
        #         assert 0 <= pe[0] < len(self._data[c]), f"Patch index should be {0} <= {pe[0]} < {len (self._data[c])}"

        # TODO: investigate exception on document load : ValueError: Out-of bounds index (3) in patch for column: random1
        # PRobably teh patch is computed against data, but the document data is not uptodate ?

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

    @property
    def view(self):
        from livebokeh.dataview import DataView

        return DataView(
            model=self,
        )  # DataModel doesnt need to keep track of views, bokeh does this already.

    """ class representing one viewplot - potentially rendered in multiple documents """

    def __init__(self, data: pandas.DataFrame, name: str, debug=True, optimized=False):
        self._debug = debug
        self._name = name
        self.optimized = optimized  # to do patch and stream on data update

        # make sure index values are unique (set semantics, or some operations might fail later on...)
        if not data.index.is_unique:
            raise TypeError(
                f"{data.index} has to be unique to guarantee proper behavior during later computations."
                "If in doubt, keep pandas' default index."
            )

        self._data = data
        self._rendered_datasources = list()
        # a set here is fine, it is never included in the bokeh document

        self._related_models = dict()

    # like apply, but no lifting...
    def relate(
        self,
        table_fun: typing.Callable[[pandas.DataFrame], pandas.DataFrame],
        column=None,
    ):

        # CAREFUL : we need to guarantee unicity (=> pure elem function!) here, to keep compute graph tractable:
        funstuff = {n: v for n, v in inspect.getmembers(table_fun)}
        if funstuff["__code__"] in self._related_models:
            # we use hte bytecode hash as unique identifier for the function
            return self._related_models[
                funstuff["__code__"]
            ]()  # CAREFUL with datasources and views

        sig = inspect.signature(table_fun)

        def wrapped(model_in: DataModel, model_out: typing.Optional[DataModel] = None):
            if column:  # result integrated as a column
                new_data = model_in.data
                new_data[column] = table_fun(model_in.data)
            else:  # completely different model...
                new_data = table_fun(model_in.data)

            if model_out is not None:  # already exists, we just modify its data
                model_out(new_data=new_data)
            else:  # the first time
                model_out = DataModel(
                    data=new_data,
                    name=f"{model_in._name} {sig.return_annotation}",  # TODO : refine naming types/models here...
                    debug=True,
                )

                # CAREFUL: we store the runnable/updatable relation !
                model_in._related_models[funstuff["__code__"]] = functools.partial(
                    wrapped, model_in=model_in, model_out=model_out
                )
            return model_out

        # the lifted function will immediately get model_in=self, since we are using this instance's list()...
        return wrapped(model_in=self)

    # TODO : cleaner API. This is one of apply|map|applymap of pandas. we should probably stay close to their API...
    def apply(self, elem_fun: typing.Callable[[typing.Any], typing.Any]):
        # some sort of fmap implementation, keeping track of compute relations, enabling updates.

        # CAREFUL : we need to guarantee unicity (=> pure elem function!) here, to keep compute graph tractable:
        funstuff = {n: v for n, v in inspect.getmembers(elem_fun)}
        if funstuff["__code__"] in self._related_models:
            # we use hte bytecode hash as unique identifier for the function
            return self._related_models[
                funstuff["__code__"]
            ]()  # CAREFUL with datasources and views

        sig = inspect.signature(elem_fun)

        # TODO : we also need to do that without the lifting part... just store a process on dataframes...
        def wrapped(model_in: DataModel, model_out: typing.Optional[DataModel] = None):
            new_data = model_in.data.apply(
                elem_fun, axis="columns", result_type="expand"
            )
            if model_out is not None:  # already exists, we just modify its data
                model_out(new_data=new_data)
            else:  # the first time
                model_out = DataModel(
                    data=new_data,
                    name=f"{model_in._name} {sig.return_annotation}",  # TODO : refine naming types/models here...
                    debug=True,
                )

                # CAREFUL: we store the runnable/updatable relation !
                model_in._related_models[funstuff["__code__"]] = functools.partial(
                    wrapped, model_in=model_in, model_out=model_out
                )
            return model_out

        # the lifted function will immediately get model_in=self, since we are using this instance's list()...
        return wrapped(model_in=self)

    def __getitem__(self, item: typing.List[str]):  # TODO: better typing than str ?
        #  indexing by columns (operation on types), comparable to type indexed families, see Martin-Loef Type Theory
        #  somewhat dual to DataView indexing by rows / elements (operation on values)

        if isinstance(
            item, list
        ):  # making explicit only one possible case in python...

            # CAREFUL : we should guarantee unicity here, because of compute storage:
            if item in self._related_models:
                return self._related_models[
                    item
                ]()  # CAREFUL with datasources and views

            # Note : __getitem__ is already lifted by pandas,
            # and we don't want to slow it down (by going down to rows and back)
            #  => double implementation of DataModel.lift() method, this being a special case...
            def wrapped(
                model_in: DataModel, model_out: typing.Optional[DataModel] = None
            ):
                subdata = model_in.data[
                    item
                ]  # __getitem__ is already lifted by pandas itself...
                if model_out is not None:  # This is an update
                    model_out(new_data=subdata)
                else:
                    model_out = DataModel(
                        data=subdata,
                        # Notice item always will be a subset of the current columns list (potentially the current index...)
                        name=f"{model_in._name.split('[')[0]}[{item}]",  # TODO : refine naming types/models here...
                        debug=True,
                    )

                    # we store the (runnable/updatable) relation
                    model_in._related_models[item] = functools.partial(
                        wrapped, model_in=model_in, model_out=model_out
                    )
                return model_out

            # cf Ahman's containers for theoretical background here: https://danel.ahman.ee/papers/msfp16.pdf
            return wrapped(self)
        elif isinstance(item, str):  # just accessing a column data...
            raise NotImplementedError
        else:  # TODO maybe
            raise NotImplementedError

    def __call__(
        self, new_data: typing.Optional[pandas.DataFrame] = None, name=None, debug=None
    ) -> DataModel:
        """ To schedule optimal push of detected data changes. """

        if name is not None:
            self._name = name
        if debug is not None:
            self._debug = debug

        # make sure index values are unique (set semantics, or some operations might fail later on...)
        if not new_data.index.is_unique:
            raise TypeError(
                f"{new_data.index} has to be unique to guarantee proper behavior during later computations."
                "If in doubt, keep pandas' default index."
            )

        # We compute related models (some may be columns in this frame...)
        for code, runnable in self._related_models.items():
            print(f"propagating update for {code}")
            runnable()  # already contains domain and codomain - with new data-, here we just press the trigger.

        # Note : This is an optimization and should not be necessary
        if self.optimized:
            # send updates to render
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
        self._data = new_data

        # Note this is necessary for updates and for further patches to not get out of bounds.
        for rds in self._rendered_datasources:
            if rds.document is not None:
                rds.document.add_next_tick_callback(
                    lambda ds=rds: setattr(ds, "data", new_data)
                )

        return self  # to be able to chain updates.


async def _internal_example():  # async because we need to schedule tasks in background...
    # Minimal server test
    import random
    import asyncio
    from datetime import datetime, timedelta

    # Note : This is "created" before a document output is known
    # and before a request is sent to the server
    start = datetime.now()
    ddmodel1 = DataModel(
        name="ddsource1",
        data=pandas.DataFrame(
            data=[random.randint(-10, 10), random.randint(-10, 10)],
            columns=["random1"],
            index=[start, start + timedelta(milliseconds=1)],
        ),
        debug=False,
    )
    ddmodel2 = DataModel(
        name="ddsource2",
        data=pandas.DataFrame(
            data=[random.randint(-10, 10), random.randint(-10, 10)],
            columns=["random2"],
            index=[start, start + timedelta(milliseconds=1)],
        ),
        debug=False,
    )

    # Note we add some initial data to have bokeh center the plot properly on the time axis TODO : fix it !

    # Producer as a background task
    async def compute_random(m, M):
        tick = ddmodel1.data.index.to_list()  # to help with full data generation
        while True:
            now = datetime.now()
            tick.append(now)
            print(now)  # print in console for explicitness

            # push FULL data updates !
            # Note some derivative computation may require more than you think
            ddmodel1(
                pandas.DataFrame(
                    columns=["random1"],
                    data={"random1": [random.randint(m, M) for t in range(len(tick))]},
                    index=tick,
                )
            )

            # Note : we can trigger only a stream update by calling with the same data + some appended...
            ddmodel2(
                ddmodel2.data.append(
                    pandas.DataFrame(
                        data={"random2": [random.randint(m, M)]}, index=[now]
                    )
                )
            )

            await asyncio.sleep(1)

    # scheduling bg async task... will start with the server (not the client request)
    asyncio.get_running_loop().create_task(compute_random(-10, 10))

    # we return the functions here so they are usable by the main code.
    # Note : the separation of these function is important to illustrate the different calls (startup /vs/ client request)
    return ddmodel1, ddmodel2


def _internal_bokeh(doc, example=None):
    moduleview = inspect.getsource(sys.modules[__name__])

    # Note: if launched by package, the result of _internal_example is passed via kwargs
    ddmodel1 = example[0]
    ddmodel2 = example[1]

    # Debug Bokeh Figure
    debug_fig = Figure(
        title="Random Test",
        plot_height=480,
        tools="pan, xwheel_zoom, reset",
        toolbar_location="left",
        y_axis_location="right",
        x_axis_type="datetime",
        sizing_mode="scale_width",
    )
    # TODO : use DataView.plot instead...
    # dynamic datasource plots as simply as possible
    debug_fig.line(
        x="index",
        y="random1",
        color="blue",
        source=ddmodel1.source,
        legend_label="Patch+Stream",
    )
    debug_fig.line(
        x="index",
        y="random2",
        color="red",
        source=ddmodel2.source,
        legend_label="Stream",
    )

    doc.add_root(
        layout(
            [
                [  # we want one row with two columns
                    [PreText(text=moduleview)],  # TODO : niceties like pygments ??
                    [
                        # to help compare / visually debug
                        debug_fig,
                        ddmodel1.view.table,
                        ddmodel2.view.table,
                    ],
                ]
            ],
        )
    )


if __name__ == "__main__":
    import asyncio

    async def main():
        from livebokeh.monosrv import monosrv

        # initializing example
        example = await _internal_example()

        await monosrv({"/": functools.partial(_internal_bokeh, example=example)})

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Exiting...")
