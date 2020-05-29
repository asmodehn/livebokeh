import asyncio
import functools
import inspect
import sys
import typing

import bokeh
import pandas
from bokeh.layouts import column, layout, row
from bokeh.models import (
    BooleanFilter,
    CDSView,
    DataTable,
    DateFormatter,
    GroupFilter,
    PreText,
    TableColumn,
)
from bokeh.palettes import viridis

from livebokeh.datamodel import DataModel
from bokeh.plotting import Figure


class DataView:  # rename ? "LiveFrameView"
    """
    A View of the model, will render a row-subset of the model.
    Note : implicitely a DataView will display *all* columns of the model.
    If this is not what is desired, a DataProcess should be put in place to transform the DataModel.

    """

    # Note : the keys of hte dit match the column of the view DF
    views: pandas.DataFrame

    filter_columns: str
    category: str

    # in a sense, the compute graph of views indexed from this one (one level only)...
    # TODO : needed or cleanup ?
    _related_views: typing.Dict[typing.List[str], functools.partial]

    # TODO : some plot visualisation in terminal ...

    # TODO : Do we need model here, or can we just rely on ColumnDataSource from bokeh ?
    #        BETTER: DataView and DataModel would be usable separately.
    def __init__(
        self,
        model: DataModel,
        filter_column: typing.Optional[str] = None,  # TODO list
        category: typing.Optional[
            str
        ] = None,  # None means all, something means only this one ( groupfilter )  # TODO BETTER !!!
        # Note: filters that require computation on updates should be handled in the DataModel
    ):  # Note : typing.Any represent the type of the row tuple
        self.model = model
        # Note : views here should be "per-line" of data
        # Note : potentially a model is already a view (root of view tree... cf Ahman's Containers...)
        # For a "per-column" view -> model needs to be transformed (via a dataprocess)

        self.filter_column = (
            filter_column if filter_column is not None else self.model.data.index.name
        )
        assert self.filter_column in [self.model.data.index.name, *self.model.columns]

        # => maybe use a special "related" frame in model, only for view and filters TODO...
        self.category = category

        # we always render ALL columns here. otherwise change your datamodel.
        self._table_columns = [
            TableColumn(
                field=f, title=f, formatter=DateFormatter(format="%m/%d/%Y %H:%M:%S")
            )
            if pandas.api.types.is_datetime64_any_dtype(self.model.data.dtypes[i - 1])
            else TableColumn(field=f, title=f)  # CAREFUL with index
            for i, f in enumerate(self.model.columns)
        ]

        # TODO : some clever introspection of model to find most appropriate arguments...
        self._table_args = {
            "sortable": False,
            "reorderable": False,
            "index_position": None,
        }
        self._plot_args = dict()

    def bokeh_view(self, ignore_filters=False):
        """ because we need a new view for each document request...

        ignore filters allow some glyph renderer not supporting filters to ignore them
        rendering everything instead of breaking...
        """
        if ignore_filters or self.category is None:
            view = CDSView(source=self.model.source)  # defaults to complete view.
        else:
            view = CDSView(
                source=self.model.source,
                filters=[
                    GroupFilter(column_name=self.filter_column, group=self.category)
                ],
            )

            # fs = dict()
            # # updating filters (changes may have happened since view creation)
            # # Ref : https://docs.bokeh.org/en/latest/docs/user_guide/data.html#groupfilter
            # for f in self.filter:
            #     self.views[f] = self.filter[f](self.model.data)
            #     fs[f] = [GroupFilter(column_name=f, group=g) for g in self.model.data[f].unique()]
            #
            # # Note: This is related to https://pandas.pydata.org/pandas-docs/stable/user_guide/categorical.html
            #
            # boolfilter = [
            #     self.filter(r) for r in self.model.data.itertuples()
            # ]  # TODO : This should be recreated at  everytick ?!?!
            # # TODO : model iterable on rows ?
            #
            # view = CDSView(
            #     source=self.model.source, filters=
            # )
        return view

    def __getitem__(self, item):
        # return a view with categorical filter applied !
        # TODO: is it necessary to recompute every update, or bokeh already takes care of that ?
        return DataView(
            model=self.model, filter_column=self.filter_column, category=item
        )

    def table_args(self, **datatable_kwargs):
        self._table_args = datatable_kwargs

    @property
    def table(self):
        # Note : index position is None, as that index (not a column) seems not usable in plots...)

        # instantiating view on render
        view = self.bokeh_view()

        return DataTable(
            source=view.source,
            view=view,  # TODO : filter representation ? categories as glyphs ?
            columns=self._table_columns,
            **self._table_args
        )

    def plot_args(self, **figure_kwargs):
        self._plot_args = figure_kwargs

    @property
    def plot(self):
        figure = Figure(**self._plot_args)

        palette = viridis(len(self.model.data.columns))
        color_index = {c: palette[i] for i, c in enumerate(self.model.data.columns)}

        # instantiating view on render (must ignore filters in plot)
        # ERROR:bokeh.core.validation.check:E-1024 (CDSVIEW_FILTERS_WITH_CONNECTED):
        #  CDSView filters are not compatible with glyphs with connected topology such as Line or Patch
        view = self.bokeh_view(ignore_filters=True)

        # by default : lines
        for c in self.model.data.columns:
            # CAREFUL : CDSView used by Glyph renderer must have a source that matches the Glyph renderer's data source
            figure.line(
                source=view.source,
                view=view,  # TODO : filter representation ? categories as glyphs ?
                x="index",
                y=c,
                color=color_index[c],
                legend_label=c,
            )

        return figure

    def histo_args(self, **figure_kwargs):
        self._histo_args = figure_kwargs

    @property
    def histo(self):
        figure = Figure(**self._histo_args)

        palette = viridis(len(self.model.data.columns))
        color_index = {c: palette[i] for i, c in enumerate(self.model.data.columns)}

        # instantiating view on render (must ignore filters in plot)
        # ERROR:bokeh.core.validation.check:E-1024 (CDSVIEW_FILTERS_WITH_CONNECTED):
        #  CDSView filters are not compatible with glyphs with connected topology such as Line or Patch
        view = self.bokeh_view()  # ignore_filters=True)

        # by default : lines
        for c in self.model.data.columns:
            # CAREFUL : CDSView used by Glyph renderer must have a source that matches the Glyph renderer's data source
            figure.vbar(
                source=view.source,
                view=view,  # TODO : filter representation ? categories as glyphs ?
                width=view.source.data["index"][1] - view.source.data["index"][0],
                x="index",
                bottom=0,
                top=c,
                color=color_index[c],
                legend_label=c,
            )

        return figure

    # TODO : more plots


async def _internal_example():
    # Minimal server test
    import random
    import asyncio
    from datetime import datetime, timedelta

    # Note : This is "created" before a document output is known
    # and before a request is sent to the server
    start = datetime.now()
    random_data_model = DataModel(
        name="random_data_model",
        data=pandas.DataFrame(
            data=[
                [random.randint(-10, 10), random.randint(-10, 10)],
                [random.randint(-10, 10), random.randint(-10, 10)],
            ],
            columns=["random1", "random2"],
            index=[start, start + timedelta(milliseconds=1)],
        ),
    )

    # we setup a filter based on random2 (only appending, to minimize confusion during plot observation...)
    filtered_model = random_data_model.relate(
        lambda df: pandas.cut(
            df["random2"], bins=[-10, 0, 10], include_lowest=True, labels=["neg", "pos"]
        ),
        column="filter",
    )

    # Note : This is "created" before a document output is known and before a request is sent to the server
    view = DataView(model=random_data_model)
    view.plot_args(
        title="Random Test",
        plot_height=480,
        tools="xpan, xwheel_zoom, reset",
        toolbar_location="left",
        y_axis_location="right",
        x_axis_type="datetime",
        sizing_mode="scale_width",
    )

    # filtering view on the left
    fview = DataView(model=filtered_model, filter_column="filter", category="pos",)
    fview.histo_args(
        title="Random Test",
        plot_height=480,
        tools="xpan, xwheel_zoom, reset",
        toolbar_location="left",
        y_axis_location="right",
        x_axis_type="datetime",
        sizing_mode="scale_width",
    )

    # Producer as a background task
    async def compute_random(m, M):
        tick = (
            random_data_model.data.index.to_list()
        )  # to help with full data generation
        while True:
            now = datetime.now()
            tick.append(now)
            print(now)  # print in console for explicitness

            new_data = {
                "random1": [
                    random.randint(m, M)  # change everything to trigger patch
                    for t in range(len(tick))  # + 1 extra element to stream
                ],
                "random2": random_data_model.data["random2"].to_list()
                + [random.randint(m, M)],  # only add one element to stream
            }

            # push FULL data updates !
            # Note some derivative computation may require more than you think
            random_data_model(
                pandas.DataFrame(
                    columns=["random1", "random2"], data=new_data, index=tick
                )
            )

            await asyncio.sleep(1)

    # bg async task...
    asyncio.create_task(compute_random(-10, 10))

    return random_data_model, view, fview


def _internal_bokeh(doc, example=None):

    # Note: if launched by package, the result of _internal_example is passed via kwargs
    random_data_model = example[0]
    view = example[1]
    fview = example[2]

    moduleview = inspect.getsource(sys.modules[__name__])

    doc.add_root(
        layout(
            [
                [  # we want one row with two columns
                    [PreText(text=moduleview)],
                    [fview.histo, random_data_model.view.table, view.plot],
                ]
            ]
        )
    )


if __name__ == "__main__":

    async def main():

        from livebokeh.monosrv import monosrv

        # initializing example
        example = await _internal_example()

        await monosrv({"/": functools.partial(_internal_bokeh, example=example)})

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Exiting...")
