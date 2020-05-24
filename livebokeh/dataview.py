import typing

import bokeh
import pandas
from bokeh.layouts import column, row
from bokeh.models import BooleanFilter, CDSView, DataTable, DateFormatter, TableColumn
from bokeh.palettes import viridis

from livebokeh.datamodel import DataModel
from bokeh.plotting import Figure


class DataView:  # rename ? "LiveFrameView"
    """
    A View of the model, will render a row-subset of the model.
    Note : implicitely a DataView will display *all* columns of the model.
    If this is not what is desired, a DataProcess should be put in place to transform the DataModel.

    """
    model: DataModel
    # Note : in bokeh, a view already contain the source.
    # We should probably do the dual in here, have the views stored in the model...

    # TODO : some plot visualisation in terminal ...

    def __init__(self, model: DataModel, filter: typing.Optional[typing.Callable[[typing.Any], bool]] = None):  # Note : typing.Any represent the type of the row tuple
        self.model = model
        # Note : views here should be "per-line" of data
        # Note : potentially a model is already a view (root of view tree... cf Ahman's Containers...)
        # For a "per-column" view -> model needs to be transformed (via a dataprocess)

        if filter is None:
            self.view = CDSView(source=self.model.source)  # defaults to complete view.
        else:
            boolfilter=[filter(r) for r in self.model.data.itertuples()]
            # TODO : model iterable on rows ?

            self.view = CDSView(source=self.model.source, filters=[
                BooleanFilter(boolfilter)
            ])

        # we always render ALL columns here. otherwise change your datamodel.
        self._table_columns = [

            TableColumn(field=f, title=f, formatter=DateFormatter(format="%m/%d/%Y %H:%M:%S"))
            if pandas.api.types.is_datetime64_any_dtype(self.model.data.dtypes[i - 1]) else  # CAREFUL with index

            TableColumn(field=f, title=f) for i, f in enumerate(self.model.columns)

        ]

        # TODO : some clever introspection of model to find most appropriate arguments...
        self._table_args = {'sortable':False, 'reorderable':False, 'index_position':None}
        self._plot_args = dict()

    def table_args(self, **datatable_kwargs):
        self._table_args = datatable_kwargs

    @property
    def table(self):
        # Note : index position is None, as that index (not a column) seems not usable in plots...)

        return DataTable(source=self.model.source, view=self.view, columns=self._table_columns,
                          **self._table_args)

    def plot_args(self, **figure_kwargs):
        self._plot_args = figure_kwargs

    @property
    def plot(self):
        figure = Figure(**self._plot_args)

        palette = viridis(len(self.model.data.columns))
        color_index = {c: palette[i] for i, c in enumerate(self.model.data.columns)}

        # by default : lines
        for c in self.model.data.columns:
            figure.line(source=self.model.source, view=self.view, x="index", y=c, color=color_index[c], legend_label=c)

        return figure

    # TODO : more plots

    def __getitem__(self, item: typing.Callable[[typing.Any], bool]): # TODO : support various ways of indexing...
        # cf Ahman's containers for theoretical background here: https://danel.ahman.ee/papers/msfp16.pdf
        return DataView(model=self.model, filter=item)


if __name__ == '__main__':
    # Minimal server test
    import random
    import asyncio
    from datetime import datetime, timedelta

    # Note : This is "created" before a document output is known
    # and before a request is sent to the server
    start = datetime.now()
    random_data_model = DataModel(name="random_data_model", data=pandas.DataFrame(data=[
            [random.randint(-10, 10), random.randint(-10, 10)],
            [random.randint(-10, 10), random.randint(-10, 10)],
        ],
            columns=["random1", "random2"],
            index=[start, start+timedelta(milliseconds=1)]
    ))

    # Note : This is "created" before a document output is known and before a request is sent to the server
    view = DataView(model=random_data_model)
    view.plot_args(title="Random Test", plot_height=480,
                   tools='xpan, xwheel_zoom, reset',
                   toolbar_location="left", y_axis_location="right",
                   x_axis_type='datetime', sizing_mode="scale_width")
    # filtering view on the left
    fview = view[
        # only show when random2 values are positive
        lambda dt: dt.random2 > 0  # TODO : convert to pandas syntax... somehow
        # here (dimension -1) we are talking about attributes, not columns...
    ]

    # Producer as a background task
    async def compute_random(m, M):
        tick = random_data_model.data.index.to_list()  # to help with full data generation
        while True:
            now = datetime.now()
            tick.append(now)
            print(now)  # print in console for explicitness

            new_data = {
                    "random1": [
                        random.randint(m, M)  # change everything to trigger patch
                        for t in range(len(tick))  # + 1 extra element to stream
                    ],
                    "random2": random_data_model.data["random2"].to_list() + [
                        random.randint(m, M)  # only add one element to stream
                    ]
                }

            # push FULL data updates !
            # Note some derivative computation may require more than you think
            random_data_model(pandas.DataFrame(
                columns=["random1", "random2"],
                data = new_data,
                index = tick
            ))

            await asyncio.sleep(1)

    def test_page(doc):
        doc.add_root(
            column(
                row(fview.plot, random_data_model.table, view.plot),
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


