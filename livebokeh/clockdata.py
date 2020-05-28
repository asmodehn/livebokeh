from __future__ import annotations

import inspect
import time
from datetime import datetime, timedelta
import pandas
import typing

from bokeh.layouts import column, grid
from bokeh.models import DataTable, DateFormatter, PreText, TableColumn
from bokeh.palettes import viridis
from bokeh.plotting import Figure

from livebokeh.dataview import DataView
from .datamodel import DataModel


class Clock:
    """ A class representing the internal clock of this process as a datamodel.
    Note we have two layers of view here.
    First one is when we __call__() the Clock instance -> we retrieve timepoints.
    Second one is when we decide to display less timepoints than the ones we retrieved...
    """

    model: DataModel  # Note : both delegation or inheritance could work here...

    @staticmethod
    def datetime2dataframe(dt: datetime = datetime.now()):
        return pandas.DataFrame(data=[[dt]], columns=["datetime"],)

    def __init__(self, model: typing.Optional[DataModel] = None):
        self.model = (
            model
            if model is not None
            else DataModel(Clock.datetime2dataframe(), name="Clock")
        )

    def __call__(self, period_secs=None, ttyout=False) -> None:
        # no period: one tick only, no return.
        df = self.datetime2dataframe(datetime.now())
        self.model(
            self.model.data.append(df, ignore_index=True)
        )  # verify_integrity=True))

        if ttyout:  # TODO: proper TUI interface...
            print(f"Clock Ticks:\n{self.model.data}")

    @property
    def dataframe(self):
        return self.model.data

    @property
    def table(
        self,
    ) -> DataTable:  # TODO : Table is actually another kind of DataView...
        """ Simplest model to visually help debug interactive update.
            This does NOT require us to call stream or patch.
        """
        return self.model.view.table

    @property
    def plot(self):
        return self.model.view.plot

    def __getitem__(self, item: typing.List[str]):  # TODO: better typing than str ?
        # Clock, like DataModel, is a container.
        # However we totally rely on DataModel to manage the derivative graph of clock updates

        def dt_component_extractor(dt):
            """ extracts components from a datetime instance"""
            return pandas.Series([getattr(dt.datetime, a) for a in item], index=item)
            # Note : we need to pass a series to retrieve proper columns in dataframe

        extracted = self.model.apply(dt_component_extractor)
        return Clock(
            model=extracted
        )  # TODO CAREFUL : we should return a Unique clock for the same index
        # => keep one related model with multiple sources...


clock = Clock()


def _internal_bokeh(doc, example=None):
    from bokeh.layouts import row
    import asyncio

    async def clock_retrieve(period_secs: float, ttyout=False) -> None:
        import asyncio

        while True:
            await asyncio.sleep(period_secs)
            clock(period_secs=None, ttyout=ttyout)  # calling itself synchronously

    # tick always in background but we retrieve its measurement every second.
    asyncio.get_running_loop().create_task(clock_retrieve(period_secs=1, ttyout=True))
    # TODO : turn off retrieval when document is detached ?

    clocksourceview = inspect.getsource(Clock)
    thissourceview = inspect.getsource(_internal_bokeh)

    doc.add_root(
        grid(
            [
                [PreText(text=clocksourceview), PreText(text=thissourceview)],
                # to help compare / visually debug
                [clock[["minute", "second"]].plot, clock[["minute", "second"]].table],
                # Note : even if we create multiple clock instances here,
                # the model is the same, and propagation will update all datasources...
            ]
        )
    )


if __name__ == "__main__":
    import asyncio

    async def main():
        from livebokeh.monosrv import monosrv

        await monosrv({"/": _internal_bokeh})

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Exiting...")
