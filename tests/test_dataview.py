import pytest
import datetime
import pandas
import random
from datetime import datetime, timedelta

from bokeh.models import Plot

from livebokeh.datamodel import DataModel
from livebokeh.dataview import DataView


def test_render():
    now = datetime.now()

    # TODO : generate sample data with hypothesis
    df = pandas.DataFrame(
        data=[
            [random.randint(-10, 10), random.randint(-10, 10)],
            [random.randint(-10, 10), random.randint(-10, 10)],
        ],
        columns=["random1", "random2"],
        index=[now, now + timedelta(milliseconds=1)],
    )

    dm = DataModel(name="TestDataModel", data=df)

    dv = DataView(model=dm)

    assert dv.model == dm
    rendered = dv.plot

    assert isinstance(rendered, Plot)

    assert dv.model == dm
    rendered2 = dv.plot

    assert isinstance(rendered, Plot)
    assert rendered2 != rendered
