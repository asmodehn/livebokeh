from datetime import datetime, timedelta

import pytest

import pandas
import random

from bokeh.models import DataSource

from livebokeh.datamodel import DataModel


def test_call():
    now = datetime.now()

    # TODO : generate sample data with hypothesis
    df = pandas.DataFrame(data=[[random.randint(-10, 10), random.randint(-10, 10)], [random.randint(-10, 10), random.randint(-10, 10)]]
                          , columns=["random1", "random2"],
                     index=[now, now + timedelta(milliseconds=1)])

    dm = DataModel(name="TestDataModel", data=df)

    ds = dm()
    assert isinstance(ds, DataSource)
    assert ds.name == "TestDataModel"
    assert ds.column_names == df.reset_index().columns.to_list()
    for c in df.columns:
        assert (ds.data[c] == df[c]).all()


if __name__ == '__main__':
    pytest.main(['-s', __file__])
