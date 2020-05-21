from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest

import pandas
import random

from bokeh.models import DataSource

from livebokeh.datamodel import DataModel


def test_intindexed_data():
    # TODO : generate sample data with hypothesis
    df = pandas.DataFrame(
        data=[[random.randint(-10, 10), random.randint(-10, 10)], [random.randint(-10, 10), random.randint(-10, 10)]]
        , columns=["random1", "random2"],
        index=[0, 1])

    dm = DataModel(name="TestDataModel", data=df)

    dd = dm.data
    assert isinstance(dd, pandas.DataFrame)
    assert (dd == df).all().all()
    # Note : this could be more complex if we want to enforce some specific data structure in model...


def test_datetimeindexed_data():
    now = datetime.now()

    # TODO : generate sample data with hypothesis
    df = pandas.DataFrame(data=[[random.randint(-10, 10), random.randint(-10, 10)], [random.randint(-10, 10), random.randint(-10, 10)]]
                          , columns=["random1", "random2"],
                     index=[now, now + timedelta(milliseconds=1)])

    dm = DataModel(name="TestDataModel", data=df)
    dd = dm.data
    assert isinstance(dd, pandas.DataFrame)
    assert (dd == df).all().all()
    # Note : this could be more complex if we want to enforce some specific data structure in model...


def test_source():
    now = datetime.now()

    # TODO : generate sample data with hypothesis
    df = pandas.DataFrame(data=[[random.randint(-10, 10), random.randint(-10, 10)], [random.randint(-10, 10), random.randint(-10, 10)]]
                          , columns=["random1", "random2"],
                     index=[now, now + timedelta(milliseconds=1)])

    dm = DataModel(name="TestDataModel", data=df)

    ds = dm.source
    assert isinstance(ds, DataSource)
    assert ds.name == "TestDataModel"
    assert ds.column_names == df.reset_index().columns.to_list()
    for c in df.columns:
        assert (ds.data[c] == df[c]).all()

    assert ds in dm._rendered_datasources


def test_data_patch():
    now = datetime.now()

    # TODO : generate sample data with hypothesis
    df = pandas.DataFrame(
        data=[[random.randint(-10, 10), random.randint(-10, 10)], [random.randint(-10, 10), random.randint(-10, 10)]]
        , columns=["random1", "random2"],
        index=[now, now + timedelta(milliseconds=1)])

    dm = DataModel(name="TestDataModel", data=df)

    no_patches = dm._patch(df)
    assert len(no_patches) == 0

    # same index, different values
    df2 = pandas.DataFrame(
        data=[[random.randint(-20, -10), random.randint(-20, -10)], [random.randint(-20, -10), random.randint(-20, -10)]]
        , columns=["random1", "random2"],
        index=[now, now + timedelta(milliseconds=1)])

    patches = dm._patch(df2)
    # Note : we need to drop timestamp index and retrieve the integer index
    assert patches == {col: [ (i,v) for i,v in s.items()]
                       for col, s in df2.reset_index(drop=True).to_dict('series').items()}

    # TODO : more fine grained tests...


def test_data_stream():
    now = datetime.now()

    # TODO : generate sample data with hypothesis
    df = pandas.DataFrame(
        data=[[random.randint(-10, 10), random.randint(-10, 10)], [random.randint(-10, 10), random.randint(-10, 10)]]
        , columns=["random1", "random2"],
        index=[now, now + timedelta(milliseconds=1)])

    dm = DataModel(name="TestDataModel", data=df)

    no_streamable = dm._stream(df)
    assert no_streamable.empty

    # different indexes
    df2 = pandas.DataFrame(
        data=[[random.randint(-10, 10), random.randint(-10, 10)], [random.randint(-10, 10), random.randint(-10, 10)]]
        , columns=["random1", "random2"],
        index=[now+timedelta(milliseconds=2), now + timedelta(milliseconds=3)])

    streamable = dm._stream(df2)
    assert (streamable == df2).all().all()


if __name__ == '__main__':
    pytest.main(['-s', __file__])
