import pandas
import pytest

from livebokeh.LiveSeries import LiveSeries, liveseries


def test_create_none():

    ls = liveseries()

    assert isinstance(ls, LiveSeries)
    assert len(ls) == 0

    ls([3])  # update

    assert isinstance(ls, LiveSeries)
    assert len(ls) == 1

    ls([3, 4, 5])  # update

    assert isinstance(ls, LiveSeries)
    assert len(ls) == 3


def test_create_int():

    ls = liveseries([42])

    assert isinstance(ls, LiveSeries)
    assert len(ls) == 1
    assert ls.dtype == int  # deducted dtype

    ls([51])

    assert isinstance(ls, LiveSeries)
    assert len(ls) == 1
    assert ls.dtype == int  # deducted dtype

    ls([51, 63])

    assert isinstance(ls, LiveSeries)
    assert len(ls) == 2
    assert ls.dtype == int  # deducted dtype


def test_create_iterable():
    ls = liveseries([2, 4, 6])

    assert isinstance(ls, LiveSeries)
    assert len(ls) == 3
    assert ls.dtype == int  # deducted dtype

    ls = liveseries([3, 5, 7, 9])

    assert isinstance(ls, LiveSeries)
    assert len(ls) == 4
    assert ls.dtype == int  # deducted dtype


def test_map():

    ls = liveseries([1, 2, 3])

    # testing mapping a function element wise
    ls2 = ls.map(lambda n: n + 10)
    assert ls2 == liveseries([11, 12, 13])

    ls([4, 5, 6, 7])  # update

    # update should have been propagated to other nodes
    assert ls2 == liveseries([14, 15, 16, 17])


def test_apply():

    ls = liveseries([1, 2, 3])

    # testing applying a function on the whole series
    ls2 = ls.apply(pandas.cut, bins=3, labels=["one", "two", "three"])
    assert ls2 == liveseries(["one", "two", "three"])

    ls([4, 5, 6, 7])  # update

    # update should have been propagated to other nodes
    assert ls2 == liveseries(["one", "one", "two", "three"])


if __name__ == "__main__":
    pytest.main(["-s", __file__])
