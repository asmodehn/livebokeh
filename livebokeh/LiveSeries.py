from __future__ import annotations

import functools
import inspect

import pandas
import numpy
import typing


ListType = typing.Union[
    typing.Iterable[typing.Any], pandas.Series, numpy.ndarray,
]


class LiveSeries:  # Live Element ? series is a list on the time dimension...

    series: pandas.Series  # Note Series is a list (not a structured type, like record)

    edges: typing.Dict[code, functools.partial]

    def __repr__(self):
        return repr(self.series.tolist())  # represent a list

    def __init__(self, series: ListType):
        self.series = series
        self.edges = dict()

    def __eq__(self, other):
        return (self.series == other.series).all()

    def __len__(self):
        return len(self.series)

    @property
    def dtype(self):
        return self.series.dtype

    def map(self, func, args=(), **kwds):
        """
        element-wise apply, ie. Lifting then apply.
        :param func:
        :param args:
        :param kwds:
        :return:
        """

        f = functools.partial(func, *args, **kwds)

        if f in self.edges:
            return self.edges[f]()  # triggering the partial function for update.
        else:  # the first time

            new_series = self.series.map(f)
            return_ls = LiveSeries(series=new_series)

            def wrapped(model_out: LiveSeries):
                new_series = self.series.map(f)
                # already exists, we just modify its data
                model_out(new_series=new_series)
                return model_out

            self.edges[f] = functools.partial(wrapped, return_ls)

            return return_ls

    def apply(self, func, args=(), **kwds):
        """
         whole-series apply.
        :param func:
        :param args:
        :param kwds:
        :return:
        """

        f = functools.partial(func, *args, **kwds)

        if f in self.edges:
            return self.edges[f]()  # triggering the partial function for update.
        else:  # the first time

            new_series = f(self.series)
            return_ls = LiveSeries(series=new_series)

            def wrapped(model_out: LiveSeries):
                new_series = f(self.series)
                # already exists, we just modify its data
                model_out(new_series=new_series)
                return model_out

            self.edges[f] = functools.partial(wrapped, return_ls)

            return return_ls

    def __call__(self, new_series: ListType):
        """ update """
        if not isinstance(new_series, pandas.Series):
            new_series = pandas.Series(new_series)

        self.series = new_series

        for f, s in self.edges.items():
            self.edges[f]()

        return self  # To chain updates

    def __getitem__(self, item):
        """ retrieve an element - if int - or retrieve a sub series - if slice - """
        return self.series[item]


def liveseries(elems: typing.Optional[ListType] = None):

    s = pandas.Series(data=elems)

    return LiveSeries(series=s)


if __name__ == "__main__":

    nums = liveseries([0, 1, 2, 3, 4, 5, 6, 7])
    print(nums)

    # map example
    nums_from_1 = nums.map(func=lambda e: e + 1)
    print(nums_from_1)

    # apply example (leveraging a map on the pandas series to compute the result series
    odds_even = nums.apply(
        func=lambda s: s.map(lambda e: "even" if e % 2 == 0 else "odd")
    )
    print(odds_even)

    # update example
    nums(pandas.Series([0, 1, 2, 3, 2, 1, 2, 3, 4, 5]))

    print("\nAfter update:")
    print(nums)
    print(nums_from_1)
    print(odds_even)

    # yet another update example
    nums(pandas.Series([9, 8, 7, 6, 5, 6, 7, 8, 9, 1, 2, 3]))

    print("\nAfter yet another update:")
    print(nums)
    print(nums_from_1)
    print(odds_even)
