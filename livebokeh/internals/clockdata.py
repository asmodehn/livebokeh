import time
from datetime import datetime, timedelta
import pandas
import typing


class Clock:
    """ A class representing the internal clock of this process as a dataframe.
    """

    dataframe: pandas.DataFrame

    @staticmethod
    def datetime2dataframe(dt: datetime):
        return pandas.DataFrame(data=[
            [dt]
        ], columns=["datetime"],
        )

    def __init__(self,):
        self.started = datetime.now()

        self.dataframe=self.datetime2dataframe(self.started)

    def __call__(self):
        df = self.datetime2dataframe(datetime.now())
        self.dataframe = self.dataframe.append(df, ignore_index=True)  # verify_integrity=True)

    def expanded_dataframe(self, year=True, month=True, day=True, hour=True, minute=True, second=True):
        columns = []
        if year:
            columns.append("year")
        if month:
            columns.append("month")
        if day:
            columns.append("day")
        if hour:
            columns.append("hour")
        if minute:
            columns.append("minute")
        if second:
            columns.append("second")
        return pandas.DataFrame(data=[
            [getattr(dt["datetime"], a) for a in columns] for idx, dt in self.dataframe.iterrows()
        ],
            columns=columns
        )


if __name__ == '__main__':

    c = Clock()
    print(c.dataframe)

    time.sleep(1)
    c()

    print(c.expanded_dataframe())
    time.sleep(2)
    c()

    print(c.dataframe)
