import asyncio
import inspect
import typing

import pandas
from bokeh.layouts import layout
from bokeh.models import PreText

from stochastic.discrete import RandomWalk

from livebokeh.datamodel import DataModel


class LiveRandomWalk:

    model: DataModel
    pregen_frame: pandas.DataFrame
    sample_size: int

    def __init__(self, sample_size=255):
        self.sample_size = sample_size

        self.pregen_frame = pandas.DataFrame(
            data={"RandomWalk": RandomWalk().sample(sample_size)}
        )
        self.current = 0

        self.model = DataModel(
            data=self.pregen_frame[: self.current], name="Random Walk", debug=False
        )

    @property
    def plot(self):
        return self.model.view.plot

    @property
    def table(self):
        return self.model.view.table

    def __call__(self):
        self.current += 1
        if self.current < len(self.pregen_frame):
            # simulate dynamic process by making more precomputed data appear...
            self.model(new_data=self.pregen_frame[: self.current])
        else:
            # resetting data
            self.pregen_frame = pandas.DataFrame(
                data={"RandomWalk": RandomWalk().sample(self.sample_size)}
            )
            self.current = 0
            self.model(new_data=self.pregen_frame[: self.current])


ld = LiveRandomWalk()


async def simulate():
    while True:  # looping over each step
        ld()
        await asyncio.sleep(0.2)


def livebokeh(doc):

    doc.add_root(
        layout(
            [
                [PreText(text=inspect.getsource(LiveRandomWalk)), ld.plot],
                # to help compare / visually debug
                [
                    [
                        PreText(text=inspect.getsource(simulate)),
                        PreText(text=inspect.getsource(livebokeh)),
                    ],
                    ld.table,
                ],
            ]
        )
    )


if __name__ == "__main__":
    import asyncio

    async def main():
        from livebokeh.monosrv import monosrv

        asyncio.create_task(simulate())

        await monosrv({"/": livebokeh})

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Exiting...")
