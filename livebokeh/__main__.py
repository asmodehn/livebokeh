# TODO : run livebokeh on itself : provide a visualization of package content...

import asyncio
import functools

from livebokeh import monosrv, datamodel, clockdata, dataview


async def main():
    # Note we need an async main here to ensure a loop is currently running.

    # retrieving all modules
    modls = [monosrv, datamodel, dataview, clockdata]

    # launching all examples, BEFORE web request
    # to make sure livebokeh code does NOT depend on doc being there

    examples = {
        # We need to start all examples in background
        # => async (this is scheduled in the current server loop)
        m.__name__: await m._internal_example()  # we do need to get a first result from the exemple to certify startup.
        for m in modls
        if hasattr(m, "_internal_example")
    }

    livebokeh_mains = {
        m.__name__: functools.partial(
            m._internal_bokeh, example=examples.get(m.__name__)
        )
        for m in modls
        if hasattr(m, "_internal_bokeh")
    }

    # retrieving livebokeh entry points for each module, with optional example
    apps = {"/" + m.__name__: livebokeh_mains.get(m.__name__) for m in modls}

    await monosrv.monosrv(apps)


try:
    asyncio.run(main())
except KeyboardInterrupt:
    print("Exiting...")
