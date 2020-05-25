# TODO : run livebokeh on itself : provide a visualization of package content...

import asyncio
import functools

from livebokeh import monosrv, datamodel, clockdata, dataview

# retrieving all modules
modls = [monosrv, datamodel, dataview, clockdata]

# launching all examples, BEFORE web request
# to make sure livebokeh code does NOT depend on doc being there

examples = {
    # We need to start all examples in background => async (this will endup being the server loop, but server isnt created just yet...
    m.__name__: asyncio.run(m._internal_example()) for m in modls if hasattr(m, "_internal_example")
}

livebokeh_mains = {
    m.__name__: functools.partial(m._internal_bokeh, example=examples.get(m.__name__))
    for m in modls if hasattr(m, "_internal_bokeh")
}

# retrieving livebokeh entry points for each module, with optional example
apps = {
    "/" + m.__name__: livebokeh_mains.get(m.__name__)
    for m in modls
}

try:
    asyncio.run(monosrv.monosrv(apps))
except KeyboardInterrupt:
    print("Exiting...")

