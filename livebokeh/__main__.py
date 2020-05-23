# TODO : run livebokeh on itself : provide a visualization of  pacakge content...

import asyncio

from livebokeh import monosrv, datamodel

# retrieving all modules
modls = [monosrv, datamodel]

# retrieving livebokeh entry points for each module
apps = {
    m.__name__.replace("livebokeh", "").replace(".", "/"): m._internal_bokeh
    for m in modls if hasattr(m, "_internal_bokeh")
}

try:
    asyncio.run(monosrv.monosrv(apps))
except KeyboardInterrupt:
    print("Exiting...")

