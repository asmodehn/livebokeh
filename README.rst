livebokeh
=========

Simpler bokeh for dynamic data

The usecase livebokeh is focusing on, is *displaying live data*, upon (webbrowser's) request, providing a webbased dynamic view of the data in your running python code.

livebokeh aims at taking care of a maximum of bokeh interfacing details for this usecase,
so that a livebokeh's user can "just plug-in" livebokeh into his code and visually see the data evolve over time.

Usage
-----

livebokeh relies on pandas for data representation and bokeh for display.

The ``DataModel`` class should be used to encapsulate the dataframe you want to visualize inside an instance.
The it will take care of displaying, and updating the visualization when the dataframe content is updated.

Note : the dataframe is considered immutable, and should be updated "all at once". 
Optimization to detect what has changed or not is taken care of inside livebokeh's code, and the visualization updates sent to the client are minimized.

Deployment
----------

There is *no deployment involved here*. This is a python package and it is therefore integrated into your python process.
This package relies on tornado code, but is not a server by itself.  

Although tornado allows to run the server as multiprocess, to fully benefit from this more complex deployment, it is advised to start processes separately.
https://www.tornadoweb.org/en/stable/guide/running.html#processes-and-ports
Therefore we do not address the deployment usecase here.

development
-----------

This is just a small package written to help debug data processing code.
So lets keep this as simple as possible.

Think about a useful situation that livebokeh doesnt support yet ? post an issue.
Found a bug ? post an issue.
Have anything else you wanna talk about ? post an issue.
Hope this can save you some time...

Roadmap:
--------

* As always, improve documentation.
* some terminal UI to display plots in TTY ?? potentially useful to debug bokeh...
* potentially support static_frame ?? potentially useful to debug pandas...
