Usage
=====

How to use livebokeh ?

This is still an early stage in development, as the structure is still quite simple.
The best bet as this might still change a lot between versions, is to examine the provided examples.

Design
------

The structure of this package consists of :
- A "Model": DataModel, holding known/observed data that change dynamically.
- A "View": DataView, representing a dynamic rendering of that data into a bokeh document.
Note that many of bokeh's restrictions have already been taken care of.

Example
-------

ClockData that is made simple by leveraging DataModel and DataView to handle the interface with bokeh for rendering dynamically,
while the program is running, and when required by a client, display graphs for it.

