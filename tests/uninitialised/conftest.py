"""
Fixtures for the ``uninitialised`` suite.

This suite verifies that the public API refuses to work before ``initialise()``
is called. It must therefore run in its own process where orcha has never been
initialised, so it deliberately does *not* provide any initialise/clean-db
fixtures (unlike the ``core`` suite). Run it separately, e.g.::

    pytest orcha/tests/uninitialised
"""
