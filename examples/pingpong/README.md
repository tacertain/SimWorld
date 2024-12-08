## pingpong example

The pingpong example is about the simplest test that could be written. The `pinger.py` and `ponger.py` files are classic example
files and can be run as two separate processes to show network connectivity. In `testall.py` is the  pytest version that can be 
run in stand-alone or in the debugger. In that version there's a `Ponger`
that runs the listener  and a `Pinger` that has a method, `ping()` that runs virtually on the client host. 