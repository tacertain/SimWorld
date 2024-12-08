## SimWorld - A test harness for distributed systems

SimWorld is an event loop for Python's asyncio that allows you to run code virtually on 
multiple hosts in a single debugging session.

### Usage

To use SimWorld, you define tests that subclass from `Host` and implement a `main()` method
(for servers) or functions that run via `Host.run_once()`. Then label your tests with `@simworld_run`.

See the examples folder for more usage examples.