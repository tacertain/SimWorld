## gRPC example

The gRPC code uses [grpclib](https://grpclib.readthedocs.io/en/latest/#) instead of Google's implementation as the former
uses the normal asyncio mechanisms for creating servers and opening sockets. Note in the documentation for grpclib that
it creates an additional file of generated code. To run the examples you will need to have run 

`python -m grpc_tools.protoc -I. --python_out=. --grpclib_python_out=. helloworld.proto`

from the `examples/grpc` directory to create these three files:

```aiignore
helloworld_grpc.py
helloworld_pb2.py
helloworld_pb2_grpc.py
```

The `greeter_client.py` and `greeter_server.py` files are from the example code and can be run as two separate processes.
In `test_greeter.py` is the pytest version that can be run in stand-alone or in the debugger. In that version there's a `ServerHost`
that runs the server and a `ClientHost` that has a method, `say_hello()` that runs virtually on the client host. This
demonstrates the testing method of examining the return data in the test case directly.

This version of the test also demonstrates using an `Event()` to wait in the test code for the server to be available. 