import grpc


class grpcServer(grpc.Server):
    def add_generic_rpc_handlers(self, generic_rpc_handlers):
        pass

    def add_registered_method_handlers(self, service_name, method_handlers):
        pass

    def add_insecure_port(self, address):
        pass

    def add_secure_port(self, address, server_credentials):
        raise NotImplementedError

    def start(self):
        pass

    def stop(self, grace):
        pass

    def wait_for_termination(self, timeout=None):
        pass