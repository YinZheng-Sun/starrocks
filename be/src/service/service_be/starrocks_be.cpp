// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include <aws/core/Aws.h>
#include <gperftools/malloc_extension.h>
#include <sys/file.h>
#include <unistd.h>

#if defined(LEAK_SANITIZER)
#include <sanitizer/lsan_interface.h>
#endif

#include "backend_service.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "exec/pipeline/query_context.h"
#include "runtime/exec_env.h"
#include "service/brpc.h"
#include "service/service.h"
#include "service/service_be/http_service.h"
#include "service/service_be/internal_service.h"
#include "service/service_be/lake_service.h"
#include "storage/storage_engine.h"
#include "util/logging.h"
#include "util/thrift_server.h"

namespace brpc {

DECLARE_uint64(max_body_size);
DECLARE_int64(socket_max_unwritten_bytes);

} // namespace brpc

void start_be() {
    using starrocks::Status;

    auto* exec_env = starrocks::ExecEnv::GetInstance();

    // Begin to start services
    // 1. Start thrift server with 'be_port'.
    starrocks::ThriftServer* thrift_server = nullptr;
    EXIT_IF_ERROR(starrocks::BackendService::create_service(exec_env, starrocks::config::be_port, &thrift_server));
    Status status = thrift_server->start();
    if (!status.ok()) {
        LOG(ERROR) << "StarRocks Be server did not start correctly, exiting";
        starrocks::shutdown_logging();
        exit(1);
    }

    // 2. Start brpc services.
    brpc::FLAGS_max_body_size = starrocks::config::brpc_max_body_size;
    brpc::FLAGS_socket_max_unwritten_bytes = starrocks::config::brpc_socket_max_unwritten_bytes;
    brpc::Server brpc_server;

    starrocks::BackendInternalServiceImpl<starrocks::PInternalService> internal_service(exec_env);
    starrocks::BackendInternalServiceImpl<doris::PBackendService> backend_service(exec_env);
    starrocks::LakeServiceImpl lake_service(exec_env);

    brpc_server.AddService(&internal_service, brpc::SERVER_DOESNT_OWN_SERVICE);
    brpc_server.AddService(&backend_service, brpc::SERVER_DOESNT_OWN_SERVICE);
    brpc_server.AddService(&lake_service, brpc::SERVER_DOESNT_OWN_SERVICE);

    brpc::ServerOptions options;
    if (starrocks::config::brpc_num_threads != -1) {
        options.num_threads = starrocks::config::brpc_num_threads;
    }
    if (brpc_server.Start(starrocks::config::brpc_port, &options) != 0) {
        LOG(ERROR) << "BRPC service did not start correctly, exiting";
        starrocks::shutdown_logging();
        exit(1);
    }

    // 3. Start HTTP service.
    std::unique_ptr<starrocks::HttpServiceBE> http_service = std::make_unique<starrocks::HttpServiceBE>(
            exec_env, starrocks::config::webserver_port, starrocks::config::webserver_num_workers);
    status = http_service->start();
    if (!status.ok()) {
        LOG(ERROR) << "Internal Error:" << status.message();
        LOG(ERROR) << "StarRocks Be http service did not start correctly, exiting";
        starrocks::shutdown_logging();
        exit(1);
    }

    while (!starrocks::k_starrocks_exit) {
        sleep(10);
    }

    http_service.reset();

    brpc_server.Stop(0);
    brpc_server.Join();

    thrift_server->stop();
    thrift_server->join();
    delete thrift_server;
}
