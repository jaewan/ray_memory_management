/// RSCODE: Replica of object_manager_server.h for spill remote

#pragma once

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/rpc/grpc_server.h"
#include "ray/rpc/server_call.h"
#include "src/ray/protobuf/remote_spill.grpc.pb.h"
#include "src/ray/protobuf/remote_spill.pb.h"

namespace ray {
namespace rpc {

#define RAY_REMOTE_SPILL_RPC_HANDLERS               \
  RPC_SERVICE_HANDLER(RemoteSpillService, SpillRemote, -1) \

/// Implementations of the `RemoteSpillGrpcService`, check interface in
/// `src/ray/protobuf/object_manager.proto`.
class RemoteSpillServiceHandler {
 public:
  /// Handle a `Spill remote` request.
  /// The implementation can handle this request asynchronously. When handling is done,
  /// the `send_reply_callback` should be called.
  ///
  /// \param[in] request The request message.
  /// \param[out] reply The reply message.
  /// \param[in] send_reply_callback The callback to be called when the request is done.
  /// RSGRPC: Handle a `SpillRemote` request (GRPC)
  virtual void HandleSpillRemote(const SpillRemoteRequest &request,
                                 SpillRemoteReply *reply,
                                 SendReplyCallback send_reply_callback) = 0;
};

/// The `GrpcService` for `RemoteSpillGrpcService`.
class RemoteSpillGrpcService : public GrpcService {
 public:
  /// Construct a `RemoteSpillGrpcService`.
  ///
  /// \param[in] port See `GrpcService`.
  /// \param[in] handler The service handler that actually handle the requests.
  RemoteSpillGrpcService(instrumented_io_context &io_service,
                          RemoteSpillServiceHandler &service_handler)
      : GrpcService(io_service), service_handler_(service_handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories) override {
    RAY_REMOTE_SPILL_RPC_HANDLERS
  }

 private:
  /// The grpc async service object.
  RemoteSpillService::AsyncService service_;
  /// The service handler that actually handle the requests.
  RemoteSpillServiceHandler &service_handler_;
};

}  // namespace rpc
}  // namespace ray
