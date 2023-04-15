// /// RSCODE: Replica of object_manager_client.h for spill remote

// #pragma once

// #include <grpcpp/grpcpp.h>
// #include <grpcpp/resource_quota.h>
// #include <grpcpp/support/channel_arguments.h>

// #include <thread>

// #include "ray/common/status.h"
// #include "ray/rpc/grpc_client.h"
// #include "ray/util/logging.h"
// #include "src/ray/protobuf/object_manager.grpc.pb.h"
// #include "src/ray/protobuf/object_manager.pb.h"

// namespace ray {
// namespace rpc {

// /// Client used for communicating with a remote node manager server.
// class RemoteSpillClient {
//  public:
//   /// Constructor.
//   ///
//   /// \param[in] address Address of the node manager server.
//   /// \param[in] port Port of the node manager server.
//   /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
//   RemoteSpillClient(const std::string &address,
//                       const int port,
//                       ClientCallManager &client_call_manager,
//                       int num_connections = 2)
//       : num_connections_(num_connections) {
//     spillremote_rr_index_ = rand() % num_connections_;
    
//     grpc_clients_.reserve(num_connections_);
//     for (int i = 0; i < num_connections_; i++) {
//       grpc_clients_.emplace_back(new GrpcClient<ObjectManagerService>(
//           address, port, client_call_manager, num_connections_));
//     }
//   };

//   /// RSGRPC: (GRPC)
//   /// Tell remote object manager to accept spilling objects
//   ///
//   /// \param request The request message
//   /// \param callback  The callback function that handles reply
//   VOID_RPC_CLIENT_METHOD(ObjectManagerService,
//                          SpillRemote,
//                          grpc_clients_[spillremote_rr_index_++ % num_connections_],
//                          /*method_timeout_ms*/ -1, ) 

//  private:
//   /// To optimize object manager performance we create multiple concurrent
//   /// GRPC connections, and use these connections in a round-robin way.
//   int num_connections_;

//   /// RSGRPC: (GRPC)
//   // Current connection index for `SpillRemote`.
//   std::atomic<unsigned int> spillremote_rr_index_;

//   /// The RPC clients.
//   std::vector<std::unique_ptr<GrpcClient<ObjectManagerService>>> grpc_clients_;
// };

// }  // namespace rpc
// }  // namespace ray