// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/object_manager/object_manager.h"

#include <chrono>

#include "ray/common/common_protocol.h"
#include "ray/stats/metric_defs.h"
#include "ray/util/util.h"

namespace asio = boost::asio;

namespace ray {

ObjectStoreRunner::ObjectStoreRunner(const ObjectManagerConfig &config,
                                     SpillObjectsCallback spill_objects_callback,
                                     std::function<void()> object_store_full_callback,
                                     AddObjectCallback add_object_callback,
                                     DeleteObjectCallback delete_object_callback) {
  plasma::plasma_store_runner.reset(
      new plasma::PlasmaStoreRunner(config.store_socket_name,
                                    config.object_store_memory,
                                    config.huge_pages,
                                    config.plasma_directory,
                                    config.fallback_directory));
  // Initialize object store.
  store_thread_ = std::thread(&plasma::PlasmaStoreRunner::Start,
                              plasma::plasma_store_runner.get(),
                              spill_objects_callback,
                              object_store_full_callback,
                              add_object_callback,
                              delete_object_callback);
  // Sleep for sometime until the store is working. This can suppress some
  // connection warnings.
  std::this_thread::sleep_for(std::chrono::microseconds(500));
}

ObjectStoreRunner::~ObjectStoreRunner() {
  plasma::plasma_store_runner->Stop();
  store_thread_.join();
  plasma::plasma_store_runner.reset();
}

/// RSCODE:
// RemoteSpill::RemoteSpill()
//   : remote_spill_rpc_work_(remote_spill_rpc_service_),
//     remote_spill_service_(remote_spill_rpc_service_, *this) {


// }

ObjectManager::ObjectManager(
    instrumented_io_context &main_service,
    const NodeID &self_node_id,
    const ObjectManagerConfig &config,
    IObjectDirectory *object_directory,
    /// RSCOMMENT: code that goes to LocalObjectManager to restore spilled objects. 
    RestoreSpilledObjectCallback restore_spilled_object,
    /// RSCODE:
    std::function<bool(const ObjectID &, int64_t)> restore_remote_spilled_object,
    std::function<std::string(const ObjectID &)> get_spilled_object_url,
    SpillObjectsCallback spill_objects_callback,
    std::function<void()> object_store_full_callback,
    AddObjectCallback add_object_callback,
    DeleteObjectCallback delete_object_callback,
    std::function<std::unique_ptr<RayObject>(const ObjectID &object_id)> pin_object,
    const std::function<void(const ObjectID &, rpc::ErrorType)> fail_pull_request)
    : main_service_(&main_service),
      self_node_id_(self_node_id),
      config_(config),
      object_directory_(object_directory),
      object_store_internal_(
          config,
          spill_objects_callback,
          object_store_full_callback,
          /*add_object_callback=*/
          [this, add_object_callback = std::move(add_object_callback)](
              const ObjectInfo &object_info) {
            main_service_->post(
                [this,
                 object_info,
                 add_object_callback = std::move(add_object_callback)]() {
                  HandleObjectAdded(object_info);
                  add_object_callback(object_info);
                },
                "ObjectManager.ObjectAdded");
          },
          /*delete_object_callback=*/
          [this, delete_object_callback = std::move(delete_object_callback)](
              const ObjectID &object_id) {
            main_service_->post(
                [this,
                 object_id,
                 delete_object_callback = std::move(delete_object_callback)]() {
                  HandleObjectDeleted(object_id);
                  delete_object_callback(object_id);
                },
                "ObjectManager.ObjectDeleted");
          }),
      buffer_pool_store_client_(std::make_shared<plasma::PlasmaClient>()),
      buffer_pool_(buffer_pool_store_client_, config_.object_chunk_size),
      rpc_work_(rpc_service_),
      /// RSCODE:
      remote_spill_rpc_work_(remote_spill_rpc_service_),
      object_manager_server_("ObjectManager",
                             config_.object_manager_port,
                             config_.object_manager_address == "127.0.0.1",
                             config_.rpc_service_threads_number),
      object_manager_service_(rpc_service_, *this),
      /// RSCODE:
      remote_spill_service_handler_(
          new RemoteSpill(config_)),
      /// RSCODE:
      remote_spill_service_(remote_spill_rpc_service_, *remote_spill_service_handler_),
      client_call_manager_(main_service, config_.rpc_service_threads_number),
      restore_spilled_object_(restore_spilled_object),
      /// RSCODE:
      restore_remote_spilled_object_(restore_remote_spilled_object),
      get_spilled_object_url_(get_spilled_object_url),
      pull_retry_timer_(*main_service_,
                        boost::posix_time::milliseconds(config.timer_freq_ms)) {
  RAY_CHECK(config_.rpc_service_threads_number > 0);

  /// RSCODE: Spill remote manager
  spill_remote_manager_.reset(new SpillRemoteManager(/* max_chunks_in_flight= */ std::max(
      static_cast<int64_t>(1L),
      static_cast<int64_t>(config_.max_bytes_in_flight / config_.object_chunk_size))));

  push_manager_.reset(new PushManager(/* max_chunks_in_flight= */ std::max(
      static_cast<int64_t>(1L),
      static_cast<int64_t>(config_.max_bytes_in_flight / config_.object_chunk_size))));

  pull_retry_timer_.async_wait([this](const boost::system::error_code &e) { Tick(e); });

  const auto &object_is_local = [this](const ObjectID &object_id) {
    return local_objects_.count(object_id) != 0;
  };
  const auto &send_pull_request = [this](const ObjectID &object_id,
                                         const NodeID &client_id) {
    SendPullRequest(object_id, client_id);
  };
  const auto &cancel_pull_request = [this](const ObjectID &object_id) {
    // We must abort this object because it may have only been partially
    // created and will cause a leak if we never receive the rest of the
    // object. This is a no-op if the object is already sealed or evicted.
    buffer_pool_.AbortCreate(object_id);
  };
  const auto &get_time = []() { return absl::GetCurrentTimeNanos() / 1e9; };
  int64_t available_memory = config.object_store_memory;
  if (available_memory < 0) {
    available_memory = 0;
  }
  pull_manager_.reset(new PullManager(self_node_id_,
                                      object_is_local,
                                      send_pull_request,
                                      cancel_pull_request,
                                      fail_pull_request,
                                      restore_spilled_object_,
                                      /// RSCODE:
                                      restore_remote_spilled_object_,
                                      get_time,
                                      config.pull_timeout_ms,
                                      available_memory,
                                      pin_object,
                                      get_spilled_object_url));

  RAY_CHECK_OK(
      buffer_pool_store_client_->Connect(config_.store_socket_name.c_str(), "", 0, 300));

  // Start object manager rpc server and send & receive request threads
  StartRpcService();
}

ObjectManager::~ObjectManager() { StopRpcService(); }

void ObjectManager::Stop() { plasma::plasma_store_runner->Stop(); }

bool ObjectManager::IsPlasmaObjectSpillable(const ObjectID &object_id) {
  return plasma::plasma_store_runner->IsPlasmaObjectSpillable(object_id);
}

void ObjectManager::RunRpcService(int index) {
  SetThreadName("rpc.obj.mgr." + std::to_string(index));
  rpc_service_.run();
}

/// RSCODE: 
void ObjectManager::RunRemoteSpillRpcService(int index) {
  SetThreadName("rpc.rs.obj.mgr." + std::to_string(index));
  remote_spill_rpc_service_.run();
}

void ObjectManager::StartRpcService() {
  rpc_threads_.resize(config_.rpc_service_threads_number);
  for (int i = 0; i < config_.rpc_service_threads_number; i++) {
    rpc_threads_[i] = std::thread(&ObjectManager::RunRpcService, this, i);
  }

  /// RSCODE:
  remote_spill_rpc_threads_.resize(config_.rpc_service_threads_number);
  for (int i = 0; i < config_.rpc_service_threads_number; i++) {
    remote_spill_rpc_threads_[i] = std::thread(&ObjectManager::RunRemoteSpillRpcService, this, i);
  }

  /// RSTODO: Delete later
  RAY_LOG(INFO) << "For counting purposes, RPC service threads number: " << config_.rpc_service_threads_number;
  object_manager_server_.RegisterService(object_manager_service_);
  /// RSCODE: 
  object_manager_server_.RegisterService(remote_spill_service_);
  object_manager_server_.Run();
}

void ObjectManager::StopRpcService() {
  rpc_service_.stop();
  for (int i = 0; i < config_.rpc_service_threads_number; i++) {
    rpc_threads_[i].join();
  }

  /// RSCODE:
  remote_spill_rpc_service_.stop();
  for (int i = 0; i < config_.rpc_service_threads_number; i++) {
    remote_spill_rpc_threads_[i].join();
  }
  
  object_manager_server_.Shutdown();

  // remote_spill_server_.Shutdown();
}

void ObjectManager::HandleObjectAdded(const ObjectInfo &object_info) {
  // Notify the object directory that the object has been added to this node.
  const ObjectID &object_id = object_info.object_id;
  RAY_LOG(DEBUG) << "Object added " << object_id;

  /// RSCODE:
  // if (received_remote_objects_origin_.contains(object_id)) {
  //   buffer_pool_store_client_->RemoteSpillIncreaseObjectCount(object_id);
  // }

  RAY_CHECK(local_objects_.count(object_id) == 0);
  local_objects_[object_id].object_info = object_info;
  used_memory_ += object_info.data_size + object_info.metadata_size;
  object_directory_->ReportObjectAdded(object_id, self_node_id_, object_info);

  // Give the pull manager a chance to pin actively pulled objects.
  pull_manager_->PinNewObjectIfNeeded(object_id);

  // Handle the unfulfilled_push_requests_ which contains the push request that is not
  // completed due to unsatisfied local objects.
  auto iter = unfulfilled_push_requests_.find(object_id);
  if (iter != unfulfilled_push_requests_.end()) {
    for (auto &pair : iter->second) {
      auto &node_id = pair.first;
      main_service_->post([this, object_id, node_id]() { Push(object_id, node_id); },
                          "ObjectManager.ObjectAddedPush");
      // When push timeout is set to -1, there will be an empty timer in pair.second.
      if (pair.second != nullptr) {
        pair.second->cancel();
      }
    }
    unfulfilled_push_requests_.erase(iter);
  }
}

void ObjectManager::HandleObjectDeleted(const ObjectID &object_id) {
  auto it = local_objects_.find(object_id);
  RAY_CHECK(it != local_objects_.end());
  auto object_info = it->second.object_info;
  local_objects_.erase(it);
  used_memory_ -= object_info.data_size + object_info.metadata_size;
  RAY_CHECK(!local_objects_.empty() || used_memory_ == 0);
  object_directory_->ReportObjectRemoved(object_id, self_node_id_, object_info);

  // Ask the pull manager to fetch this object again as soon as possible, if
  // it was needed by an active pull request.
  pull_manager_->ResetRetryTimer(object_id);
}

uint64_t ObjectManager::Pull(const std::vector<rpc::ObjectReference> &object_refs,
                             BundlePriority prio) {
  std::vector<rpc::ObjectReference> objects_to_locate;
  auto request_id = pull_manager_->Pull(object_refs, prio, &objects_to_locate);

  const auto &callback = [this](const ObjectID &object_id,
                                const std::unordered_set<NodeID> &client_ids,
                                const std::string &spilled_url,
                                const NodeID &spilled_node_id,
                                bool pending_creation,
                                size_t object_size) {
    pull_manager_->OnLocationChange(object_id,
                                    client_ids,
                                    spilled_url,
                                    spilled_node_id,
                                    pending_creation,
                                    object_size);
  };

  for (const auto &ref : objects_to_locate) {
    // Subscribe to object notifications. A notification will be received every
    // time the set of node IDs for the object changes. Notifications will also
    // be received if the list of locations is empty. The set of node IDs has
    // no ordering guarantee between notifications.

    auto object_id = ObjectRefToId(ref);

    /// RSTODO: Delete later
    RAY_LOG(INFO) << "About to call SubscribeObjectLocations on object: " << object_id;

    RAY_CHECK_OK(object_directory_->SubscribeObjectLocations(
        object_directory_pull_callback_id_, object_id, ref.owner_address(), callback));
  }

  return request_id;
}

void ObjectManager::CancelPull(uint64_t request_id) {
  const auto objects_to_cancel = pull_manager_->CancelPull(request_id);
  for (const auto &object_id : objects_to_cancel) {
    RAY_CHECK_OK(object_directory_->UnsubscribeObjectLocations(
        object_directory_pull_callback_id_, object_id));
  }
}

void ObjectManager::SendPullRequest(const ObjectID &object_id, const NodeID &client_id, const std::function<void()> callback, const bool from_remote) {
  auto rpc_client = GetRpcClient(client_id);

  /// RSTODO: Delete this later
  if (from_remote) {
    RAY_LOG(INFO) << "Sending pull request for: " << object_id;
  }

  if (rpc_client) {
    // Try pulling from the client.
    rpc_service_.post(
        /// RSCODE:
        [this, object_id, client_id, from_remote, rpc_client, callback]() {
          rpc::PullRequest pull_request;
          pull_request.set_object_id(object_id.Binary());
          pull_request.set_node_id(self_node_id_.Binary());

          /// RSCODE: Add from_remote argument to request
          pull_request.set_from_remote(from_remote);

          rpc_client->Pull(
              pull_request,
              /// RSCODE:
              [object_id, client_id, callback](const Status &status, const rpc::PullReply &reply) {
                if (!status.ok()) {
                  RAY_LOG(WARNING) << "Send pull " << object_id << " request to client "
                                   << client_id << " failed due to" << status.message();
                }
                /// RSCODE: Callback to update object pending restore
                else {
                  RAY_LOG(INFO) << "We are calling callback to update object pending restore for object: " << object_id;
                  callback();
                }
              });
        },
        "ObjectManager.SendPull");
    /// RSCODE: Add code to delete object entry from hash map if remote
    if (from_remote) {
      /// RSTODO: Delete later
      RAY_LOG(INFO) << "Erasing from spilled_remote_objects_url_ after pull for object: " << object_id;
      spilled_remote_objects_url_.erase(object_id);
    }
  } else {
    RAY_LOG(ERROR) << "Couldn't send pull request from " << self_node_id_ << " to "
                   << client_id << " of object " << object_id
                   << " , setup rpc connection failed.";
  }
}

void ObjectManager::HandlePushTaskTimeout(const ObjectID &object_id,
                                          const NodeID &node_id) {
  RAY_LOG(WARNING) << "Invalid Push request ObjectID: " << object_id
                   << " after waiting for " << config_.push_timeout_ms << " ms.";
  auto iter = unfulfilled_push_requests_.find(object_id);
  // Under this scenario, `HandlePushTaskTimeout` can be invoked
  // although timer cancels it.
  // 1. wait timer is done and the task is queued.f
  // 2. While task is queued, timer->cancel() is invoked.
  // In this case this method can be invoked although it is not timed out.
  // https://www.boost.org/doc/libs/1_66_0/doc/html/boost_asio/reference/basic_deadline_timer/cancel/overload1.html.
  if (iter == unfulfilled_push_requests_.end()) {
    return;
  }
  size_t num_erased = iter->second.erase(node_id);
  RAY_CHECK(num_erased == 1);
  if (iter->second.size() == 0) {
    unfulfilled_push_requests_.erase(iter);
  }
}

void ObjectManager::HandleSendFinished(const ObjectID &object_id,
                                       const NodeID &node_id,
                                       uint64_t chunk_index,
                                       double start_time,
                                       double end_time,
                                       ray::Status status) {
  /// RSTODO: Comment this out for now
  // RAY_LOG(DEBUG) << "HandleSendFinished on " << self_node_id_ << " to " << node_id
  //                << " of object " << object_id << " chunk " << chunk_index
  //                << ", status: " << status.ToString();
  if (!status.ok()) {
    // TODO(rkn): What do we want to do if the send failed?
    RAY_LOG(DEBUG) << "Failed to send a push request for an object " << object_id
                   << " to " << node_id << ". Chunk index: " << chunk_index;
  }
}

/// RSTODO: Refactor and delete this later
void ObjectManager::TempAccessPullRequest(const ObjectID &object_id, const NodeID &node_id, const std::function<void()> callback) {
  SendPullRequest(object_id, node_id, callback, true);
}

/// RSCODE: Function to identify remote node with available memory
std::vector<ObjectID> ObjectManager::FindNodeToSpill(const std::vector<ObjectID> requested_objects_to_spill, const std::function<void(ObjectID)> callback) {
  std::vector<ObjectID> objects_to_spill_to_disk;

  const auto remote_connections = object_directory_->LookupAllRemoteConnections();
  for (const auto &connection_info : remote_connections) {
    /// RSTODO: Delete later
    RAY_LOG(INFO) << "Iterating through remote connections";

    const NodeID node_id = connection_info.node_id;

    auto rpc_client = GetRpcClient(node_id);
    if (!rpc_client) {
      RAY_LOG(INFO) << "Failed to establish connection for FindNodeToSpill with remote object manager.";
    }

    rpc::CheckAvailableRemoteMemoryRequest check_available_remote_memory_request;
    rpc::ClientCallback<rpc::CheckAvailableRemoteMemoryReply> callback =
      [this, node_id] (const Status &status, const rpc::CheckAvailableRemoteMemoryReply &reply) {
        if (status.ok()) {
          /// RSTODO: Delete later
          RAY_LOG(INFO) << "Starting to add available memory to hashmap for node: " << node_id;
          {
            absl::MutexLock lock(&mutex_);
            node_to_available_memory_.emplace(node_id, reply.available_memory());
          }

          RAY_LOG(INFO) << "Finishing adding available memory to hashmap for node: " << node_id;
        }
      };

    /// RSTODO: Delete later
    RAY_LOG(INFO) << "About to call CheckAvailableRemoteMemory RPC on node: " << node_id;

    rpc_client->CheckAvailableRemoteMemory(check_available_remote_memory_request, callback);   
  }

  // Print contents of node_to_available_memory
  for (const auto &pair : node_to_available_memory_) {
    RAY_LOG(INFO) << "Node: " << pair.first << " has available memory: " << pair.second;
  }

  // Iterate through node_to_available_memory to find node with most available memory
  for(size_t i = 0; i < requested_objects_to_spill.size(); i++) {
    /// RSTODO: Delete later
    RAY_LOG(INFO) << "We are finding available memory for object: " << requested_objects_to_spill[i];

    NodeID node_id;
    int64_t max_available_memory = 0;
    for (const auto &pair : node_to_available_memory_) {
      if (pair.second > max_available_memory) {
        node_id = pair.first;
        max_available_memory = pair.second;
      }
    }
    const ObjectInfo &object_info = local_objects_[requested_objects_to_spill[i]].object_info;
    int64_t data_size = object_info.data_size;

    if (data_size > max_available_memory) {
      /// RSTODO: Delete later
      RAY_LOG(INFO) << "Data size: " << data_size << " is greater than max available memory: " << max_available_memory 
        << " for node: " << node_id << " and we will spill to disk instead for object: " << requested_objects_to_spill[i];

      objects_to_spill_to_disk.push_back(requested_objects_to_spill[i]);
    } else {
      /// RSTODO: Only update available memory if spill is successful in case of fault tolerance
      {
        absl::MutexLock lock(&mutex_);
        node_to_available_memory_[node_id] -= data_size;
      }
      SpillRemote(requested_objects_to_spill[i], node_id, callback);
    }
  }

  return objects_to_spill_to_disk;
}

/// RSGRPC:
void ObjectManager::HandleCheckAvailableRemoteMemory(const rpc::CheckAvailableRemoteMemoryRequest &request,
                                      rpc::CheckAvailableRemoteMemoryReply *reply,
                                      rpc::SendReplyCallback send_reply_callback) {
  /// RSTODO: Delete later
  RAY_LOG(INFO) << "Starting call HandleCheckAvailableRemoteMemory RPC";

  RAY_LOG(INFO) << "Available memory: " << config_.object_store_memory - used_memory_;  

  // Something like this
  reply->set_available_memory(config_.object_store_memory - used_memory_);

  /// RSTODO: Delete later
  RAY_LOG(INFO) << "Finishing call HandleCheckAvailableRemoteMemory RPC";

  send_reply_callback(Status::OK(), nullptr, nullptr);
}

/// RSCODE: Decrement object ref count
void ObjectManager::RemoteSpillDecrementRefCount(const ObjectID &object_id) {
  buffer_pool_store_client_->RemoteSpillDecreaseObjectCount(object_id);
}

/// RSCODE: Increment object ref count
void ObjectManager::RemoteSpillIncrementRefCount(const ObjectID &object_id) {
  buffer_pool_store_client_->RemoteSpillIncreaseObjectCount(object_id);
}

/// RSTODO: Delete later
void ObjectManager::RemoteSpillViewRefCount(const ObjectID &object_id) {
  buffer_pool_store_client_->RemoteSpillViewObjectCount(object_id);
}

/// RSCODE:
void ObjectManager::DeleteRemoteSpilledObject(const ObjectID &object_id) {
  RAY_LOG(DEBUG) << "About to call DeleteRemoteSpilledObject RPC on object: " << object_id;
  const auto it = spilled_remote_objects_to_free_.find(object_id);
  if (it != spilled_remote_objects_to_free_.end()) {
    NodeID node_id = it->second;
    rpc_service_.post(
    [this, object_id, node_id]() {
      DeleteRemoteSpilledObjectRequest(object_id, node_id);
    },
    "ObjectManager.DeleteRemoteSpilledObject");
  } else {
    RAY_LOG(DEBUG) << "Remote object not in spilled_remote_objects_to_free_";
  }
}

/// RSCODE:
void ObjectManager::DeleteRemoteSpilledObjectRequest(const ObjectID &object_id, const NodeID &node_id) {
  auto rpc_client = GetRpcClient(node_id);
  rpc::DeleteRemoteSpilledObjectRequest delete_remote_spilled_object_request;
  delete_remote_spilled_object_request.set_remote_spilled_object_id(object_id.Binary());

  rpc::ClientCallback<rpc::DeleteRemoteSpilledObjectReply> callback =
      [this, object_id, node_id] (const Status &status, const rpc::DeleteRemoteSpilledObjectReply &reply) {
        if (status.ok()) {
          RAY_LOG(INFO) << "Successfully freed " << object_id << " in remote node " << node_id;
        }
      };

  rpc_client->DeleteRemoteSpilledObject(delete_remote_spilled_object_request, callback);  
}

/// RSGRPC:
void ObjectManager::HandleDeleteRemoteSpilledObject(const rpc::DeleteRemoteSpilledObjectRequest &request,
                                      rpc::DeleteRemoteSpilledObjectReply *reply,
                                      rpc::SendReplyCallback send_reply_callback) {
  ObjectID object_id = ObjectID::FromBinary(request.remote_spilled_object_id());

  /// RSTODO: Delete this later
  RAY_LOG(INFO) << "About to free object in remote node: " << object_id;

  /// RSTODO: Delete this later
  for (auto it = local_objects_.begin(); it != local_objects_.end(); it++) {
    RAY_LOG(INFO) << "Object in local_objects_: " << it->first;
  }

  auto it = local_objects_.find(object_id);
  // Check if object is local
  if (it != local_objects_.end()) {
    RAY_LOG(INFO) << "Object " << object_id << " is local";
    // RemoteSpillDecrementRefCount(object_id);
  }

  // remote_spill_service_handler_.received_remote_objects_origin_.erase(object_id);

  send_reply_callback(Status::OK(), nullptr, nullptr);
}

/// RSCODE:
void ObjectManager::IncrementRemoteObjectRefCount(const ObjectID &object_id) {
  RAY_LOG(DEBUG) << "About to call IncrementRemoteObjectRefCount RPC on object: " << object_id;
  const auto it = spilled_remote_objects_to_free_.find(object_id);
  if (it != spilled_remote_objects_to_free_.end()) {
    NodeID node_id = it->second;
    rpc_service_.post(
    [this, object_id, node_id]() {
      IncrementRemoteObjectRefCountRequest(object_id, node_id);
    },
    "ObjectManager.DeleteRemoteSpilledObject");
  } else {
    RAY_LOG(DEBUG) << "Remote object not in spilled_remote_objects_to_free_";
  }
}

/// RSCODE:
void ObjectManager::IncrementRemoteObjectRefCountRequest(const ObjectID &object_id, const NodeID &node_id) {
  auto rpc_client = GetRpcClient(node_id);
  rpc::IncrementRemoteObjectRefCountRequest increment_remote_object_ref_count_request;
  increment_remote_object_ref_count_request.set_remote_spilled_object_id(object_id.Binary());

  rpc::ClientCallback<rpc::IncrementRemoteObjectRefCountReply> callback =
      [this, object_id, node_id] (const Status &status, const rpc::IncrementRemoteObjectRefCountReply &reply) {
        if (status.ok()) {
          RAY_LOG(INFO) << "Successfully incremented ref count of object: " << object_id << " in remote node " << node_id;
        }
      };

  rpc_client->IncrementRemoteObjectRefCount(increment_remote_object_ref_count_request, callback);  
}

/// RSGRPC:
void ObjectManager::HandleIncrementRemoteObjectRefCount(const rpc::IncrementRemoteObjectRefCountRequest &request,
                                      rpc::IncrementRemoteObjectRefCountReply *reply,
                                      rpc::SendReplyCallback send_reply_callback) {
  ObjectID object_id = ObjectID::FromBinary(request.remote_spilled_object_id());

  /// RSTODO: Delete this later
  RAY_LOG(INFO) << "In remote node, about to increment ref count of object: " << object_id;

  RemoteSpillIncrementRefCount(object_id);

  send_reply_callback(Status::OK(), nullptr, nullptr);
}
/// RSCODE: Implement spill function to spill object to remote memory
void ObjectManager::SpillRemote(const ObjectID &object_id, const NodeID &node_id, const std::function<void(ObjectID)> callback) {
  /// RSCODE: Add code to add object id to node id mapping
  RAY_LOG(INFO) << "Object we are trying to spill: " << object_id;
  spilled_remote_objects_url_.emplace(object_id, node_id);
  spilled_remote_objects_to_free_.emplace(object_id, node_id);
  spilled_remote_objects_tracker_.emplace(object_id, node_id);

  if (pulled_objects_from_remote_.contains(object_id)) {
    pulled_objects_from_remote_.erase(object_id );
  }

  /// RSTODO: Delete later
  RAY_LOG(INFO) << "Spill Remote Mapping Info in SpillRemote: " << spilled_remote_objects_url_.size();

  RAY_LOG(DEBUG) << "Spill remotely on " << self_node_id_ << " to " << node_id << " of object "
              << object_id;

  const ObjectInfo &object_info = local_objects_[object_id].object_info;
  /// RSTODO: Original code but doesn't work with remote spill
  // uint64_t data_size = static_cast<uint64_t>(object_info.data_size);
  // uint64_t metadata_size = static_cast<uint64_t>(object_info.metadata_size);
  // uint64_t offset = 0;

  /// RSTODO: Delete later
  RAY_LOG(INFO) << "Owner address: " << object_info.owner_ip_address << " for object: " << object_id;

  rpc::Address owner_address;
  owner_address.set_raylet_id(object_info.owner_raylet_id.Binary());
  owner_address.set_ip_address(object_info.owner_ip_address);
  owner_address.set_port(object_info.owner_port);
  owner_address.set_worker_id(object_info.owner_worker_id.Binary());

  std::pair<std::shared_ptr<MemoryObjectReader>, ray::Status> reader_status =
      buffer_pool_.CreateObjectReader(object_id, owner_address);
  Status status = reader_status.second;
  if (!status.ok()) {
    RAY_LOG_EVERY_N_OR_DEBUG(INFO, 100)
        << "Ignoring stale read request for already deleted object: " << object_id;
    return;
  }

  auto object_reader = std::move(reader_status.first);
  RAY_CHECK(object_reader) << "object_reader can't be null";

  /// RSTODO: Comment out for now
  // if (object_reader->GetDataSize() != data_size ||
  //     object_reader->GetMetadataSize() != metadata_size) {
  //   // TODO(scv119): handle object size changes in a more graceful way.
  //   RAY_LOG(WARNING) << "Object id:" << object_id
  //                    << "'s size mismatches our record. Expected data size: " << data_size
  //                    << ", expected metadata size: " << metadata_size
  //                    << ", actual data size: " << object_reader->GetDataSize()
  //                    << ", actual metadata size: " << object_reader->GetMetadataSize()
  //                    << ". This is likely due to a race condition."
  //                    << " We will update the object size and proceed sending the object.";
  //   local_objects_[object_id].object_info.data_size = 0;
  //   local_objects_[object_id].object_info.metadata_size = 1;
  // }

  /// RSTODO: Comment this code if you want to test spilling
  SpillRemoteInternal(object_id,
                     node_id,
                     std::make_shared<ChunkObjectReader>(std::move(object_reader),
                     config_.object_chunk_size),
                     callback);

  /// RSTODO: Original code but doesn't work with remote spill
  // std::string result(data_size, '\0');
  // RAY_LOG(INFO) << "test1";
  // object_reader->ReadFromDataSection(offset, data_size, &result[0]);

  // RAY_LOG(INFO) << "test2";

  // rpc::SpillRemoteRequest spill_remote_request;
  // auto spill_id = UniqueID::FromRandom();
  // spill_remote_request.set_spill_id(spill_id.Binary());
  // spill_remote_request.set_object_id(object_id.Binary());
  // spill_remote_request.set_node_id(node_id.Binary());
  // spill_remote_request.set_allocated_owner_address(&owner_address);
  // spill_remote_request.set_chunk_index(0);
  // spill_remote_request.set_data_size(data_size);
  // spill_remote_request.set_metadata_size(metadata_size);
  // spill_remote_request.set_data(std::move(result));  

  // rpc::ClientCallback<rpc::SpillRemoteReply> callback =
  //     [] (const Status &status, const rpc::SpillRemoteReply &reply) {
  //       std::cout << "hello";
  //     };

  // RAY_LOG(INFO) << "test3";
  // rpc_client->SpillRemote(spill_remote_request, callback);
  // RAY_LOG(INFO) << "test4";
}

/// RSTODO: Delete from_remote arg later
void ObjectManager::Push(const ObjectID &object_id, const NodeID &node_id, const bool from_remote) {
  RAY_LOG(DEBUG) << "Push on " << self_node_id_ << " to " << node_id << " of object "
                 << object_id;

  /// RSTODO: Delete this later
  if (from_remote) {
    RAY_LOG(INFO) << "Push from remote from " << self_node_id_ << " to " << node_id << " of object "
                 << object_id;
  }

  if (local_objects_.count(object_id) != 0) {
    /// RSTODO: Delete later
    RAY_LOG(INFO) << "Pushing from local for object " << object_id;

    /// RSCODE:
    return PushLocalObject(object_id, node_id, from_remote);
  }

  // Push from spilled object directly if the object is on local disk.
  auto object_url = get_spilled_object_url_(object_id);
  if (!object_url.empty() && RayConfig::instance().is_external_storage_type_fs()) {
    /// RSTODO: Delete later
    RAY_LOG(INFO) << "Try to push from file system";

    return PushFromFilesystem(object_id, node_id, object_url);
  }

  // Avoid setting duplicated timer for the same object and node pair.
  auto &nodes = unfulfilled_push_requests_[object_id];

  if (nodes.count(node_id) == 0) {
    // If config_.push_timeout_ms < 0, we give an empty timer
    // and the task will be kept infinitely.
    std::unique_ptr<boost::asio::deadline_timer> timer;
    if (config_.push_timeout_ms == 0) {
      // The Push request fails directly when config_.push_timeout_ms == 0.
      RAY_LOG(WARNING) << "Invalid Push request ObjectID " << object_id
                       << " due to direct timeout setting. (0 ms timeout)";
    } else if (config_.push_timeout_ms > 0) {
      // Put the task into a queue and wait for the notification of Object added.
      timer.reset(new boost::asio::deadline_timer(*main_service_));
      auto clean_push_period = boost::posix_time::milliseconds(config_.push_timeout_ms);
      timer->expires_from_now(clean_push_period);
      timer->async_wait(
          [this, object_id, node_id](const boost::system::error_code &error) {
            // Timer killing will receive the boost::asio::error::operation_aborted,
            // we only handle the timeout event.
            if (!error) {
              HandlePushTaskTimeout(object_id, node_id);
            }
          });
    }
    if (config_.push_timeout_ms != 0) {
      nodes.emplace(node_id, std::move(timer));
    }
  }
}

void ObjectManager::PushLocalObject(const ObjectID &object_id, const NodeID &node_id, bool from_remote) {
  const ObjectInfo &object_info = local_objects_[object_id].object_info;
  uint64_t data_size = static_cast<uint64_t>(object_info.data_size);
  uint64_t metadata_size = static_cast<uint64_t>(object_info.metadata_size);

  rpc::Address owner_address;
  owner_address.set_raylet_id(object_info.owner_raylet_id.Binary());
  owner_address.set_ip_address(object_info.owner_ip_address);
  owner_address.set_port(object_info.owner_port);
  owner_address.set_worker_id(object_info.owner_worker_id.Binary());

  std::pair<std::shared_ptr<MemoryObjectReader>, ray::Status> reader_status =
      buffer_pool_.CreateObjectReader(object_id, owner_address);
  Status status = reader_status.second;
  if (!status.ok()) {
    RAY_LOG_EVERY_N_OR_DEBUG(INFO, 100)
        << "Ignoring stale read request for already deleted object: " << object_id;
    return;
  }

  auto object_reader = std::move(reader_status.first);
  RAY_CHECK(object_reader) << "object_reader can't be null";

  if (object_reader->GetDataSize() != data_size ||
      object_reader->GetMetadataSize() != metadata_size) {
    // TODO(scv119): handle object size changes in a more graceful way.
    RAY_LOG(WARNING) << "Object id:" << object_id
                     << "'s size mismatches our record. Expected data size: " << data_size
                     << ", expected metadata size: " << metadata_size
                     << ", actual data size: " << object_reader->GetDataSize()
                     << ", actual metadata size: " << object_reader->GetMetadataSize()
                     << ". This is likely due to a race condition."
                     << " We will update the object size and proceed sending the object.";
    local_objects_[object_id].object_info.data_size = 0;
    local_objects_[object_id].object_info.metadata_size = 1;
  }

  PushObjectInternal(object_id,
                     node_id,
                     std::make_shared<ChunkObjectReader>(std::move(object_reader),
                                                         config_.object_chunk_size),
                     /*from_disk=*/false,
                     from_remote);
}

void ObjectManager::PushFromFilesystem(const ObjectID &object_id,
                                       const NodeID &node_id,
                                       const std::string &spilled_url) {
  // SpilledObjectReader::CreateSpilledObjectReader does synchronous IO; schedule it off
  // main thread.
  rpc_service_.post(
      [this, object_id, node_id, spilled_url, chunk_size = config_.object_chunk_size]() {
        auto optional_spilled_object =
            SpilledObjectReader::CreateSpilledObjectReader(spilled_url);
        if (!optional_spilled_object.has_value()) {
          RAY_LOG_EVERY_N_OR_DEBUG(INFO, 100)
              << "Ignoring stale read request for already deleted object: " << object_id;
          return;
        }
        auto chunk_object_reader = std::make_shared<ChunkObjectReader>(
            std::make_shared<SpilledObjectReader>(
                std::move(optional_spilled_object.value())),
            chunk_size);

        // Schedule PushObjectInternal back to main_service as PushObjectInternal access
        // thread unsafe datastructure.
        main_service_->post(
            [this,
             object_id,
             node_id,
             chunk_object_reader = std::move(chunk_object_reader)]() {
              PushObjectInternal(object_id,
                                 node_id,
                                 std::move(chunk_object_reader),
                                 /*from_disk=*/true);
            },
            "ObjectManager.PushLocalSpilledObjectInternal");
      },
      "ObjectManager.CreateSpilledObject");
}

/// RSCODE: SpillRemoteInternal function called from SpillRemote
void ObjectManager::SpillRemoteInternal(const ObjectID &object_id,
                                       const NodeID &node_id,
                                       std::shared_ptr<ChunkObjectReader> chunk_reader,
                                       const std::function<void(ObjectID)> callback) {
  auto rpc_client = GetRemoteSpillRpcClient(node_id);
  if (!rpc_client) {
    RAY_LOG(INFO)
        << "Failed to establish connection for SpillRemote with remote object manager.";
    return;
  }

  RAY_LOG(DEBUG) << "Sending object chunks of " << object_id << " to node " << node_id
                 << ", number of chunks: " << chunk_reader->GetNumChunks()
                 << ", total data size: " << chunk_reader->GetObject().GetObjectSize();

  /// RSCODE: Temporarily increment ref count
  // buffer_pool_store_client_->RemoteSpillIncreaseObjectCount(object_id);

  /// RSTODO: Maybe have spill manager and StartSpillRemote?
  auto spill_id = UniqueID::FromRandom();
  spill_remote_manager_->StartSpillRemote(
      node_id, object_id, chunk_reader->GetNumChunks(), [=](int64_t chunk_id) {
        /// RSTODO: Delete later
        RAY_LOG(INFO) << "For counting purposes: We are about to call rpc_service_ for spill for object: " << object_id;
        remote_spill_rpc_service_.post(
            [=]() {
              // Post to the multithreaded RPC event loop so that data is copied
              // off of the main thread.
              SpillObjectChunk(
                  spill_id,
                  object_id,
                  node_id,
                  chunk_id,
                  rpc_client,
                  [=](const Status &status) {
                    // Post back to the main event loop because the
                    // PushManager is thread-safe.
                    main_service_->post(
                        [this, node_id, object_id, callback]() {
                          spill_remote_manager_->OnChunkComplete(node_id, object_id, callback);
                        },
                        "ObjectManager.SpillRemote");
                  },
                  chunk_reader);
            },
            "ObjectManager.SpillRemote");
      }); 
}

void ObjectManager::PushObjectInternal(const ObjectID &object_id,
                                       const NodeID &node_id,
                                       std::shared_ptr<ChunkObjectReader> chunk_reader,
                                       bool from_disk,
                                       bool from_remote) {
  auto rpc_client = GetRpcClient(node_id);
  if (!rpc_client) {
    // Push is best effort, so do nothing here.
    RAY_LOG(INFO)
        << "Failed to establish connection for Push with remote object manager.";
    return;
  }

  RAY_LOG(DEBUG) << "Sending object chunks of " << object_id << " to node " << node_id
                 << ", number of chunks: " << chunk_reader->GetNumChunks()
                 << ", total data size: " << chunk_reader->GetObject().GetObjectSize();

  auto push_id = UniqueID::FromRandom();
  push_manager_->StartPush(
      node_id, object_id, chunk_reader->GetNumChunks(), [=](int64_t chunk_id) {
        /// RSTODO: Delete later
        RAY_LOG(INFO) << "For counting purposes: We are about to call rpc_service_ for push for object: " << object_id;
        rpc_service_.post(
            [=]() {
              // Post to the multithreaded RPC event loop so that data is copied
              // off of the main thread.
              SendObjectChunk(
                  push_id,
                  object_id,
                  node_id,
                  chunk_id,
                  rpc_client,
                  [=](const Status &status) {
                    // Post back to the main event loop because the
                    // PushManager is thread-safe.
                    main_service_->post(
                        [this, node_id, object_id]() {
                          push_manager_->OnChunkComplete(node_id, object_id);
                        },
                        "ObjectManager.Push");
                  },
                  chunk_reader,
                  from_disk,
                  from_remote);
            },
            "ObjectManager.Push");
      });
}

/// RSCODE: Code to spill chunk
void ObjectManager::SpillObjectChunk(const UniqueID &spill_id,
                                    const ObjectID &object_id,
                                    const NodeID &node_id,
                                    uint64_t chunk_index,
                                    std::shared_ptr<rpc::RemoteSpillClient> rpc_client,
                                    std::function<void(const Status &)> on_complete,
                                    std::shared_ptr<ChunkObjectReader> chunk_reader) {
  double start_time = absl::GetCurrentTimeNanos() / 1e9;
  rpc::SpillRemoteRequest spill_remote_request;
  spill_remote_request.set_spill_id(spill_id.Binary());
  spill_remote_request.set_object_id(object_id.Binary());
  spill_remote_request.set_node_id(node_id.Binary());
  spill_remote_request.mutable_owner_address()->CopyFrom(
      chunk_reader->GetObject().GetOwnerAddress());
  spill_remote_request.set_chunk_index(chunk_index);
  spill_remote_request.set_data_size(chunk_reader->GetObject().GetObjectSize());
  spill_remote_request.set_metadata_size(chunk_reader->GetObject().GetMetadataSize());

  /// RSTODO: Delete later
  RAY_LOG(INFO) << "For counting purposes: We are about to calling SpillObjectChunk for object: " << object_id;

  auto optional_chunk = chunk_reader->GetChunk(chunk_index);
  if (!optional_chunk.has_value()) {
    RAY_LOG(DEBUG) << "Read chunk " << chunk_index << " of object " << object_id
                   << " failed. It may have been evicted.";
    return;
  }
  spill_remote_request.set_data(std::move(optional_chunk.value()));

  num_bytes_pushed_from_plasma_ += spill_remote_request.data().length();

  /// RSTODO: Delete later
  RAY_LOG(INFO) << "Number of bytes pushed from head node: " << num_bytes_pushed_from_plasma_ << " for current object: " << object_id;

  rpc::ClientCallback<rpc::SpillRemoteReply> callback =
      [this, start_time, object_id, node_id, chunk_index, on_complete] (const Status &status, const rpc::SpillRemoteReply &reply) {
        if (!status.ok()) {
          RAY_LOG(WARNING) << "Send object " << object_id << " g " << node_id
                           << " failed due to" << status.message()
                           << ", chunk index: " << chunk_index;
          
          /// RSTODO: Delete this later
          RAY_LOG(INFO) << "Spill to remote failed on object: " << object_id;
        }
        /// RSTODO: Delete this later
        RAY_LOG(INFO) << "Successfully spilled to remote for object: " << object_id;

        double end_time = absl::GetCurrentTimeNanos() / 1e9;
        HandleSendFinished(object_id, node_id, chunk_index, start_time, end_time, status);
        on_complete(status);

        /// RSTODO: Delete this later
        RAY_LOG(INFO) << "Finished calling on_complete for object: " << object_id;
      };

  rpc_client->SpillRemote(spill_remote_request, callback);  
}

void ObjectManager::SendObjectChunk(const UniqueID &push_id,
                                    const ObjectID &object_id,
                                    const NodeID &node_id,
                                    uint64_t chunk_index,
                                    std::shared_ptr<rpc::ObjectManagerClient> rpc_client,
                                    std::function<void(const Status &)> on_complete,
                                    std::shared_ptr<ChunkObjectReader> chunk_reader,
                                    bool from_disk,
                                    bool from_remote) {
  double start_time = absl::GetCurrentTimeNanos() / 1e9;
  rpc::PushRequest push_request;
  // Set request header
  push_request.set_push_id(push_id.Binary());
  push_request.set_object_id(object_id.Binary());
  push_request.mutable_owner_address()->CopyFrom(
      chunk_reader->GetObject().GetOwnerAddress());
  push_request.set_node_id(self_node_id_.Binary());
  push_request.set_data_size(chunk_reader->GetObject().GetObjectSize());
  push_request.set_metadata_size(chunk_reader->GetObject().GetMetadataSize());
  push_request.set_chunk_index(chunk_index);
  push_request.set_from_remote(from_remote);

  /// RSTODO: Delete later
  RAY_LOG(INFO) << "For counting purposes: We are about to calling SendObjectChunk for object: " << object_id;

  // read a chunk into push_request and handle errors.
  auto optional_chunk = chunk_reader->GetChunk(chunk_index);
  if (!optional_chunk.has_value()) {
    RAY_LOG(DEBUG) << "Read chunk " << chunk_index << " of object " << object_id
                   << " failed. It may have been evicted.";
    on_complete(Status::IOError("Failed to read spilled object"));
    return;
  }
  push_request.set_data(std::move(optional_chunk.value()));
  if (from_disk) {
    num_bytes_pushed_from_disk_ += push_request.data().length();
  } else {
    num_bytes_pushed_from_plasma_ += push_request.data().length();
  }

  /// RSTODO: Delete later
  // Jaewon -> At some point "self_node_id" is the remote node and "node_id" is the head node (confirmed through logs)
  // "num_bytes_pushed_from_plasma" also decreases when this happens
  // "num_bytes_pushed_from_plasma" is the "total bytes pushed from head" in head node logs
  // "num_bytes_pushed_from_plasma" is the 
  RAY_LOG(INFO) << "Push from " << self_node_id_ << " to " << node_id << " of object "
                 << object_id;
  RAY_LOG(INFO) << "Total bytes pulled from remote: " << num_bytes_pushed_from_plasma_;

  // record the time cost between send chunk and receive reply
  rpc::ClientCallback<rpc::PushReply> callback =
      [this, start_time, object_id, node_id, chunk_index, on_complete](
          const Status &status, const rpc::PushReply &reply) {
        // TODO: Just print warning here, should we try to resend this chunk?
        if (!status.ok()) {
          RAY_LOG(WARNING) << "Send object " << object_id << " chunk to node " << node_id
                           << " failed due to" << status.message()
                           << ", chunk index: " << chunk_index;
        }
        double end_time = absl::GetCurrentTimeNanos() / 1e9;
        HandleSendFinished(object_id, node_id, chunk_index, start_time, end_time, status);
        on_complete(status);
      };

  rpc_client->Push(push_request, callback);
}

/// Implementation of ObjectManagerServiceHandler
void ObjectManager::HandlePush(const rpc::PushRequest &request,
                               rpc::PushReply *reply,
                               rpc::SendReplyCallback send_reply_callback) {
  ObjectID object_id = ObjectID::FromBinary(request.object_id());
  NodeID node_id = NodeID::FromBinary(request.node_id());

  /// RSTODO: Delete later
  RAY_LOG(INFO) << "For counting purposes: We are calling HandlePush for pull for object: " << object_id;

  // Serialize.
  uint64_t chunk_index = request.chunk_index();
  uint64_t metadata_size = request.metadata_size();
  uint64_t data_size = request.data_size();
  const rpc::Address &owner_address = request.owner_address();
  const std::string &data = request.data();

  /// RSCODE:
  bool from_remote = request.from_remote();

  /// RSTODO: Delete this later
  RAY_LOG(INFO) << "Number bytes pulled from remote in push handler: " << request.data().length();

  bool success = ReceiveObjectChunk(
      node_id, object_id, owner_address, data_size, metadata_size, chunk_index, data, from_remote);
  num_chunks_received_total_++;
  if (!success) {
    num_chunks_received_total_failed_++;
    RAY_LOG(INFO) << "Received duplicate or cancelled chunk at index " << chunk_index
                  << " of object " << object_id << ": overall "
                  << num_chunks_received_total_failed_ << "/"
                  << num_chunks_received_total_ << " failed";
  }

  send_reply_callback(Status::OK(), nullptr, nullptr);
}

/// RSGRPC: (GRPC)
void RemoteSpill::HandleSpillRemote(const rpc::SpillRemoteRequest &request,
                                    rpc::SpillRemoteReply *reply,
                                    rpc::SendReplyCallback send_reply_callback) {
  /// RSTODO: Delete this later
  RAY_LOG(INFO) << "About to write data into remote node memory";
  
  ObjectID object_id = ObjectID::FromBinary(request.object_id());
  NodeID node_id = NodeID::FromBinary(request.node_id());
  uint64_t chunk_index = request.chunk_index();
  uint64_t data_size = request.data_size();
  uint64_t metadata_size = request.metadata_size();
  const std::string &data = request.data();
  const rpc::Address &owner_address = request.owner_address();

  /// RSTODO: Comment out for now
  // auto chunk_status = buffer_pool_.CreateChunk(
  //   object_id, owner_address, data_size, metadata_size, chunk_index);

  // if (chunk_status.ok()) {
  //   // Avoid handling this chunk if it's already being handled by another process.
  //   buffer_pool_.WriteChunk(object_id, data_size, metadata_size, chunk_index, data);
  // }

  /// RSTODO: Delete this later
  // Jaewon -> num_bytes_received_total is increasing the remote node (confirmed through logs)
  // RAY_LOG(INFO) << "ToTr of bytes received: " << num_bytes_received_total_;

  /// RSTODO: Delete this later
  // Jaewon -> "self_node_id" and "node_id" are the same, and "chunk index" is always 0
  // RAY_LOG(INFO) << "SpillToRemote on " << self_node_id_ << " from " << node_id
  //               << " of object " << object_id << " chunk index: " << chunk_index
  //               << ", chunk data size: " << data.size()
  //               << ", object size: " << data_size;

  /// RSTODO: Tony -> potentially delete this later
  RemoteSpillReceiveObjectChunk(node_id, object_id, owner_address, 
                     data_size, metadata_size, chunk_index, 
                     data, true /* from_remote */, true /* from remote spill */);

  send_reply_callback(Status::OK(), nullptr, nullptr);
}

bool RemoteSpill::RemoteSpillReceiveObjectChunk(const NodeID &node_id,
                                                const ObjectID &object_id,
                                                const rpc::Address &owner_address,
                                                uint64_t data_size,
                                                uint64_t metadata_size,
                                                uint64_t chunk_index,
                                                const std::string &data, 
                                                const bool from_remote,
                                                const bool from_remote_spill) {
  // num_bytes_received_total_ += data.size();

  // RAY_LOG(DEBUG) << "ReceiveObjectChunk on " << self_node_id_ << " from " << node_id
  //                << " of object " << object_id << " chunk index: " << chunk_index
  //                << ", chunk data size: " << data.size()
  //                << ", object size: " << data_size;
  
  /// RSTODO: Delete later
  RAY_LOG(INFO) << "ReceiveObjectChunk on " << node_id
                << " of object " << object_id;
  /// RSCODE: current solution: add default from_remote param so that
  /// pull manager checking is not invoked. 
  if (!from_remote) {
    // num_chunks_received_cancelled_++;
    // This object is no longer being actively pulled. Do not create the object.
    return false;
  }
  auto chunk_status = buffer_pool_.CreateChunk(
      object_id, owner_address, data_size, metadata_size, chunk_index);
  if (!from_remote) {
    // num_chunks_received_cancelled_++;
    // This object is no longer being actively pulled. Abort the object. We
    // have to check again here because the pull manager runs in a different
    // thread and the object may have been deactivated right before creating
    // the chunk.
    RAY_LOG(INFO) << "Aborting object creation because it is no longer actively pulled: "
                  << object_id;
    buffer_pool_.AbortCreate(object_id);
    return false;
  }

  /// RSTODO: Maybe delete later
  if (from_remote_spill) {
    if (!received_remote_objects_origin_.contains(object_id)) {
      RAY_LOG(INFO) << "Increasing ref count of object for remote spill for object: " << object_id;
      received_remote_objects_origin_.emplace(object_id, node_id);
      buffer_pool_store_client_->RemoteSpillIncreaseObjectCount(object_id);
    }
  }

  /// RSCODE: Try incrementing object count before write chunk
  // if (from_remote_spill) {
  //   buffer_pool_store_client_->RemoteSpillIncreaseObjectCount(object_id);
  // }

  if (chunk_status.ok()) {
    // Avoid handling this chunk if it's already being handled by another process.
    buffer_pool_.WriteChunk(object_id, data_size, metadata_size, chunk_index, data, from_remote_spill);
    if (from_remote) {
      RAY_LOG(INFO) << "Successfully called WriteChunk on remote object: " << object_id;
    }

    // if (from_remote && !from_remote_spill) {
    //   /// RSTODO: Refector this to only increase ref count once
    //   RAY_LOG(INFO) << "About to inc ref count of pulled object test 1";
    //   if (!pulled_objects_from_remote_.contains(object_id)) {
    //     pulled_objects_from_remote_.emplace(object_id, node_id);
    //     RAY_LOG(INFO) << "About to inc ref count of pulled object test 2";
    //     buffer_pool_store_client_->RemoteSpillIncreaseObjectCount(object_id);
    //   }
    // }
    return true;
  } else {
    // num_chunks_received_failed_due_to_plasma_++;
    RAY_LOG(INFO) << "Error receiving chunk:" << chunk_status.message();
    if (!from_remote && chunk_status.IsOutOfDisk()) {
      // pull_manager_->SetOutOfDisk(object_id);
    }
    return false;
  }
}

bool ObjectManager::ReceiveObjectChunk(const NodeID &node_id,
                                       const ObjectID &object_id,
                                       const rpc::Address &owner_address,
                                       uint64_t data_size,
                                       uint64_t metadata_size,
                                       uint64_t chunk_index,
                                       const std::string &data, 
                                       const bool from_remote,
                                       const bool from_remote_spill) {
  num_bytes_received_total_ += data.size();

  RAY_LOG(DEBUG) << "ReceiveObjectChunk on " << self_node_id_ << " from " << node_id
                 << " of object " << object_id << " chunk index: " << chunk_index
                 << ", chunk data size: " << data.size()
                 << ", object size: " << data_size;
  
  /// RSTODO: Delete later
  RAY_LOG(INFO) << "ReceiveObjectChunk on " << node_id
                << " of object " << object_id;
  /// RSCODE: current solution: add default from_remote param so that
  /// pull manager checking is not invoked. 
  if (!from_remote && !pull_manager_->IsObjectActive(object_id)) {
    num_chunks_received_cancelled_++;
    // This object is no longer being actively pulled. Do not create the object.
    return false;
  }
  auto chunk_status = buffer_pool_.CreateChunk(
      object_id, owner_address, data_size, metadata_size, chunk_index);
  if (!from_remote && !pull_manager_->IsObjectActive(object_id)) {
    num_chunks_received_cancelled_++;
    // This object is no longer being actively pulled. Abort the object. We
    // have to check again here because the pull manager runs in a different
    // thread and the object may have been deactivated right before creating
    // the chunk.
    RAY_LOG(INFO) << "Aborting object creation because it is no longer actively pulled: "
                  << object_id;
    buffer_pool_.AbortCreate(object_id);
    return false;
  }

  // keep track of received objects. 
  // doesn't care about fault tolerance. 
  // if (from_remote_spill) {
  //   if (!received_remote_objects_origin_.contains(object_id)) {
  //     received_remote_objects_origin_.emplace(object_id, node_id);
  //     buffer_pool_store_client_->RemoteSpillIncreaseObjectCount(object_id);
  //   }
  // }

  /// RSCODE: Try incrementing object count before write chunk
  // if (from_remote_spill) {
  //   buffer_pool_store_client_->RemoteSpillIncreaseObjectCount(object_id);
  // }

  if (chunk_status.ok()) {
    // Avoid handling this chunk if it's already being handled by another process.
    buffer_pool_.WriteChunk(object_id, data_size, metadata_size, chunk_index, data);
    if (from_remote) {
      RAY_LOG(INFO) << "Successfully called WriteChunk on remote object: " << object_id;
    }

    // if (from_remote && !from_remote_spill) {
    //   /// RSTODO: Refector this to only increase ref count once
    //   RAY_LOG(INFO) << "About to inc ref count of pulled object test 1";
    //   if (!pulled_objects_from_remote_.contains(object_id)) {
    //     pulled_objects_from_remote_.emplace(object_id, node_id);
    //     RAY_LOG(INFO) << "About to inc ref count of pulled object test 2";
    //     buffer_pool_store_client_->RemoteSpillIncreaseObjectCount(object_id);
    //   }
    // }
    return true;
  } else {
    num_chunks_received_failed_due_to_plasma_++;
    RAY_LOG(INFO) << "Error receiving chunk:" << chunk_status.message();
    if (!from_remote && chunk_status.IsOutOfDisk()) {
      pull_manager_->SetOutOfDisk(object_id);
    }
    return false;
  }
}

void ObjectManager::HandlePull(const rpc::PullRequest &request,
                               rpc::PullReply *reply,
                               rpc::SendReplyCallback send_reply_callback) {
  ObjectID object_id = ObjectID::FromBinary(request.object_id());
  NodeID node_id = NodeID::FromBinary(request.node_id());
  /// RSCODE:
  bool from_remote = request.from_remote();

  RAY_LOG(DEBUG) << "Received pull request from node " << node_id << " for object ["
                 << object_id << "].";
  
  /// RSTODO: Delete this later
  if (from_remote) {
    RAY_LOG(INFO) << "Received remote pull request from node " << node_id << " for object ["
                 << object_id << "].";
  }

  main_service_->post([this, object_id, node_id, from_remote]() { Push(object_id, node_id, from_remote); },
                      "ObjectManager.HandlePull");

  /// RSCODE: Check if request is for remote object
  // If so, add functionality such as freeing the object in the remote node
  if (from_remote) {
    /// RSCODE: Free object here
    /// RSTODO: Might need a different method of freeing
    // std::vector<ObjectID> object_ids;
    // object_ids.push_back(object_id);
    // FreeObjects(object_ids, true);
    // buffer_pool_store_client_->RemoteSpillDecreaseObjectCount(object_id);
  }
    
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void ObjectManager::HandleFreeObjects(const rpc::FreeObjectsRequest &request,
                                      rpc::FreeObjectsReply *reply,
                                      rpc::SendReplyCallback send_reply_callback) {
  std::vector<ObjectID> object_ids;
  for (const auto &e : request.object_ids()) {
    object_ids.emplace_back(ObjectID::FromBinary(e));
  }
  FreeObjects(object_ids, /* local_only */ true);
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void ObjectManager::FreeObjects(const std::vector<ObjectID> &object_ids,
                                bool local_only) {
  /// RSTODO: Delete this later
  RAY_LOG(DEBUG) << "We are freeing objects in memory";
  buffer_pool_.FreeObjects(object_ids);
  if (!local_only) {
    const auto remote_connections = object_directory_->LookupAllRemoteConnections();
    std::vector<std::shared_ptr<rpc::ObjectManagerClient>> rpc_clients;
    for (const auto &connection_info : remote_connections) {
      auto rpc_client = GetRpcClient(connection_info.node_id);
      if (rpc_client != nullptr) {
        rpc_clients.push_back(rpc_client);
      }
    }
    rpc_service_.post(
        [this, object_ids, rpc_clients]() {
          SpreadFreeObjectsRequest(object_ids, rpc_clients);
        },
        "ObjectManager.FreeObjects");
  }
}

void ObjectManager::SpreadFreeObjectsRequest(
    const std::vector<ObjectID> &object_ids,
    const std::vector<std::shared_ptr<rpc::ObjectManagerClient>> &rpc_clients) {
  // This code path should be called from node manager.
  rpc::FreeObjectsRequest free_objects_request;
  for (const auto &e : object_ids) {
    free_objects_request.add_object_ids(e.Binary());
  }

  for (auto &rpc_client : rpc_clients) {
    rpc_client->FreeObjects(free_objects_request,
                            [](const Status &status, const rpc::FreeObjectsReply &reply) {
                              if (!status.ok()) {
                                RAY_LOG(WARNING)
                                    << "Send free objects request failed due to"
                                    << status.message();
                              }
                            });
  }
}

std::shared_ptr<rpc::ObjectManagerClient> ObjectManager::GetRpcClient(
    const NodeID &node_id) {

  /// RSTODO: Delete later
  RAY_LOG(INFO) << "Node id in GetRpcClient: " << node_id;

  auto it = remote_object_manager_clients_.find(node_id);
  if (it == remote_object_manager_clients_.end()) {
    RemoteConnectionInfo connection_info(node_id);
    object_directory_->LookupRemoteConnectionInfo(connection_info);
    if (!connection_info.Connected()) {
      return nullptr;
    }
    auto object_manager_client = std::make_shared<rpc::ObjectManagerClient>(
        connection_info.ip, connection_info.port, client_call_manager_);

    RAY_LOG(DEBUG) << "Get rpc client, address: " << connection_info.ip
                   << ", port: " << connection_info.port
                   << ", local port: " << GetServerPort();

    it = remote_object_manager_clients_.emplace(node_id, std::move(object_manager_client))
             .first;
  }
  return it->second;
}

/// RSCODE:
std::shared_ptr<rpc::RemoteSpillClient> ObjectManager::GetRemoteSpillRpcClient(
    const NodeID &node_id) {

  /// RSTODO: Delete later
  RAY_LOG(INFO) << "Node id in GetRemoteSpillRpcClient: " << node_id;

  auto it = remote_spill_clients_.find(node_id);
  if (it == remote_spill_clients_.end()) {
    RemoteConnectionInfo connection_info(node_id);
    object_directory_->LookupRemoteConnectionInfo(connection_info);
    if (!connection_info.Connected()) {
      return nullptr;
    }
    auto remote_spill_client = std::make_shared<rpc::RemoteSpillClient>(
        connection_info.ip, connection_info.port, client_call_manager_);

    RAY_LOG(DEBUG) << "Get rpc client, address: " << connection_info.ip
                   << ", port: " << connection_info.port
                   << ", local port: " << GetServerPort();

    it = remote_spill_clients_.emplace(node_id, std::move(remote_spill_client))
             .first;
  }
  return it->second;
}

/// RSCODE:
// std::shared_ptr<rpc::RemoteSpillClient> ObjectManager::GetRemoteSpillRpcClient(
//     const NodeID &node_id) {

//   /// RSTODO: Delete later
//   RAY_LOG(INFO) << "Node id in GetRemoteSpillRpcClient: " << node_id;

//   auto it = remote_object_manager_clients_.find(node_id);
//   if (it == remote_object_manager_clients_.end()) {
//     RemoteConnectionInfo connection_info(node_id);
//     object_directory_->LookupRemoteConnectionInfo(connection_info);
//     if (!connection_info.Connected()) {
//       return nullptr;
//     }
//     auto remote_spill_client = std::make_shared<rpc::RemoteSpillClient>(
//         connection_info.ip, connection_info.port, client_call_manager_);

//     RAY_LOG(DEBUG) << "Get rpc client, address: " << connection_info.ip
//                    << ", port: " << connection_info.port
//                    << ", local port: " << GetServerPort();

//     it = remote_object_manager_clients_.emplace(node_id, std::move(remote_spill_client))
//              .first;
//   }
//   return it->second;
// }

std::string ObjectManager::DebugString() const {
  std::stringstream result;
  result << "ObjectManager:";
  result << "\n- num local objects: " << local_objects_.size();
  result << "\n- num unfulfilled push requests: " << unfulfilled_push_requests_.size();
  result << "\n- num object pull requests: " << pull_manager_->NumObjectPullRequests();
  result << "\n- num chunks received total: " << num_chunks_received_total_;
  result << "\n- num chunks received failed (all): " << num_chunks_received_total_failed_;
  result << "\n- num chunks received failed / cancelled: "
         << num_chunks_received_cancelled_;
  result << "\n- num chunks received failed / plasma error: "
         << num_chunks_received_failed_due_to_plasma_;
  result << "\nEvent stats:" << rpc_service_.stats().StatsString();
  /// RSCODE:
  result << "\n" << spill_remote_manager_->DebugString();
  result << "\n" << push_manager_->DebugString();
  result << "\n" << object_directory_->DebugString();
  result << "\n" << buffer_pool_.DebugString();
  result << "\n" << pull_manager_->DebugString();
  return result.str();
}

void ObjectManager::RecordMetrics() {
  pull_manager_->RecordMetrics();
  push_manager_->RecordMetrics();
  // used_memory_ includes the fallback allocation, so we should add it again here
  // to calculate the exact available memory.
  stats::ObjectStoreAvailableMemory().Record(
      config_.object_store_memory - used_memory_ +
      plasma::plasma_store_runner->GetFallbackAllocated());
  // Subtract fallback allocated memory. It is tracked separately by
  // `ObjectStoreFallbackMemory`.
  stats::ObjectStoreUsedMemory().Record(
      used_memory_ - plasma::plasma_store_runner->GetFallbackAllocated());
  stats::ObjectStoreFallbackMemory().Record(
      plasma::plasma_store_runner->GetFallbackAllocated());
  stats::ObjectStoreLocalObjects().Record(local_objects_.size());
  stats::ObjectManagerPullRequests().Record(pull_manager_->NumObjectPullRequests());

  ray::stats::STATS_object_manager_bytes.Record(num_bytes_pushed_from_plasma_,
                                                "PushedFromLocalPlasma");
  ray::stats::STATS_object_manager_bytes.Record(num_bytes_pushed_from_disk_,
                                                "PushedFromLocalDisk");
  ray::stats::STATS_object_manager_bytes.Record(num_bytes_received_total_, "Received");

  ray::stats::STATS_object_manager_received_chunks.Record(num_chunks_received_total_,
                                                          "Total");
  ray::stats::STATS_object_manager_received_chunks.Record(
      num_chunks_received_total_failed_, "FailedTotal");
  ray::stats::STATS_object_manager_received_chunks.Record(num_chunks_received_cancelled_,
                                                          "FailedCancelled");
  ray::stats::STATS_object_manager_received_chunks.Record(
      num_chunks_received_failed_due_to_plasma_, "FailedPlasmaFull");
}

void ObjectManager::FillObjectStoreStats(rpc::GetNodeStatsReply *reply) const {
  auto stats = reply->mutable_store_stats();
  stats->set_object_store_bytes_used(used_memory_);
  stats->set_object_store_bytes_fallback(
      plasma::plasma_store_runner->GetFallbackAllocated());
  stats->set_object_store_bytes_avail(config_.object_store_memory);
  stats->set_num_local_objects(local_objects_.size());
  stats->set_consumed_bytes(plasma::plasma_store_runner->GetConsumedBytes());
  stats->set_object_pulls_queued(pull_manager_->HasPullsQueued());
}

void ObjectManager::Tick(const boost::system::error_code &e) {
  RAY_CHECK(!e) << "The raylet's object manager has failed unexpectedly with error: " << e
                << ". Please file a bug report on here: "
                   "https://github.com/ray-project/ray/issues";

  // Request the current available memory from the object
  // store.
  plasma::plasma_store_runner->GetAvailableMemoryAsync([this](size_t available_memory) {
    main_service_->post(
        [this, available_memory]() {
          pull_manager_->UpdatePullsBasedOnAvailableMemory(available_memory);
        },
        "ObjectManager.UpdateAvailableMemory");
  });

  pull_manager_->Tick();

  auto interval = boost::posix_time::milliseconds(config_.timer_freq_ms);
  pull_retry_timer_.expires_from_now(interval);
  pull_retry_timer_.async_wait([this](const boost::system::error_code &e) { Tick(e); });
}

}  // namespace ray
