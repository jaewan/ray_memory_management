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

#include "ray/raylet/local_object_manager.h"

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/stats/metric_defs.h"
#include "ray/util/util.h"

namespace ray {

namespace raylet {

void LocalObjectManager::PinObjectsAndWaitForFree(
    const std::vector<ObjectID> &object_ids,
    std::vector<std::unique_ptr<RayObject>> &&objects,
    const rpc::Address &owner_address) {
  RAY_LOG(DEBUG) << "[JAE_DEBUG] [" << __func__ << "] Called :" << object_ids.size();
  static const bool enable_eagerSpill = RayConfig::instance().enable_EagerSpill();
  for (size_t i = 0; i < object_ids.size(); i++) {
    const auto &object_id = object_ids[i];
    if (objects_waiting_for_free_.count(object_id)) {
      continue;
    }
    objects_waiting_for_free_.insert(object_id);

    auto &object = objects[i];
    if (object == nullptr) {
      RAY_LOG(ERROR) << "Plasma object " << object_id
                     << " was evicted before the raylet could pin it.";
      continue;
    }

    const auto inserted =
        local_objects_.emplace(object_id, std::make_pair<>(owner_address, false));
    if (inserted.second) {
      // This is the first time we're pinning this object.
      RAY_LOG(DEBUG) << "Pinning object " << object_id;
      pinned_objects_size_ += object->GetSize();
      pinned_objects_.emplace(object_id, std::move(object));
      pinned_objects_prioity_[objectID_to_priority_[object_id]].insert(object_id);
    } else {
      auto original_worker_id =
          WorkerID::FromBinary(inserted.first->second.first.worker_id());
      auto new_worker_id = WorkerID::FromBinary(owner_address.worker_id());
      if (original_worker_id != new_worker_id) {
        // TODO(swang): Handle this case. We should use the new owner address
        // and object copy.
        RAY_LOG(WARNING)
            << "Received PinObjects request from a different owner " << new_worker_id
            << " from the original " << original_worker_id << ". Object " << object_id
            << " may get freed while the new owner still has the object in scope.";
      }
      continue;
    }

    // Create a object eviction subscription message.
    auto wait_request = std::make_unique<rpc::WorkerObjectEvictionSubMessage>();
    wait_request->set_object_id(object_id.Binary());
    wait_request->set_intended_worker_id(owner_address.worker_id());
    rpc::Address subscriber_address;
    subscriber_address.set_raylet_id(self_node_id_.Binary());
    subscriber_address.set_ip_address(self_node_address_);
    subscriber_address.set_port(self_node_port_);
    wait_request->mutable_subscriber_address()->CopyFrom(subscriber_address);

    // If the subscription succeeds, register the subscription callback.
    // Callback is invoked when the owner publishes the object to evict.
    auto subscription_callback = [this, owner_address](const rpc::PubMessage &msg) {
      RAY_CHECK(msg.has_worker_object_eviction_message());
      const auto object_eviction_msg = msg.worker_object_eviction_message();
      const auto object_id = ObjectID::FromBinary(object_eviction_msg.object_id());
      ReleaseFreedObject(object_id);
      core_worker_subscriber_->Unsubscribe(
          rpc::ChannelType::WORKER_OBJECT_EVICTION, owner_address, object_id.Binary());
    };

    // Callback that is invoked when the owner of the object id is dead.
    auto owner_dead_callback = [this, owner_address](const std::string &object_id_binary,
                                                     const Status &) {
      const auto object_id = ObjectID::FromBinary(object_id_binary);
      ReleaseFreedObject(object_id);
    };

    auto sub_message = std::make_unique<rpc::SubMessage>();
    sub_message->mutable_worker_object_eviction_message()->Swap(wait_request.get());

    RAY_CHECK(core_worker_subscriber_->Subscribe(std::move(sub_message),
                                                 rpc::ChannelType::WORKER_OBJECT_EVICTION,
                                                 owner_address,
                                                 object_id.Binary(),
                                                 /*subscribe_done_callback=*/nullptr,
                                                 subscription_callback,
                                                 owner_dead_callback));
  }
  if(enable_eagerSpill){
	RAY_LOG(DEBUG) << "[JAE_DEBUG] [" << __func__ << "] Calling EagerSpill()";
    EagerSpill();
  }
}

void LocalObjectManager::EagerSpill() {
	/*
  if (eager_spill_running_ || !objects_pending_eager_spill_.empty()
		  || RayConfig::instance().object_spilling_config().empty()) {
		  */
  if (eager_spill_running_ ||
		  RayConfig::instance().object_spilling_config().empty()) {
    return;
  }
  eager_spill_running_ = true;

  EagerSpillObjectsOfSize(min_spilling_size_);

  eager_spill_running_ = false;
}

double LocalObjectManager::GetSpillTime(){
  //Default 3 seconds
  //TODO(Jae) Set according to objects in the object store
  return RayConfig::instance().spill_wait_time();
}

static inline double distribution(double i, double b){
  return pow(((b-1)/b),(b-i))*(1/(b*(1-pow(1-(1/b),b))));
}

bool LocalObjectManager::SkiRental(){
  static uint64_t ski_rental_timestamp;
  static bool ski_rental_started = false;
  uint64_t current_timestamp = std::chrono::duration_cast<std::chrono::milliseconds>
							 (std::chrono::system_clock::now().time_since_epoch()).count();
  RAY_LOG(DEBUG) << "[JAE_DEBUG] SkiRental Called ";
  if(!ski_rental_started){
    ski_rental_started = true;
    ski_rental_timestamp = current_timestamp;
	return false;
  }
  if((current_timestamp - ski_rental_timestamp) >= GetSpillTime()){
	ski_rental_started = false;
  RAY_LOG(DEBUG) << "[JAE_DEBUG] SkiRental Return true ";
	return true;
  }
  return false;
  /*

  double diff = (double)(current_timestamp - ski_rental_timestamp);
  double avg = (double)((diff + last_called - ski_rental_timestamp)/2);
  double b = GetSpillTime();
  if(diff >= b)
    return true;
  double n = current_timestamp - last_called;
  bool spill = false;

  std::random_device rd;
  std::mt19937 gen(rd());
  double p = distribution(avg, b);
  std::discrete_distribution<> distrib({1-p, p});
  //SkiRental() is not called every timestep, thus compensate
  for(double i=0; i<n; i++){
    spill |= distrib(gen);
  }

  last_called = current_timestamp;
  if(spill)
    ski_rental_started = false;
  return spill;
  */
}

void LocalObjectManager::ReleaseFreedObject(const ObjectID &object_id) {
  static const bool enable_eagerSpill = RayConfig::instance().enable_EagerSpill();
  static const bool enable_blocktasks_spill = RayConfig::instance().enable_BlockTasksSpill();
  // Only free the object if it is not already freed.
  if(enable_eagerSpill && enable_blocktasks_spill){
    if (!SkiRental()){
      RAY_LOG(DEBUG) << "[JAE_DEBUG] Wait for Ski-Rental ";
	  return;
	}
  }
  auto it = local_objects_.find(object_id);
  if (it == local_objects_.end() || it->second.second) {
    return;
  }
  // Mark the object as freed. NOTE(swang): We have to mark this instead of
  // deleting the entry immediately in case the object is currently being
  // spilled. In that case, we should process the free event once the object
  // spill is complete.
  it->second.second = true;

  RAY_LOG(DEBUG) << "Unpinning object " << object_id;
  // The object should be in one of these states: pinned, spilling, or spilled.
  RAY_CHECK((pinned_objects_.count(object_id) > 0) ||
            (spilled_objects_url_.count(object_id) > 0) ||
            (objects_pending_spill_.count(object_id)) > 0 ||
			(objects_pending_eager_spill_.count(object_id)) > 0 ||
			(eager_spilled_objects_.count(object_id)));
  if (pinned_objects_.count(object_id)) {
	RemovePinnedObjects(object_id, pinned_objects_[object_id]->GetSize());
    local_objects_.erase(it);
  } else {
    // If the object is being spilled or is already spilled, then we will clean
    // up the local_objects_ entry once the spilled copy has been
    // freed.
    spilled_object_pending_delete_.push(object_id);
  }

  if(eager_spilled_objects_.count(object_id)) {
	auto entry = spilled_objects_url_.find(object_id);
    RAY_CHECK(entry != spilled_objects_url_.end());
	auto object_url = entry->second;
    RAY_LOG(DEBUG) << "[JAE_DEBUG] delete eager spilled object " << object_id;
    std::vector<std::string> object_url_to_delete;
    object_url_to_delete.emplace_back(object_url);
    eager_spilled_objects_.erase(object_id);

    //DeleteSpilledObjects(object_url_to_delete);

  }else if(objects_pending_eager_spill_.contains(object_id)){
    RAY_LOG(DEBUG) << "[JAE_DEBUG] object " << object_id << "was freed during eager spill";
	freed_during_eager_spill_.emplace(object_id);
  }

  // Try to evict all copies of the object from the cluster.
  if (free_objects_period_ms_ >= 0) {
    objects_to_free_.push_back(object_id);
  }
  if (objects_to_free_.size() == free_objects_batch_size_ ||
      free_objects_period_ms_ == 0) {
    FlushFreeObjects();
  }
}

void LocalObjectManager::FlushFreeObjects() {
  if (!objects_to_free_.empty()) {
    RAY_LOG(DEBUG) << "Freeing " << objects_to_free_.size() << " out-of-scope objects";
    on_objects_freed_(objects_to_free_);
    objects_to_free_.clear();
  }
  RAY_LOG(DEBUG) << "[JAE_DEBUG] from FlushFreeObjects calling ProcessSpilledObjectsDeleteQueue for:" << spilled_object_pending_delete_.size();
  ProcessSpilledObjectsDeleteQueue(free_objects_batch_size_);
  last_free_objects_at_ms_ = current_time_ms();
}

void LocalObjectManager::SpillObjectUptoMaxThroughput() {
  const bool enable_eagerSpill = RayConfig::instance().enable_EagerSpill();
  RAY_CHECK(!enable_eagerSpill);
  if (RayConfig::instance().object_spilling_config().empty()) {
    return;
  }

  RAY_LOG(DEBUG) << "[JAE_DEBUG] [" << __func__ << "] Called";
  // Spill as fast as we can using all our spill workers.
  bool can_spill_more = true;
  while (can_spill_more) {
    if (!SpillObjectsOfSize(min_spilling_size_)) {
      break;
    }
    {
      absl::MutexLock lock(&mutex_);
      can_spill_more = num_active_workers_ < max_active_workers_;
    }
  }
}

bool LocalObjectManager::IsSpillingInProgress() {
  absl::MutexLock lock(&mutex_);
  return num_active_workers_ > 0;
}

bool LocalObjectManager::EagerSpillObjectsOfSize(int64_t num_bytes_to_spill) {
  int64_t bytes_to_spill = 0;
  // Write to disk from reverse order
  auto it_expired = expired_objects_.begin();
  auto it_priority = pinned_objects_prioity_.rbegin();
  std::vector<ObjectID> objects_to_eager_spill;
  int64_t counts = 0;
  // Eager-spill from expired objects first and scan through priority
  RAY_LOG(DEBUG) << "Choosing objects to eager-spill from expired(Ski-rental) of total size " << num_bytes_to_spill;
  while (bytes_to_spill <= num_bytes_to_spill && it_expired != expired_objects_.end() &&
         counts < max_fused_object_count_) {
    auto &obj_id = *it_expired;
    if (is_plasma_object_eager_spillable_(obj_id)) {
      bytes_to_spill += pinned_objects_[obj_id]->GetSize();
      objects_to_eager_spill.push_back(obj_id);
    }else{
	  expired_objects_.erase(it_expired);
	}
    it_expired++;
    counts += 1;
  }
  RAY_LOG(DEBUG) << "Choosing objects to eager-spill from low priority of total size " << num_bytes_to_spill;
  while (bytes_to_spill <= num_bytes_to_spill && it_priority != pinned_objects_prioity_.rend() &&
         counts < max_fused_object_count_) {
	auto it = it_priority->second.begin();
    while(bytes_to_spill <= num_bytes_to_spill && it!= it_priority->second.end() &&
         counts < max_fused_object_count_){
      const ObjectID &obj_id = *it;
      if (is_plasma_object_eager_spillable_(obj_id)) {
        bytes_to_spill += pinned_objects_[obj_id]->GetSize();
        objects_to_eager_spill.push_back(obj_id);
      }
	  it++;
	  counts++;
	}
    it_priority++;
  }
  if(it_priority == pinned_objects_prioity_.rend() && it_expired == expired_objects_.end() &&
     bytes_to_spill < num_bytes_to_spill && !objects_pending_eager_spill_.empty()) {
    // We have gone through all spillable objects but we have not yet reached
    // the minimum bytes to eager spill and we are already spilling other objects.
    // Let those eager spill requests finish before we try to eager spill the current
    // objects. This gives us some time to decide whether we really need to
    // eager spill the current objects or if we can afford to wait for additional
    // objects to fuse with.
    return false;
  }
  if (!objects_to_eager_spill.empty()) {
    RAY_LOG(DEBUG) << "Eager Spilling objects of total size " << bytes_to_spill
                   << " num objects " << objects_to_eager_spill.size();
    auto start_time = absl::GetCurrentTimeNanos();
    EagerSpillObjectsInternal(objects_to_eager_spill, [this, bytes_to_spill, objects_to_eager_spill,
                                            start_time](const Status &status) {
	 //TODO(JAE) this is weird. Here is what causes seg fault
      if (!status.ok()) {
        RAY_LOG(DEBUG) << "Failed to eager spill objects: " << status.ToString();
      } else {
        auto now = absl::GetCurrentTimeNanos();
        RAY_LOG(DEBUG) << "Eager Spilled " << bytes_to_spill << " bytes in "
                       << (now - start_time) / 1e6 << "ms";
        spilled_bytes_total_ += bytes_to_spill;
        spilled_objects_total_ += objects_to_eager_spill.size();
        // Adjust throughput timing to account for concurrent spill operations.
        spill_time_total_s_ += (now - std::max(start_time, last_spill_finish_ns_)) / 1e9;
        if (now - last_spill_log_ns_ > 1e9) {
          last_spill_log_ns_ = now;
          std::stringstream msg;
          // Keep :info_message: in sync with LOG_PREFIX_INFO_MESSAGE in
          // ray_constants.py.
          msg << ":info_message:Eager Spilled "
              << static_cast<int>(spilled_bytes_total_ / (1024 * 1024)) << " MiB, "
              << spilled_objects_total_ << " objects, write throughput "
              << static_cast<int>(spilled_bytes_total_ / (1024 * 1024) /
                                  spill_time_total_s_)
              << " MiB/s.";
          if (next_spill_error_log_bytes_ > 0 &&
              spilled_bytes_total_ >= next_spill_error_log_bytes_) {
            // Add an advisory the first time this is logged.
            if (next_spill_error_log_bytes_ ==
                RayConfig::instance().verbose_spill_logs()) {
              msg << " Set RAY_verbose_spill_logs=0 to disable this message.";
            }
            // Exponential backoff on the spill messages.
            next_spill_error_log_bytes_ *= 2;
            RAY_LOG(ERROR) << msg.str();
          } else {
            RAY_LOG(INFO) << msg.str();
          }
        }
        last_spill_finish_ns_ = now;
      }
    });
    return true;
  }
  return false;
}

bool LocalObjectManager::SpillObjectsOfSize(int64_t num_bytes_to_spill) {
  const bool enable_eagerSpill = RayConfig::instance().enable_EagerSpill();
  RAY_CHECK(!enable_eagerSpill);
  if (RayConfig::instance().object_spilling_config().empty()) {
    return false;
  }

  int64_t bytes_to_spill = 0;
  auto it = pinned_objects_.begin();
  std::vector<ObjectID> objects_to_spill;
  int64_t counts = 0;
  while (it != pinned_objects_.end() && counts < max_fused_object_count_) {
    if (is_plasma_object_spillable_(it->first)) {
      bytes_to_spill += it->second->GetSize();
      objects_to_spill.push_back(it->first);
    }
    it++;
    counts += 1;
  }
  if (objects_to_spill.empty()) {
    return false;
  }

  if (it == pinned_objects_.end() && bytes_to_spill < num_bytes_to_spill &&
      !objects_pending_spill_.empty()) {
    // We have gone through all spillable objects but we have not yet reached
    // the minimum bytes to spill and we are already spilling other objects.
    // Let those spill requests finish before we try to spill the current
    // objects. This gives us some time to decide whether we really need to
    // spill the current objects or if we can afford to wait for additional
    // objects to fuse with.
    return false;
  }
  RAY_LOG(DEBUG) << "Spilling objects of total size " << bytes_to_spill << " num objects "
                 << objects_to_spill.size();
  auto start_time = absl::GetCurrentTimeNanos();
  SpillObjectsInternal(
      objects_to_spill,
      [this, bytes_to_spill, objects_to_spill, start_time](const Status &status) {
        if (!status.ok()) {
          RAY_LOG(DEBUG) << "Failed to spill objects: " << status.ToString();
        } else {
          auto now = absl::GetCurrentTimeNanos();
          RAY_LOG(DEBUG) << "Spilled " << bytes_to_spill << " bytes in "
                         << (now - start_time) / 1e6 << "ms";
          spilled_bytes_total_ += bytes_to_spill;
          spilled_objects_total_ += objects_to_spill.size();
          // Adjust throughput timing to account for concurrent spill operations.
          spill_time_total_s_ +=
              (now - std::max(start_time, last_spill_finish_ns_)) / 1e9;
          if (now - last_spill_log_ns_ > 1e9) {
            last_spill_log_ns_ = now;
            std::stringstream msg;
            // Keep :info_message: in sync with LOG_PREFIX_INFO_MESSAGE in
            // ray_constants.py.
            msg << ":info_message:Spilled "
                << static_cast<int>(spilled_bytes_total_ / (1024 * 1024)) << " MiB, "
                << spilled_objects_total_ << " objects, write throughput "
                << static_cast<int>(spilled_bytes_total_ / (1024 * 1024) /
                                    spill_time_total_s_)
                << " MiB/s.";
            if (next_spill_error_log_bytes_ > 0 &&
                spilled_bytes_total_ >= next_spill_error_log_bytes_) {
              // Add an advisory the first time this is logged.
              if (next_spill_error_log_bytes_ ==
                  RayConfig::instance().verbose_spill_logs()) {
                msg << " Set RAY_verbose_spill_logs=0 to disable this message.";
              }
              // Exponential backoff on the spill messages.
              next_spill_error_log_bytes_ *= 2;
              RAY_LOG(ERROR) << msg.str();
            } else {
              RAY_LOG(INFO) << msg.str();
            }
          }
          last_spill_finish_ns_ = now;
        }
      });
  return true;
}

void LocalObjectManager::SpillObjects(const std::vector<ObjectID> &object_ids,
                                      std::function<void(const ray::Status &)> callback) {
  const bool enable_eagerSpill = RayConfig::instance().enable_EagerSpill();
  RAY_CHECK(!enable_eagerSpill);
  SpillObjectsInternal(object_ids, callback);
}

void LocalObjectManager::RemovePinnedObjects(const ObjectID &object_id, size_t size){
  const auto it = pinned_objects_.find(object_id);
  if(it != pinned_objects_.end()){
    pinned_objects_size_ -= size;
    pinned_objects_.erase(object_id);
  }else{
	RAY_LOG(DEBUG) << "[JAE_DEBUG] [" << __func__ << "] Error pinned_object not found:";
  }
  const auto obj2pri_it = objectID_to_priority_.find(object_id);
  if(obj2pri_it != objectID_to_priority_.end()){
    pinned_objects_prioity_[obj2pri_it->second].erase(object_id);
    if(pinned_objects_prioity_[obj2pri_it->second].size() == 0){
      pinned_objects_prioity_.erase(obj2pri_it->second);
    }
  }
}

void LocalObjectManager::EagerSpillObjectsInternal(
    const std::vector<ObjectID> &object_ids,
    std::function<void(const ray::Status &)> callback) {
  std::vector<ObjectID> objects_to_spill;
  // Filter for the objects that can be spilled.
  for (const auto &id : object_ids) {
    // We should not spill an object that we are not the primary copy for, or
    // objects that are already being spilled.
    if (pinned_objects_.count(id) == 0 && objects_pending_eager_spill_.count(id) == 0) {
      if (callback) {
        callback(
            Status::Invalid("Requested spill for object that is not marked as "
                            "the primary copy."));
      }
      return;
    }

    // Add objects that we are the primary copy for, and that we are not
    // already spilling.
    auto it = pinned_objects_.find(id);
    if (it != pinned_objects_.end()) {
      RAY_LOG(DEBUG) << "Eager Spilling object " << id;
      objects_to_spill.push_back(id);
      store_object_count_(id, true, false);

      // Move a pinned object to the pending spill object.
      size_t object_size = it->second->GetSize();
      num_bytes_pending_spill_ += object_size;
      objects_pending_eager_spill_[id] = std::move(it->second);

	  RemovePinnedObjects(id, object_size);
    }
  }

  if (objects_to_spill.empty()) {
    if (callback) {
      callback(Status::Invalid("All objects are already being spilled."));
    }
    return;
  }
  {
    absl::MutexLock lock(&mutex_);
    num_active_workers_ += 1;
  }
  io_worker_pool_.PopSpillWorker(
      [this, objects_to_spill, callback](std::shared_ptr<WorkerInterface> io_worker) {
        rpc::SpillObjectsRequest request;
        std::vector<ObjectID> requested_objects_to_spill;
        for (const auto &object_id : objects_to_spill) {
          RAY_LOG(DEBUG) << "Sending eager spill request for object " << object_id;
		  if(freed_during_eager_spill_.contains(object_id)){
            RAY_LOG(DEBUG) << "[JAE_DEEBUG] freed before eager spill";
		    continue;
		  }
          auto it = objects_pending_eager_spill_.find(object_id);
          RAY_CHECK(it != objects_pending_eager_spill_.end()) << " object:" << object_id;
          auto freed_it = local_objects_.find(object_id);
          // If the object hasn't already been freed, spill it.
          if (freed_it == local_objects_.end() || freed_it->second.second) {
            num_bytes_pending_spill_ -= it->second->GetSize();
            objects_pending_eager_spill_.erase(it);
          } else {
            auto ref = request.add_object_refs_to_spill();
            ref->set_object_id(object_id.Binary());
            ref->mutable_owner_address()->CopyFrom(freed_it->second.first);
            RAY_LOG(DEBUG) << "Sending spill request for object " << object_id;
            requested_objects_to_spill.push_back(object_id);
          }
        }

        if (request.object_refs_to_spill_size() == 0) {
          {
            absl::MutexLock lock(&mutex_);
            num_active_workers_ -= 1;
          }
          io_worker_pool_.PushSpillWorker(io_worker);
          callback(Status::OK());
          return;
        }

		if(requested_objects_to_spill.size()){
          io_worker->rpc_client()->SpillObjects(
            request, [this, requested_objects_to_spill, callback, io_worker](
                         const ray::Status &status, const rpc::SpillObjectsReply &r) {
              {
                absl::MutexLock lock(&mutex_);
                num_active_workers_ -= 1;
              }
              io_worker_pool_.PushSpillWorker(io_worker);
              size_t num_objects_spilled = status.ok() ? r.spilled_objects_url_size() : 0;
              // Object spilling is always done in the order of the request.
              // For example, if an object succeeded, it'll guarentee that all objects
              // before this will succeed.
              RAY_CHECK(num_objects_spilled <= requested_objects_to_spill.size());
              for (size_t i = num_objects_spilled; i != requested_objects_to_spill.size();
					  ++i) {
                const auto &object_id = requested_objects_to_spill[i];
                auto it = objects_pending_eager_spill_.find(object_id);
                RAY_CHECK(it != objects_pending_eager_spill_.end());
				RAY_LOG(DEBUG) << "[JAE_DEBUG] eager spilling error for obj: " << object_id;
                pinned_objects_size_ += it->second->GetSize();
                pinned_objects_.emplace(object_id, std::move(it->second));
                pinned_objects_prioity_[objectID_to_priority_[object_id]].insert(object_id);
                objects_pending_eager_spill_.erase(it);
              }

              if (!status.ok()) {
                for (size_t i = num_objects_spilled; i != requested_objects_to_spill.size(); ++i) {
                  const auto &object_id = requested_objects_to_spill[i];
				  store_object_count_(object_id, false, true);
				}
                RAY_LOG(ERROR) << "Failed to send object eager spilling request: "
                               << status.ToString();
              } else {
                OnObjectEagerSpilled(requested_objects_to_spill, r);
              }
              if (callback) {
                callback(status);
              }
          });
		}
      });
}

void LocalObjectManager::SpillObjectsInternal(
    const std::vector<ObjectID> &object_ids,
    std::function<void(const ray::Status &)> callback) {
  const bool enable_eagerSpill = RayConfig::instance().enable_EagerSpill();
  RAY_CHECK(!enable_eagerSpill);
  std::vector<ObjectID> objects_to_spill;
  // Filter for the objects that can be spilled.
  for (const auto &id : object_ids) {
    // We should not spill an object that we are not the primary copy for, or
    // objects that are already being spilled.
    if (pinned_objects_.count(id) == 0 && objects_pending_spill_.count(id) == 0) {
      if (callback) {
        callback(
            Status::Invalid("Requested spill for object that is not marked as "
                            "the primary copy."));
      }
      return;
    }

    // Add objects that we are the primary copy for, and that we are not
    // already spilling.
    auto it = pinned_objects_.find(id);
    if (it != pinned_objects_.end()) {
      RAY_LOG(DEBUG) << "Spilling object " << id;
      objects_to_spill.push_back(id);

      // Move a pinned object to the pending spill object.
      auto object_size = it->second->GetSize();
      num_bytes_pending_spill_ += object_size;
      objects_pending_spill_[id] = std::move(it->second);

      pinned_objects_size_ -= object_size;
      pinned_objects_.erase(it);
    }
  }

  if (objects_to_spill.empty()) {
    if (callback) {
      callback(Status::Invalid("All objects are already being spilled."));
    }
    return;
  }
  {
    absl::MutexLock lock(&mutex_);
    num_active_workers_ += 1;
  }
  io_worker_pool_.PopSpillWorker(
      [this, objects_to_spill, callback](std::shared_ptr<WorkerInterface> io_worker) {
        rpc::SpillObjectsRequest request;
        std::vector<ObjectID> requested_objects_to_spill;
        for (const auto &object_id : objects_to_spill) {
          auto it = objects_pending_spill_.find(object_id);
          RAY_CHECK(it != objects_pending_spill_.end());
          auto freed_it = local_objects_.find(object_id);
          // If the object hasn't already been freed, spill it.
          if (freed_it == local_objects_.end() || freed_it->second.second) {
            num_bytes_pending_spill_ -= it->second->GetSize();
            objects_pending_spill_.erase(it);
          } else {
            auto ref = request.add_object_refs_to_spill();
            ref->set_object_id(object_id.Binary());
            ref->mutable_owner_address()->CopyFrom(freed_it->second.first);
            RAY_LOG(DEBUG) << "Sending spill request for object " << object_id;
            requested_objects_to_spill.push_back(object_id);
          }
        }

        if (request.object_refs_to_spill_size() == 0) {
          {
            absl::MutexLock lock(&mutex_);
            num_active_workers_ -= 1;
          }
          io_worker_pool_.PushSpillWorker(io_worker);
          callback(Status::OK());
          return;
        }

        io_worker->rpc_client()->SpillObjects(
            request,
            [this, requested_objects_to_spill, callback, io_worker](
                const ray::Status &status, const rpc::SpillObjectsReply &r) {
              {
                absl::MutexLock lock(&mutex_);
                num_active_workers_ -= 1;
              }
              io_worker_pool_.PushSpillWorker(io_worker);
              size_t num_objects_spilled = status.ok() ? r.spilled_objects_url_size() : 0;
              // Object spilling is always done in the order of the request.
              // For example, if an object succeeded, it'll guarentee that all objects
              // before this will succeed.
              RAY_CHECK(num_objects_spilled <= requested_objects_to_spill.size());
              for (size_t i = num_objects_spilled; i != requested_objects_to_spill.size();
                   ++i) {
                const auto &object_id = requested_objects_to_spill[i];
                auto it = objects_pending_spill_.find(object_id);
                RAY_CHECK(it != objects_pending_spill_.end());
                pinned_objects_size_ += it->second->GetSize();
                num_bytes_pending_spill_ -= it->second->GetSize();
                pinned_objects_.emplace(object_id, std::move(it->second));
                objects_pending_spill_.erase(it);
              }

              if (!status.ok()) {
                RAY_LOG(ERROR) << "Failed to send object spilling request: "
                               << status.ToString();
              } else {
                OnObjectSpilled(requested_objects_to_spill, r);
              }
              if (callback) {
                callback(status);
              }
            });
      });

  // Deleting spilled objects can fall behind when there is a lot
  // of concurrent spilling and object frees. Clear the queue here
  // if needed.
  if (spilled_object_pending_delete_.size() >= free_objects_batch_size_) {
    RAY_LOG(DEBUG) << "[JAE_DEBUG] calling ProcessSpilledObjectsDeleteQueue for:" << spilled_object_pending_delete_.size();
    ProcessSpilledObjectsDeleteQueue(free_objects_batch_size_);
  }
}

bool LocalObjectManager::DeleteEagerSpilledObjects(bool delete_all){
	bool ret = false;
	if(delete_all){
	  for(auto &eager_spilled_it : eager_spilled_objects_){
        const ObjectID &object_id = eager_spilled_it.first;
	    if(!is_plasma_object_spillable_(object_id)){
          RAY_LOG(DEBUG) << "[JAE_DEBUG] DeleteEagerSpilledObjects object " << object_id
			 <<" not spillable";
	      continue;
	    }
	    ret = true;
        RAY_LOG(DEBUG) << "[JAE_DEBUG] DeleteEagerSpilledObjects deleting " << object_id;
		RemovePinnedObjects(object_id, eager_spilled_it.second.first);
        store_object_count_(object_id, false, true);
        RAY_LOG(DEBUG) << "[JAE_DEBUG] DeleteEagerSpilledObjects deleted " << object_id;
	  }
	  eager_spilled_objects_.clear();
	}else{
      //TODO(Jae) Ski-Rental
	}
	if(!ret){
      RAY_LOG(DEBUG) << "[JAE_DEBUG] DeleteEagerSpilledObjects nothing to delete pinned:"
		  << pinned_objects_.size() << " eager_spilled:" << eager_spilled_objects_.size()
		  <<" pending_eager_spill:"<< objects_pending_eager_spill_.size();
	}
	return ret;
}

void LocalObjectManager::OnObjectEagerSpilled(const std::vector<ObjectID> &object_ids,
                                         const rpc::SpillObjectsReply &worker_reply) {
  RAY_LOG(DEBUG) << "[JAE_DEBUG] OnObjectEagerSpilled called #objs:"<<worker_reply.spilled_objects_url_size();
  std::vector<std::string> object_url_to_delete;
  for (size_t i = 0; i < static_cast<size_t>(worker_reply.spilled_objects_url_size());
       ++i) {
    const ObjectID &object_id = object_ids[i];
    const std::string &object_url = worker_reply.spilled_objects_url(i);
    RAY_LOG(DEBUG) << "Object " << object_id << " eager spilled at " << object_url;
    // Update the object_id -> url_ref_count to use it for deletion later.
    // We need to track the references here because a single file can contain
    // multiple objects, and we shouldn't delete the file until
    // all the objects are gone out of scope.
    // object_url is equivalent to url_with_offset.
    auto parsed_url = ParseURL(object_url);
    const auto base_url_it = parsed_url->find("url");
    RAY_CHECK(base_url_it != parsed_url->end());
    if (!url_ref_count_.contains(base_url_it->second)) {
      url_ref_count_[base_url_it->second] = 1;
    } else {
      url_ref_count_[base_url_it->second] += 1;
    }

    // Mark that the object is spilled and unpin the pending requests.
    spilled_objects_url_.emplace(object_id, object_url);
    RAY_LOG(DEBUG) << "Removing pending eager spill object " << object_id;
    auto it = objects_pending_eager_spill_.find(object_id);
    RAY_CHECK(it != objects_pending_eager_spill_.end());
    const auto object_size = it->second->GetSize();
    num_bytes_pending_spill_ -= object_size;
    objects_pending_eager_spill_.erase(it);

    // Asynchronously Update the spilled URL.
    auto freed_it = local_objects_.find(object_id);
    if (freed_it == local_objects_.end() || freed_it->second.second) {
      RAY_LOG(DEBUG) << "Eager Spilled object already freed, skipping send of spilled URL to "
                        "object directory for object "
                     << object_id;
      continue;
    }
    const auto &worker_addr = freed_it->second.first;
    object_directory_->ReportObjectSpilled(
        object_id, self_node_id_, worker_addr, object_url, is_external_storage_type_fs_);

	eager_spilled_objects_.emplace(object_id, std::make_pair(object_size, worker_addr));
	RemovePinnedObjects(object_id, object_size);
	objectID_to_priority_.erase(object_id);
	if(freed_during_eager_spill_.contains(object_id)){
	  object_url_to_delete.emplace_back(object_url);
	  freed_during_eager_spill_.erase(object_id);
	}
  }
  if(!object_url_to_delete.empty()){
    RAY_LOG(DEBUG) << "[JAE_DEBUG] deleting freed objects during eager spill";
    //DeleteSpilledObjects(object_url_to_delete);
  }
  RAY_LOG(DEBUG) << "Finished spilling " << object_ids.size() << " objects. Calling EagerSpill()";
  EagerSpill();
}

void LocalObjectManager::OnObjectSpilled(const std::vector<ObjectID> &object_ids,
                                         const rpc::SpillObjectsReply &worker_reply) {
  const bool enable_eagerSpill = RayConfig::instance().enable_EagerSpill();
  RAY_CHECK(!enable_eagerSpill);
  for (size_t i = 0; i < static_cast<size_t>(worker_reply.spilled_objects_url_size());
       ++i) {
    const ObjectID &object_id = object_ids[i];
    const std::string &object_url = worker_reply.spilled_objects_url(i);
    RAY_LOG(DEBUG) << "Object " << object_id << " spilled at " << object_url;

    // Update the object_id -> url_ref_count to use it for deletion later.
    // We need to track the references here because a single file can contain
    // multiple objects, and we shouldn't delete the file until
    // all the objects are gone out of scope.
    // object_url is equivalent to url_with_offset.
    auto parsed_url = ParseURL(object_url);
    const auto base_url_it = parsed_url->find("url");
    RAY_CHECK(base_url_it != parsed_url->end());
    if (!url_ref_count_.contains(base_url_it->second)) {
      url_ref_count_[base_url_it->second] = 1;
    } else {
      url_ref_count_[base_url_it->second] += 1;
    }

    // Mark that the object is spilled and unpin the pending requests.
    spilled_objects_url_.emplace(object_id, object_url);
    RAY_LOG(DEBUG) << "Unpinning pending spill object " << object_id;
    auto it = objects_pending_spill_.find(object_id);
    RAY_CHECK(it != objects_pending_spill_.end());
    const auto object_size = it->second->GetSize();
    num_bytes_pending_spill_ -= object_size;
    objects_pending_spill_.erase(it);

    // Asynchronously Update the spilled URL.
    auto freed_it = local_objects_.find(object_id);
    if (freed_it == local_objects_.end() || freed_it->second.second) {
      RAY_LOG(DEBUG) << "Spilled object already freed, skipping send of spilled URL to "
                        "object directory for object "
                     << object_id;
      continue;
    }
    const auto &worker_addr = freed_it->second.first;
    object_directory_->ReportObjectSpilled(
        object_id, self_node_id_, worker_addr, object_url, is_external_storage_type_fs_);
  }
}

std::string LocalObjectManager::GetLocalSpilledObjectURL(const ObjectID &object_id) {
  if (!is_external_storage_type_fs_) {
    // If the external storage is cloud storage like S3, returns the empty string.
    // In that case, the URL is supposed to be obtained by OBOD.
    return "";
  }
  auto entry = spilled_objects_url_.find(object_id);
  if (entry != spilled_objects_url_.end()) {
    return entry->second;
  } else {
    return "";
  }
}

void LocalObjectManager::AsyncRestoreSpilledObject(
    const ObjectID &object_id,
    int64_t object_size,
    const std::string &object_url,
    std::function<void(const ray::Status &)> callback) {
  if (objects_pending_restore_.count(object_id) > 0) {
    // If the same object is restoring, we dedup here.
    return;
  }

  RAY_CHECK(objects_pending_restore_.emplace(object_id).second)
      << "Object dedupe wasn't done properly. Please report if you see this issue.";
  num_bytes_pending_restore_ += object_size;
  io_worker_pool_.PopRestoreWorker([this, object_id, object_size, object_url, callback](
                                       std::shared_ptr<WorkerInterface> io_worker) {
    auto start_time = absl::GetCurrentTimeNanos();
    RAY_LOG(DEBUG) << "Sending restore spilled object request";
    rpc::RestoreSpilledObjectsRequest request;
    request.add_spilled_objects_url(std::move(object_url));
    request.add_object_ids_to_restore(object_id.Binary());
    io_worker->rpc_client()->RestoreSpilledObjects(
        request,
        [this, start_time, object_id, object_size, callback, io_worker](
            const ray::Status &status, const rpc::RestoreSpilledObjectsReply &r) {
          io_worker_pool_.PushRestoreWorker(io_worker);
          num_bytes_pending_restore_ -= object_size;
          objects_pending_restore_.erase(object_id);
          if (!status.ok()) {
            RAY_LOG(ERROR) << "Failed to send restore spilled object request: "
                           << status.ToString();
          } else {
            auto now = absl::GetCurrentTimeNanos();
            auto restored_bytes = r.bytes_restored_total();
            RAY_LOG(DEBUG) << "Restored " << restored_bytes << " in "
                           << (now - start_time) / 1e6 << "ms. Object id:" << object_id;
            restored_bytes_total_ += restored_bytes;
            restored_objects_total_ += 1;
            // Adjust throughput timing to account for concurrent restore operations.
            restore_time_total_s_ +=
                (now - std::max(start_time, last_restore_finish_ns_)) / 1e9;
            if (now - last_restore_log_ns_ > 1e9) {
              last_restore_log_ns_ = now;
              RAY_LOG(INFO) << "Restored "
                            << static_cast<int>(restored_bytes_total_ / (1024 * 1024))
                            << " MiB, " << restored_objects_total_
                            << " objects, read throughput "
                            << static_cast<int>(restored_bytes_total_ / (1024 * 1024) /
                                                restore_time_total_s_)
                            << " MiB/s";
            }
            last_restore_finish_ns_ = now;
          }
          if (callback) {
            callback(status);
          }
        });
  });
}

void LocalObjectManager::ProcessSpilledObjectsDeleteQueue(uint32_t max_batch_size) {
  std::vector<std::string> object_urls_to_delete;
  // Process upto batch size of objects to delete.
  while (!spilled_object_pending_delete_.empty() &&
         object_urls_to_delete.size() < max_batch_size) {
    auto &object_id = spilled_object_pending_delete_.front();
    // If the object is still spilling, do nothing. This will block other entries to be
    // processed, but it should be fine because the spilling will be eventually done,
    // and deleting objects is the low priority tasks. This will instead enable simpler
    // logic after this block.
    if (objects_pending_spill_.contains(object_id)) {
      break;
    }

    // Object id is either spilled or not spilled at this point.
    const auto spilled_objects_url_it = spilled_objects_url_.find(object_id);
    if (spilled_objects_url_it != spilled_objects_url_.end()) {
      // If the object was spilled, see if we can delete it. We should first check the
      // ref count.
      std::string &object_url = spilled_objects_url_it->second;
      // Note that here, we need to parse the object url to obtain the base_url.
      auto parsed_url = ParseURL(object_url);
      const auto base_url_it = parsed_url->find("url");
      RAY_CHECK(base_url_it != parsed_url->end());
      const auto &url_ref_count_it = url_ref_count_.find(base_url_it->second);
      RAY_CHECK(url_ref_count_it != url_ref_count_.end())
          << "url_ref_count_ should exist when spilled_objects_url_ exists. Please "
             "submit a Github issue if you see this error.";
      url_ref_count_it->second -= 1;

      // If there's no more refs, delete the object.
      if (url_ref_count_it->second == 0) {
        url_ref_count_.erase(url_ref_count_it);
        RAY_LOG(DEBUG) << "The URL " << object_url
                       << " is deleted because the references are out of scope.";
        object_urls_to_delete.emplace_back(object_url);
      }
      spilled_objects_url_.erase(spilled_objects_url_it);
    } else {
      // If the object was not spilled, it gets pinned again. Unpin here to
      // prevent a memory leak.
      pinned_objects_.erase(object_id);
    }
    local_objects_.erase(object_id);
    spilled_object_pending_delete_.pop();
  }
  if (object_urls_to_delete.size() > 0) {
    DeleteSpilledObjects(object_urls_to_delete);
  }
}

void LocalObjectManager::DeleteSpilledObjects(std::vector<std::string> &urls_to_delete) {
  io_worker_pool_.PopDeleteWorker(
      [this, urls_to_delete](std::shared_ptr<WorkerInterface> io_worker) {
        RAY_LOG(DEBUG) << "Sending delete spilled object request. Length: "
                       << urls_to_delete.size();
        rpc::DeleteSpilledObjectsRequest request;
        for (const auto &url : urls_to_delete) {
          request.add_spilled_objects_url(std::move(url));
        }
        io_worker->rpc_client()->DeleteSpilledObjects(
            request,
            [this, io_worker](const ray::Status &status,
                              const rpc::DeleteSpilledObjectsReply &reply) {
              io_worker_pool_.PushDeleteWorker(io_worker);
              if (!status.ok()) {
                RAY_LOG(ERROR) << "Failed to send delete spilled object request: "
                               << status.ToString();
              }else{
                RAY_LOG(DEBUG) << "Deleted spilled object ";
			  }
            });
      });
}

void LocalObjectManager::FillObjectSpillingStats(rpc::GetNodeStatsReply *reply) const {
  auto stats = reply->mutable_store_stats();
  stats->set_spill_time_total_s(spill_time_total_s_);
  stats->set_spilled_bytes_total(spilled_bytes_total_);
  stats->set_spilled_objects_total(spilled_objects_total_);
  stats->set_restore_time_total_s(restore_time_total_s_);
  stats->set_restored_bytes_total(restored_bytes_total_);
  stats->set_restored_objects_total(restored_objects_total_);
  stats->set_object_store_bytes_primary_copy(pinned_objects_size_);
}

void LocalObjectManager::RecordMetrics() const {
  /// Record Metrics.
  if (spilled_bytes_total_ != 0 && spill_time_total_s_ != 0) {
    ray::stats::STATS_spill_manager_throughput_mb.Record(
        spilled_bytes_total_ / 1024 / 1024 / spill_time_total_s_, "Spilled");
  }
  if (restored_bytes_total_ != 0 && restore_time_total_s_ != 0) {
    ray::stats::STATS_spill_manager_throughput_mb.Record(
        restored_bytes_total_ / 1024 / 1024 / restore_time_total_s_, "Restored");
  }
  ray::stats::STATS_spill_manager_objects.Record(pinned_objects_.size(), "Pinned");
  ray::stats::STATS_spill_manager_objects.Record(objects_pending_restore_.size(),
                                                 "PendingRestore");
  ray::stats::STATS_spill_manager_objects.Record(objects_pending_spill_.size(),
                                                 "PendingSpill");

  ray::stats::STATS_spill_manager_objects_bytes.Record(pinned_objects_size_, "Pinned");
  ray::stats::STATS_spill_manager_objects_bytes.Record(num_bytes_pending_spill_,
                                                       "PendingSpill");
  ray::stats::STATS_spill_manager_objects_bytes.Record(num_bytes_pending_restore_,
                                                       "PendingRestore");
  ray::stats::STATS_spill_manager_objects_bytes.Record(spilled_bytes_total_, "Spilled");
  ray::stats::STATS_spill_manager_objects_bytes.Record(restored_objects_total_,
                                                       "Restored");

  ray::stats::STATS_spill_manager_request_total.Record(spilled_objects_total_, "Spilled");
  ray::stats::STATS_spill_manager_request_total.Record(restored_objects_total_,
                                                       "Restored");
}

int64_t LocalObjectManager::GetPrimaryBytes() const {
  return pinned_objects_size_ + num_bytes_pending_spill_;
}

bool LocalObjectManager::HasLocallySpilledObjects() const {
  if (!is_external_storage_type_fs_) {
    // External storage is not local.
    return false;
  }
  // Report non-zero usage when there are spilled / spill-pending live objects, to
  // prevent this node from being drained. Note that the value reported here is also
  // used for scheduling.
  return !spilled_objects_url_.empty();
}

std::string LocalObjectManager::DebugString() const {
  std::stringstream result;
  result << "LocalObjectManager:\n";
  result << "- num pinned objects: " << pinned_objects_.size() << "\n";
  result << "- pinned objects size: " << pinned_objects_size_ << "\n";
  result << "- num objects pending restore: " << objects_pending_restore_.size() << "\n";
  result << "- num objects pending spill: " << objects_pending_spill_.size() << "\n";
  result << "- num bytes pending spill: " << num_bytes_pending_spill_ << "\n";
  result << "- cumulative spill requests: " << spilled_objects_total_ << "\n";
  result << "- cumulative restore requests: " << restored_objects_total_ << "\n";
  result << "- spilled objects pending delete: " << spilled_object_pending_delete_.size()
         << "\n";
  return result.str();
}

};  // namespace raylet
};  // namespace ray
