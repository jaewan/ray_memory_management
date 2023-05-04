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

#include "ray/core_worker/transport/direct_task_transport.h"

#include "ray/core_worker/transport/dependency_resolver.h"

#include <string>
#include <fstream>
#include <iostream>
#include <cerrno>

namespace ray {
namespace core {

inline void LeaseGrant(const std::string &fnc_name, const Priority &pri, int request_num, bool is_spillback){
	/*
  std::ofstream log_stream("/tmp/ray/core_worker_log", std::ios_base::app);
  std::ostringstream stream;
  stream << fnc_name << " " << pri << " request_num: " << request_num << " remote worker:" << is_spillback << "\n";
  std::string log_str = stream.str();
  log_stream << log_str;
  log_stream.close();
	*/
}

inline void LogPlaceSeq(const std::string &fnc_name, const Priority &pri){
	/*
  std::ofstream log_stream("/tmp/ray/core_worker_log", std::ios_base::app);
  std::ostringstream stream;
  stream << fnc_name << " " << pri << "\n";
  std::string log_str = stream.str();
  log_stream << log_str;
  log_stream.close();
	*/
}

inline void LogLeaseSeq(const TaskID &task_id, const std::string &fnc_name, const Priority &pri, ray::NodeID raylet_id){
	/*
  std::ofstream log_stream("/tmp/ray/core_worker_log", std::ios_base::app);
  std::ostringstream stream;
  stream << task_id <<" " <<
	fnc_name << " " << pri << " raylet:" << raylet_id  << "\n";
  std::string log_str = stream.str();
  log_stream << log_str;
  log_stream.close();
	*/
}

Status CoreWorkerDirectTaskSubmitter::SubmitTask(TaskSpecification task_spec) {
  RAY_LOG(DEBUG) << "Submit task " << task_spec.TaskId() << " TaskSubmitter JobId:" << task_spec.JobId();
  num_tasks_submitted_++;

  resolver_.ResolveDependencies(task_spec, [this, task_spec](Status status) {
    task_finisher_->MarkDependenciesResolved(task_spec.TaskId());
    if (!status.ok()) {
      RAY_LOG(WARNING) << "Resolving task dependencies failed " << status.ToString();
      RAY_UNUSED(task_finisher_->FailOrRetryPendingTask(
          task_spec.TaskId(), rpc::ErrorType::DEPENDENCY_RESOLUTION_FAILED, &status));
      return;
    }
    RAY_LOG(DEBUG) << "Task dependencies resolved " << task_spec.TaskId();
    if (task_spec.IsActorCreationTask()) {
      // If gcs actor management is enabled, the actor creation task will be sent to
      // gcs server directly after the in-memory dependent objects are resolved. For
      // more details please see the protocol of actor management based on gcs.
      // https://docs.google.com/document/d/1EAWide-jy05akJp6OMtDn58XOK7bUyruWMia4E-fV28/edit?usp=sharing
      auto actor_id = task_spec.ActorCreationId();
      auto task_id = task_spec.TaskId();
      RAY_LOG(DEBUG) << "Creating actor via GCS actor id = : " << actor_id;
      RAY_CHECK_OK(actor_creator_->AsyncCreateActor(
          task_spec,
          [this, actor_id, task_id](Status status, const rpc::CreateActorReply &reply) {
            if (status.ok()) {
              RAY_LOG(DEBUG) << "Created actor, actor id = " << actor_id;
              // Copy the actor's reply to the GCS for ref counting purposes.
              rpc::PushTaskReply push_task_reply;
              push_task_reply.mutable_borrowed_refs()->CopyFrom(reply.borrowed_refs());
              task_finisher_->CompletePendingTask(
                  task_id, push_task_reply, reply.actor_address());
            } else {
              rpc::RayErrorInfo ray_error_info;
              if (status.IsSchedulingCancelled()) {
                RAY_LOG(DEBUG) << "Actor creation cancelled, actor id = " << actor_id;
                task_finisher_->MarkTaskCanceled(task_id);
                if (reply.has_death_cause()) {
                  ray_error_info.mutable_actor_died_error()->CopyFrom(
                      reply.death_cause());
                }
              } else {
                RAY_LOG(INFO) << "Failed to create actor " << actor_id
                              << " with status: " << status.ToString();
              }
              RAY_UNUSED(task_finisher_->FailOrRetryPendingTask(
                  task_id,
                  rpc::ErrorType::ACTOR_CREATION_FAILED,
                  &status,
                  ray_error_info.has_actor_died_error() ? &ray_error_info : nullptr));
            }
          }));
      return;
    }

    bool keep_executing = true;
    {
      absl::MutexLock lock(&mu_);
      if (cancelled_tasks_.find(task_spec.TaskId()) != cancelled_tasks_.end()) {
        cancelled_tasks_.erase(task_spec.TaskId());
        keep_executing = false;
      }

      if (keep_executing) {
        // Note that the dependencies in the task spec are mutated to only contain
        // plasma dependencies after ResolveDependencies finishes.
        const SchedulingKey scheduling_key(task_spec.GetSchedulingClass(),
                                           task_spec.GetDependencyIds(),
                                           task_spec.IsActorCreationTask()
                                               ? task_spec.ActorCreationId()
                                               : ActorID::Nil(),
                                           task_spec.GetRuntimeEnvHash());
				// This is a DFS patch
				/*
				static const std::vector<ObjectID> empty_dependencies;
        const SchedulingKey scheduling_key(0,
																					 empty_dependencies,
                                           task_spec.IsActorCreationTask()
                                               ? task_spec.ActorCreationId()
                                               : ActorID::Nil(),
                                           0);
				 */
        const auto priority = task_spec.GetPriority();
        auto inserted = tasks_.emplace(task_spec.TaskId(), TaskEntry(task_spec, scheduling_key, priority));
        if(!inserted.second){
        	tasks_.erase(task_spec.TaskId());
        	tasks_.emplace(task_spec.TaskId(), TaskEntry(task_spec, scheduling_key, priority));
				}
        //RAY_CHECK(inserted.second);
        const auto task_key = inserted.first->second.task_key;
				auto &scheduling_key_entry = scheduling_key_entries_[scheduling_key];
        //RAY_CHECK(scheduling_key_entry.task_priority_queue.emplace(task_key).second) << task_spec.TaskId();
        if(!scheduling_key_entry.task_priority_queue.emplace(task_key).second){
					scheduling_key_entry.task_priority_queue.erase(task_key);
					scheduling_key_entry.task_priority_queue.emplace(task_key);
				}

        auto ptq_inserted = priority_task_queues_.emplace(priority);
        if(!ptq_inserted.second){
        	priority_task_queues_.erase(priority);
        	priority_task_queues_.emplace(priority);
				}
        //RAY_CHECK(ptq_inserted.second);
        auto ptqnp_inserted = priority_task_queues_not_pushed_.emplace(priority);
        if(!ptqnp_inserted.second){
        	priority_task_queues_not_pushed_.erase(priority);
        	priority_task_queues_not_pushed_.emplace(priority);
				}
        //RAY_CHECK(ptqnp_inserted.second);
        auto ptts_inserted = priority_to_task_spec_.emplace(priority, task_spec);
				if(!ptts_inserted.second){
					priority_to_task_spec_.erase(priority);
					priority_to_task_spec_.emplace(priority, task_spec);
				}
        //RAY_CHECK(ptts_inserted.second);

        RAY_LOG(DEBUG) << "Placed task " << task_key.second << " " << task_key.first;
        scheduling_key_entry.resource_spec = task_spec;

        if (!AllWorkersBusy()) {
          // There are idle workers, so we don't need more
          // workers.

          for (auto active_worker_addr : active_workers_) {
            RAY_CHECK(worker_to_lease_entry_.find(active_worker_addr) !=
                      worker_to_lease_entry_.end());
            auto &lease_entry = worker_to_lease_entry_[active_worker_addr];
            if (!lease_entry.is_busy) {
              OnWorkerIdle(active_worker_addr,
                           scheduling_key,
                           /*was_error*/ false,
                           /*worker_exiting*/ false,
                           lease_entry.assigned_resources);
              break;
            }
          }
        }
        RequestNewWorkerIfNeeded(scheduling_key);
      }
    }
    if (!keep_executing) {
      RAY_UNUSED(task_finisher_->FailOrRetryPendingTask(
          task_spec.TaskId(), rpc::ErrorType::TASK_CANCELLED, nullptr));
    }
  });
  return Status::OK();
}

void CoreWorkerDirectTaskSubmitter::AddWorkerLeaseClient(
    const rpc::WorkerAddress &addr,
    std::shared_ptr<WorkerLeaseInterface> lease_client,
    const google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry> &assigned_resources,
    const SchedulingKey &scheduling_key) {
  client_cache_->GetOrConnect(addr.ToProto());
  int64_t expiration = current_time_ms() + lease_timeout_ms_;
  LeaseEntry new_lease_entry =
      LeaseEntry(std::move(lease_client), expiration, assigned_resources, scheduling_key);
  worker_to_lease_entry_.emplace(addr, new_lease_entry);

  RAY_CHECK(active_workers_.emplace(addr).second);
  RAY_CHECK(active_workers_.size() >= 1);
}

void CoreWorkerDirectTaskSubmitter::ReturnWorker(const rpc::WorkerAddress addr,
                                                 bool was_error,
                                                 bool worker_exiting) {
  RAY_LOG(DEBUG) << "Returning worker " << addr.worker_id << " to raylet "
                 << addr.raylet_id;
  RAY_CHECK(active_workers_.size() >= 1);
  auto &lease_entry = worker_to_lease_entry_[addr];
  RAY_CHECK(lease_entry.lease_client);
  RAY_CHECK(!lease_entry.is_busy);

  // Decrement the number of active workers consuming tasks from the queue 
  active_workers_.erase(addr);

  auto status = lease_entry.lease_client->ReturnWorker(
      addr.port, addr.worker_id, was_error, worker_exiting);
  if (!status.ok()) {
    RAY_LOG(ERROR) << "Error returning worker to raylet: " << status.ToString();
  }
  worker_to_lease_entry_.erase(addr);
}

void CoreWorkerDirectTaskSubmitter::ReturnWorker(const rpc::WorkerAddress addr,
                                                 bool was_error,
                                                 bool worker_exiting,
                                                 const SchedulingKey &scheduling_key) {
  RAY_LOG(DEBUG) << "Returning worker " << addr.worker_id << " to raylet "
                 << addr.raylet_id;
  auto &scheduling_key_entry = scheduling_key_entries_[scheduling_key];
  RAY_CHECK(scheduling_key_entry.active_workers.size() >= 1);
  auto &lease_entry = worker_to_lease_entry_[addr];
  RAY_CHECK(lease_entry.lease_client);
  RAY_CHECK(!lease_entry.is_busy);

  // Decrement the number of active workers consuming tasks from the queue associated
  // with the current scheduling_key
  scheduling_key_entry.active_workers.erase(addr);
  if (scheduling_key_entry.CanDelete()) {
    // We can safely remove the entry keyed by scheduling_key from the
    // scheduling_key_entries_ hashmap.
    scheduling_key_entries_.erase(scheduling_key);
  }

  auto status = lease_entry.lease_client->ReturnWorker(
      addr.port, addr.worker_id, was_error, worker_exiting);
  if (!status.ok()) {
    RAY_LOG(ERROR) << "Error returning worker to raylet: " << status.ToString();
  }
  worker_to_lease_entry_.erase(addr);
}

// Dispatch task to leased worker strictly following the requested task as raylet has pulled
// its arguments. So also ignore block_requested_priority
void CoreWorkerDirectTaskSubmitter::OnWorkerIdle(
    const rpc::WorkerAddress &addr,
    const SchedulingKey &scheduling_key,
    const Priority &lease_granted_pri,
    bool was_error,
    bool worker_exiting,
    const google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry> &assigned_resources) {
	// Calling this function means all its arguments are present in the object store
	const auto &task = priority_to_task_spec_[lease_granted_pri];
	for (size_t i = 0; i < task.NumArgs(); i++) {
		if (task.ArgByRef(i)) {
			spilled_objects_.erase(task.ArgId(i));
		}
		for (const auto &in : task.ArgInlinedRefs(i)) {
			auto object_id = ObjectID::FromBinary(in.object_id());
			spilled_objects_.erase(object_id);
		}
	}
	// This is because the lease granted task has already been pushed as core_worker pushes
	// tasks to workers regardless of granted task
	if(!priority_to_task_spec_.contains(lease_granted_pri) || !priority_task_queues_not_pushed_.contains(lease_granted_pri)){
		//RAY_CHECK(false) << "Jae Handle this";
		RAY_LOG(DEBUG) << "!!!Error Jae [JAE_DEBUG] OnWorkerIdle task with priority:"<<lease_granted_pri 
									 << " does not exist , redirecting to general OnWorkerIdle or blocked by backpressure";
		OnWorkerIdle(addr,
								 scheduling_key,
								 /*error=*/was_error,
								 /*worker_exiting=*/worker_exiting,
								 assigned_resources);
		return;
	}
  auto &lease_entry = worker_to_lease_entry_[addr];
  RAY_LOG(DEBUG) << "[JAE_DEBUG] OnWorkerIdle worker:"<<addr.worker_id << " was_error:" 
                 << was_error << " worker_exiting:"<< worker_exiting << " lease time expired:" 
	             	 << (current_time_ms() > lease_entry.lease_expiration_time)
			  	 			 << " OnWorkerIdle Called on priority:" << lease_granted_pri;
  if (!lease_entry.lease_client) {
    return;
  }

  // Return the worker if there was an error executing the previous task,
  // the lease is expired; Return the worker if there are no more applicable
  // queued tasks.
  if ((was_error || worker_exiting )){
       //current_time_ms() > lease_entry.lease_expiration_time)) {
    RAY_CHECK(active_workers_.size() >= 1);

    // Return the worker only if there are no tasks to do.
    if (!lease_entry.is_busy) {
      ReturnWorker(addr, was_error, worker_exiting);
    }
  } else {
    auto &client = *client_cache_->GetOrConnect(addr.ToProto());

    Priority pri(lease_granted_pri.score);
		priority_task_queues_.erase(pri);
    priority_task_queues_not_pushed_.erase(pri);
    while (!lease_entry.is_busy) {
      const auto &task_spec = priority_to_task_spec_[pri];
      lease_entry.is_busy = true;

      RAY_CHECK(active_workers_.size() >= 1);
      num_busy_workers_++;

      executing_tasks_.emplace(task_spec.TaskId(), addr);
      lease_entry.lease_expiration_time = current_time_ms() - 1;
      PushNormalTask(addr, client, scheduling_key, task_spec, assigned_resources);
      const auto it = tasks_.find(task_spec.TaskId());
      scheduling_key_entries_[scheduling_key].task_priority_queue.erase(it->second.task_key);
      tasks_.erase(task_spec.TaskId());
    }

    CancelWorkerLeaseIfNeeded(scheduling_key);
  }
  RequestNewWorkerIfNeeded(scheduling_key);
}

void CoreWorkerDirectTaskSubmitter::OnWorkerIdle(
    const rpc::WorkerAddress &addr,
    const SchedulingKey &scheduling_key,
    bool was_error,
    bool worker_exiting,
    const google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry> &assigned_resources) {
  auto &lease_entry = worker_to_lease_entry_[addr];
  auto pri_it = priority_task_queues_not_pushed_.begin();
  RAY_LOG(DEBUG) << "[JAE_DEBUG] OnWorkerIdle worker:"<<addr.worker_id << " was_error:" 
                 << was_error << " worker_exiting:"<< worker_exiting << " lease time expired:" 
								 << (current_time_ms() > lease_entry.lease_expiration_time)
							   << " should be blocked:" << (*pri_it > block_requested_priority_[addr.raylet_id]);
  if (!lease_entry.lease_client && pri_it == priority_task_queues_not_pushed_.end()) {
    return;
  }

  // Return the worker if there was an error executing the previous task,
  // the lease is expired; Return the worker if there are no more applicable
  // queued tasks.
  if ((was_error || worker_exiting ||
       current_time_ms() > lease_entry.lease_expiration_time) || *pri_it > block_requested_priority_[addr.raylet_id]) {
    //priority_task_queues_not_pushed_lock.unlock();
    RAY_CHECK(active_workers_.size() >= 1);

    // Return the worker only if there are no tasks to do.
    if (!lease_entry.is_busy) {
      ReturnWorker(addr, was_error, worker_exiting);
    }else{
			RAY_LOG(DEBUG) << "[JAE_DEBUG] OnWorkerIdle lease_entry is busy";
		}
  } else {
		bool spilled_arguments = true;
		while(spilled_arguments){
			spilled_arguments = false;
			const auto &task = priority_to_task_spec_[*pri_it];
			for (size_t i = 0; i < task.NumArgs(); i++) {
				if (task.ArgByRef(i) && spilled_objects_.contains(task.ArgId(i))) {
					spilled_arguments = true;
					break;
				}
				for (const auto &in : task.ArgInlinedRefs(i)) {
					auto object_id = ObjectID::FromBinary(in.object_id());
					if (spilled_objects_.contains(object_id)) {
						spilled_arguments = true;
						break;
					}
				}
			}
			if(spilled_arguments){
				RAY_LOG(DEBUG) << "[JAE_DEBUG] OnWorkerIdle priority:" << *pri_it << " has spilled args, skipping";
				pri_it++;
				if (pri_it == priority_task_queues_not_pushed_.end()) {
					ReturnWorker(addr, was_error, worker_exiting);
					RequestNewWorkerIfNeeded(scheduling_key);
					return;
				}
			}else{
				// Find locality
				if ( pri_it->score.size() == 1)
					break;
				if(locality_node_cache_.contains(*pri_it)){
					if((locality_node_cache_[*pri_it]) == addr.raylet_id){
						locality_node_cache_.erase(*pri_it);
						break;
					}
				}else if(auto task_locality_node_id = lease_policy_->GetBestNodeIdForTask(task)){
					locality_node_cache_.emplace(*pri_it, *task_locality_node_id);
					if((*task_locality_node_id) == addr.raylet_id){
						break;
					}
				}
			}//end locality
		}//end while	
    const Priority pri(pri_it->score);
		priority_task_queues_.erase(pri);
    priority_task_queues_not_pushed_.erase(pri_it);
    //priority_task_queues_not_pushed_lock.unlock();
    auto &client = *client_cache_->GetOrConnect(addr.ToProto());

    while (!lease_entry.is_busy) {
      const auto &task_spec = priority_to_task_spec_[pri];
      lease_entry.is_busy = true;

      // Increment the total number of tasks in flight to any worker associated with the
      // current scheduling_key

      RAY_CHECK(active_workers_.size() >= 1);
      num_busy_workers_++;

      executing_tasks_.emplace(task_spec.TaskId(), addr);
      lease_entry.lease_expiration_time = current_time_ms() - 1;
      PushNormalTask(addr, client, scheduling_key, task_spec, assigned_resources);
      const auto it = tasks_.find(task_spec.TaskId());
      scheduling_key_entries_[scheduling_key].task_priority_queue.erase(it->second.task_key);
      tasks_.erase(task_spec.TaskId());
    }

    CancelWorkerLeaseIfNeeded(scheduling_key);
  }
  RequestNewWorkerIfNeeded(scheduling_key);
}

void CoreWorkerDirectTaskSubmitter::CancelWorkerLeaseIfNeeded(
    const SchedulingKey &scheduling_key) {
  auto &scheduling_key_entry = scheduling_key_entries_[scheduling_key];
  auto &task_priority_queue = scheduling_key_entry.task_priority_queue;
  if (!task_priority_queue.empty()) {
    // There are still pending tasks, or there are tasks that can be stolen by a new
    // worker, so let the worker lease request succeed.
    return;
  }

  RAY_LOG(DEBUG) << "Task queue is empty; canceling lease request";

  for (auto &pending_lease_request : scheduling_key_entry.pending_lease_requests) {
    // There is an in-flight lease request. Cancel it.
    auto lease_client = GetOrConnectLeaseClient(&pending_lease_request.second);
    auto &task_id = pending_lease_request.first;
    RAY_LOG(DEBUG) << "Canceling lease request " << task_id;
    lease_client->CancelWorkerLease(
        task_id,
        [this, scheduling_key](const Status &status,
                               const rpc::CancelWorkerLeaseReply &reply) {
          absl::MutexLock lock(&mu_);
          if (status.ok() && !reply.success()) {
            // The cancellation request can fail if the raylet does not have
            // the request queued. This can happen if: a) due to message
            // reordering, the raylet has not yet received the worker lease
            // request, or b) we have already returned the worker lease
            // request. In the former case, we should try the cancellation
            // request again. In the latter case, the in-flight lease request
            // should already have been removed from our local state, so we no
            // longer need to cancel.
            CancelWorkerLeaseIfNeeded(scheduling_key);
          }
        });
  }
}

std::shared_ptr<WorkerLeaseInterface>
CoreWorkerDirectTaskSubmitter::GetOrConnectLeaseClient(
    const rpc::Address *raylet_address) {
  std::shared_ptr<WorkerLeaseInterface> lease_client;
  RAY_CHECK(raylet_address != nullptr);
  if (NodeID::FromBinary(raylet_address->raylet_id()) != local_raylet_id_) {
    // A remote raylet was specified. Connect to the raylet if needed.
    NodeID raylet_id = NodeID::FromBinary(raylet_address->raylet_id());
    auto it = remote_lease_clients_.find(raylet_id);
    if (it == remote_lease_clients_.end()) {
      RAY_LOG(INFO) << "Connecting to raylet " << raylet_id;
      it = remote_lease_clients_
               .emplace(raylet_id,
                        lease_client_factory_(raylet_address->ip_address(),
                                              raylet_address->port()))
               .first;
    }
    lease_client = it->second;
  } else {
    lease_client = local_lease_client_;
  }

  return lease_client;
}

void CoreWorkerDirectTaskSubmitter::ReportWorkerBacklog() {
  absl::MutexLock lock(&mu_);
  ReportWorkerBacklogInternal();
}

void CoreWorkerDirectTaskSubmitter::ReportWorkerBacklogInternal() {
  absl::flat_hash_map<SchedulingClass, std::pair<TaskSpecification, int64_t>> backlogs;
  for (auto &scheduling_key_and_entry : scheduling_key_entries_) {
    const SchedulingClass scheduling_class = std::get<0>(scheduling_key_and_entry.first);
    if (backlogs.find(scheduling_class) == backlogs.end()) {
      backlogs[scheduling_class].first = scheduling_key_and_entry.second.resource_spec;
      backlogs[scheduling_class].second = 0;
    }
    // We report backlog size per scheduling class not per scheduling key
    // so we need to aggregate backlog sizes of different scheduling keys
    // with the same scheduling class
    backlogs[scheduling_class].second += scheduling_key_and_entry.second.BacklogSize();
    scheduling_key_and_entry.second.last_reported_backlog_size =
        scheduling_key_and_entry.second.BacklogSize();
  }

  std::vector<rpc::WorkerBacklogReport> backlog_reports;
  for (const auto &backlog : backlogs) {
    rpc::WorkerBacklogReport backlog_report;
    backlog_report.mutable_resource_spec()->CopyFrom(backlog.second.first.GetMessage());
    backlog_report.set_backlog_size(backlog.second.second);
    backlog_reports.emplace_back(backlog_report);
  }
  local_lease_client_->ReportWorkerBacklog(WorkerID::FromBinary(rpc_address_.worker_id()),
                                           backlog_reports);
}

void CoreWorkerDirectTaskSubmitter::ReportWorkerBacklogIfNeeded(
    const SchedulingKey &scheduling_key) {
  const auto &scheduling_key_entry = scheduling_key_entries_[scheduling_key];

  if (scheduling_key_entry.last_reported_backlog_size !=
      scheduling_key_entry.BacklogSize()) {
    ReportWorkerBacklogInternal();
  }
}

void CoreWorkerDirectTaskSubmitter::SetBlockSpill(const std::string &raylet_address,
		const rpc::RequestWorkerLeaseReply &reply){
	static const bool enable_BlockTasks = RayConfig::instance().enable_BlockTasks();
	if (enable_BlockTasks){
		std::vector<int64_t> block_score(reply.priority().data(), reply.priority().data() +
			reply.priority().size());
		block_requested_priority_[NodeID::FromBinary(raylet_address)].Set(block_score);
		RAY_LOG(DEBUG) << "[JAE_DEBUG] SetBlockSpill set from raylet: " 
									 << NodeID::FromBinary(raylet_address) << " to priority:" << block_score;
	}
}

void CoreWorkerDirectTaskSubmitter::RequestNewWorkerIfNeeded(
    const SchedulingKey &scheduling_key, const rpc::Address *raylet_address) {
  auto &scheduling_key_entry = scheduling_key_entries_[scheduling_key];

  if (!AllWorkersBusy() || priority_task_queues_.empty()) {
    // There are idle workers, so we don't need more.
    return;
  }
  if (scheduling_key_entry.pending_lease_requests.size() ==
      10) {
    RAY_LOG(DEBUG) << "Exceeding the pending request limit "
                   << max_pending_lease_requests_per_scheduling_category_;
    return;
  }
	/*
  if(num_leases_on_flight_ >= 32){
	   RAY_LOG(DEBUG) << "Exceeding the pending request limit " << 32;
     return;
  }
	*/
  auto priority_it = priority_task_queues_.begin();
	// Remove all Nill jobs. This could happen for race condition
	while(priority_it != priority_task_queues_.end() && priority_to_task_spec_[*priority_it].JobId().IsNil()){
		priority_it = priority_task_queues_.erase(priority_it);
	}
	if(priority_it == priority_task_queues_.end()){
		return;
	}
  const bool is_spillback = (raylet_address != nullptr);
  auto resource_spec_msg = priority_to_task_spec_[*priority_it].GetMutableMessage();
	resource_spec_msg.set_task_id(TaskID::FromRandom(job_id_).Binary());
  TaskSpecification resource_spec = TaskSpecification(resource_spec_msg);

  if ( resource_spec.JobId().IsNil() ){
		RAY_LOG(DEBUG) << "[JAE_DEBUG] ERROR Jae redirect request should be handled. Local raylet already calculated the remote resource";
		if(is_spillback)
		  RAY_CHECK(is_spillback) << "Jae redirect request should be handled. Local raylet already calculated the remote resource";
    return;
  }

  Priority pri(priority_it->score);
  const TaskID task_id = resource_spec.TaskId();
  resource_spec.SetPriority(pri);
  priority_task_queues_.erase(priority_it);
  int64_t queue_size = priority_task_queues_.size();

  num_leases_on_flight_++;
  num_leases_requested_++;

	LeaseGrant("\t Lease req:" , pri, priority_task_queues_.size(), is_spillback);

  rpc::Address best_node_address;
  bool is_selected_based_on_locality = false;
  if (raylet_address == nullptr) {
    // If no raylet address is given, find the best worker for our next lease request.
    std::tie(best_node_address, is_selected_based_on_locality) =
        lease_policy_->GetBestNodeForTask(resource_spec);
    raylet_address = &best_node_address;
		if(is_selected_based_on_locality){
			locality_node_cache_.emplace(pri, NodeID::FromBinary(raylet_address->raylet_id()));
		}
  }

  auto lease_client = GetOrConnectLeaseClient(raylet_address);
  RAY_LOG(DEBUG) << "Requesting lease from raylet "
                 << NodeID::FromBinary(raylet_address->raylet_id()) << " for task "
                 << task_id << " priority:" << pri
								 << " JobId:" << resource_spec.JobId() 
								 << " num_leases_on_flight_:" << num_leases_on_flight_
								 << " is_spillback:" << is_spillback;
  // Subtract 1 so we don't double count the task we are requesting for.

	static int request_num = 0;

  lease_client->RequestWorkerLease(
      resource_spec.GetMessage(),
      /*grant_or_reject=*/is_spillback,
      [this, scheduling_key, task_id, is_spillback, raylet_address = *raylet_address, pri](
          const Status &status, const rpc::RequestWorkerLeaseReply &reply) {
        std::deque<TaskSpecification> tasks_to_fail;
        rpc::RayErrorInfo error_info;
        ray::Status error_status;
        rpc::ErrorType error_type = rpc::ErrorType::WORKER_DIED;
        {
          absl::MutexLock lock(&mu_);
					num_leases_on_flight_--;

          auto &scheduling_key_entry = scheduling_key_entries_[scheduling_key];
          auto lease_client = GetOrConnectLeaseClient(&raylet_address);
          scheduling_key_entry.pending_lease_requests.erase(task_id);

          if (status.ok()) {
            if (reply.canceled()) {
              RAY_LOG(DEBUG) << "Lease canceled for task: " << task_id
                             << ", canceled type: "
                             << rpc::RequestWorkerLeaseReply::SchedulingFailureType_Name(
                                    reply.failure_type());
              if (reply.failure_type() ==
                      rpc::RequestWorkerLeaseReply::
                          SCHEDULING_CANCELLED_RUNTIME_ENV_SETUP_FAILED ||
                  reply.failure_type() ==
                      rpc::RequestWorkerLeaseReply::
                          SCHEDULING_CANCELLED_PLACEMENT_GROUP_REMOVED ||
                  reply.failure_type() ==
                      rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_UNSCHEDULABLE) {
                // We need to actively fail all of the pending tasks in the queue when the
                // placement group was removed or the runtime env failed to be set up.
                // Such an operation is straightforward for the scenario of placement
                // group removal as all tasks in the queue are associated with the same
                // placement group, but in the case of runtime env setup failed, This
                // makes an implicit assumption that runtime_env failures are not
                // transient -- we may consider adding some retries in the future.
                RAY_CHECK(false) << "Jae you should handle worker lease failure"; // TODO(Jae) Handle this case for DFS patch
								//priority_task_queues_.emplace(pri);
                if (reply.failure_type() ==
                    rpc::RequestWorkerLeaseReply::
                        SCHEDULING_CANCELLED_RUNTIME_ENV_SETUP_FAILED) {
                  error_type = rpc::ErrorType::RUNTIME_ENV_SETUP_FAILED;
                  error_info.mutable_runtime_env_setup_failed_error()->set_error_message(
                      reply.scheduling_failure_message());
                } else if (reply.failure_type() ==
                           rpc::RequestWorkerLeaseReply::
                               SCHEDULING_CANCELLED_UNSCHEDULABLE) {
                  error_type = rpc::ErrorType::TASK_UNSCHEDULABLE_ERROR;
                  *(error_info.mutable_error_message()) =
                      reply.scheduling_failure_message();
                } else {
                  error_type = rpc::ErrorType::TASK_PLACEMENT_GROUP_REMOVED;
                }
              auto &task_priority_queue = scheduling_key_entry.task_priority_queue;
              while (!task_priority_queue.empty()) {
                const auto task_key_it = task_priority_queue.begin();
                const auto &task_id = task_key_it->second;
                const auto task_it = tasks_.find(task_id);
                RAY_CHECK(task_it != tasks_.end());
                task_priority_queue.erase(task_key_it);
                tasks_to_fail.push_back(std::move(task_it->second.task_spec));
                tasks_.erase(task_it);
              }
                //tasks_to_fail = std::move(scheduling_key_entry.task_queue);
                //scheduling_key_entry.task_queue.clear();
                if (scheduling_key_entry.CanDelete()) {
                  scheduling_key_entries_.erase(scheduling_key);
                }
              } else {
                RequestNewWorkerIfNeeded(scheduling_key);
              }
            } else if (reply.rejected()) {
              RAY_LOG(DEBUG) << "Lease rejected " << task_id;
							if(priority_task_queues_not_pushed_.contains(pri)){
								priority_task_queues_.emplace(pri);
							}
              // It might happen when the first raylet has a stale view
              // of the spillback raylet resources.
              // Retry the request at the first raylet since the resource view may be
              // refreshed.
              RAY_CHECK(is_spillback);
              RequestNewWorkerIfNeeded(scheduling_key);
            } else if (!reply.worker_address().raylet_id().empty()) {
              // We got a lease for a worker. Add the lease client state and try to
              // assign work to the worker.
              rpc::WorkerAddress addr(reply.worker_address());
							request_num++;
							//LeaseGrant("\t\tRequest Granted" , pri, request_num, is_spillback);
              RAY_LOG(DEBUG) << "Lease granted to task " << task_id << " with priority " 
                << pri <<" from raylet " << addr.raylet_id << " with worker " << addr.worker_id;

              auto resources_copy = reply.resource_mapping();
							SetBlockSpill(raylet_address.raylet_id(), reply);
              AddWorkerLeaseClient(
                  addr, std::move(lease_client), resources_copy, scheduling_key);
              RAY_CHECK(active_workers_.size() >= 1);
							if(priority_task_queues_not_pushed_.contains(pri)){
								LeaseGrant("\t\tRequest Granted" , pri, request_num, is_spillback);
								priority_task_queues_.emplace(pri);
							}
							if (reply.pulled_task()){
								RAY_LOG(DEBUG) << "[JAE_DEBUG] strictly follow the req" << pri;
								OnWorkerIdle(addr,
														 scheduling_key,
														 pri,
														 /*error=*/false,
														 /*worker_exiting=*/false,
														 resources_copy);
							}else{
								OnWorkerIdle(addr,
														 scheduling_key,
														 //pri,
														 /*error=*/false,
														 /*worker_exiting=*/false,
														 resources_copy);
							}
            } else {
              // The raylet redirected us to a different raylet to retry at.
              RAY_CHECK(!is_spillback);
              RAY_LOG(DEBUG) << "Redirect lease for task " << task_id << " from raylet "
                             << NodeID::FromBinary(raylet_address.raylet_id())
                             << " to raylet "
                             << NodeID::FromBinary(reply.retry_at_raylet_address().raylet_id())
			  										 << " is_spillback:" << (&reply.retry_at_raylet_address() != nullptr);
							if(priority_task_queues_not_pushed_.contains(pri)){
								LeaseGrant("\t\tRedirect Request" , pri, priority_task_queues_.size(), is_spillback);
								priority_task_queues_.emplace(pri);
							}else{
							}

              RequestNewWorkerIfNeeded(scheduling_key, &reply.retry_at_raylet_address());
            }
          } else if (lease_client != local_lease_client_) {
						if(priority_task_queues_not_pushed_.contains(pri)){
							priority_task_queues_.emplace(pri);
						}
            // A lease request to a remote raylet failed. Retry locally if the lease is
            // still needed.
            // TODO(swang): Fail after some number of retries?
            RAY_LOG(INFO)
                << "Retrying attempt to schedule task at remote node. Try again "
                   "on a local node. Error: "
                << status.ToString();

            RequestNewWorkerIfNeeded(scheduling_key);

          } else {
						if(priority_task_queues_not_pushed_.contains(pri)){
							priority_task_queues_.emplace(pri);
						}
            if (status.IsGrpcUnavailable()) {
							RAY_CHECK(false) << "Jae you should handle worker lease gRPC unavailable"; // TODO(Jae) Handle this case for DFS patch
              RAY_LOG(WARNING)
                  << "The worker failed to receive a response from the local "
                  << "raylet because the raylet is unavailable (crashed). "
                  << "Error: " << status;
              if (worker_type_ == WorkerType::WORKER) {
                // Exit the worker so that caller can retry somewhere else.
                RAY_LOG(WARNING) << "Terminating the worker due to local raylet death";
                QuickExit();
              }
              RAY_CHECK(worker_type_ == WorkerType::DRIVER);
              error_type = rpc::ErrorType::LOCAL_RAYLET_DIED;
              error_status = status;
              auto &task_priority_queue = scheduling_key_entry.task_priority_queue;
              while (!task_priority_queue.empty()) {
                const auto task_key_it = task_priority_queue.begin();
                const auto &task_id = task_key_it->second;
                const auto task_it = tasks_.find(task_id);
                RAY_CHECK(task_it != tasks_.end());
                task_priority_queue.erase(task_key_it);
                tasks_to_fail.push_back(std::move(task_it->second.task_spec));
                tasks_.erase(task_it);
              }
              //tasks_to_fail = std::move(scheduling_key_entry.task_queue);
              //scheduling_key_entry.task_queue.clear();
              if (scheduling_key_entry.CanDelete()) {
                scheduling_key_entries_.erase(scheduling_key);
              }
            } else {
              RAY_LOG(WARNING)
                  << "The worker failed to receive a response from the local raylet, but "
                     "raylet is still alive. Try again on a local node. Error: "
                  << status;
              // TODO(sang): Maybe we should raise FATAL error if it happens too many
              // times.
              RequestNewWorkerIfNeeded(scheduling_key);
            }
          }
        }

        while (!tasks_to_fail.empty()) {
          auto &task_spec = tasks_to_fail.front();
          if (task_spec.IsActorCreationTask() &&
              error_type == rpc::ErrorType::TASK_PLACEMENT_GROUP_REMOVED) {
            RAY_UNUSED(task_finisher_->FailPendingTask(
                task_spec.TaskId(),
                rpc::ErrorType::ACTOR_PLACEMENT_GROUP_REMOVED,
                &error_status,
                &error_info));
          } else {
            RAY_UNUSED(task_finisher_->FailPendingTask(
                task_spec.TaskId(), error_type, &error_status, &error_info));
          }
          tasks_to_fail.pop_front();
        }
      },
      queue_size,
      is_selected_based_on_locality);
  scheduling_key_entry.pending_lease_requests.emplace(task_id, *raylet_address);
  ReportWorkerBacklogIfNeeded(scheduling_key);
}

void CoreWorkerDirectTaskSubmitter::PushNormalTask(
    const rpc::WorkerAddress &addr,
    rpc::CoreWorkerClientInterface &client,
    const SchedulingKey &scheduling_key,
    const TaskSpecification &task_spec,
    const google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry> &assigned_resources) {
  RAY_LOG(DEBUG) << "Pushing task " << task_spec.TaskId() << " to worker "
                 << addr.worker_id << " of raylet " << addr.raylet_id
                 << " with priority:" << task_spec.GetPriority() 
								 << " with num_arg:" << task_spec.NumArgs();
  LogLeaseSeq(task_spec.TaskId(), task_spec.GetName(), task_spec.GetPriority(), addr.raylet_id);

  auto task_id = task_spec.TaskId();
  auto request = std::make_unique<rpc::PushTaskRequest>();
  bool is_actor = task_spec.IsActorTask();
  bool is_actor_creation = task_spec.IsActorCreationTask();

  // NOTE(swang): CopyFrom is needed because if we use Swap here and the task
  // fails, then the task data will be gone when the TaskManager attempts to
  // access the task.
  request->mutable_task_spec()->CopyFrom(task_spec.GetMessage());
  auto msg_priority = request->mutable_task_spec()->mutable_priority();
  msg_priority->Clear();
  Priority pri = task_spec.GetPriority();
  for (auto &s : pri.score) {
    msg_priority->Add(s);
  }

  request->mutable_resource_mapping()->CopyFrom(assigned_resources);
  request->set_intended_worker_id(addr.worker_id.Binary());
  task_finisher_->MarkTaskWaitingForExecution(task_id);
  client.PushNormalTask(
      std::move(request),
      [this,
       pri,
       task_id,
       is_actor,
       is_actor_creation,
       scheduling_key,
       addr,
       assigned_resources](Status status, const rpc::PushTaskReply &reply) {
        {
          RAY_LOG(DEBUG) << "Task " << task_id << " finished from worker "
                         << addr.worker_id << " of raylet " << addr.raylet_id;
          absl::MutexLock lock(&mu_);
          executing_tasks_.erase(task_id);

          // Decrement the number of tasks in flight to the worker
          auto &lease_entry = worker_to_lease_entry_[addr];
          RAY_CHECK(lease_entry.is_busy);
          lease_entry.is_busy = false;

          // Decrement the total number of tasks in flight to any worker with the current
          // scheduling_key.
          RAY_CHECK_GE(active_workers_.size(), 1u);
          RAY_CHECK_GE(num_busy_workers_, 1u);
          num_busy_workers_--;

          if (!status.ok() || !is_actor_creation || reply.worker_exiting()) {
            // Successful actor creation leases the worker indefinitely from the raylet.
            OnWorkerIdle(addr,
                         scheduling_key,
                         /*error=*/!status.ok(),
                         /*worker_exiting=*/reply.worker_exiting(),
                         assigned_resources);
          }
        }

        if (!status.ok()) {
          // TODO: It'd be nice to differentiate here between process vs node
          // failure (e.g., by contacting the raylet). If it was a process
          // failure, it may have been an application-level error and it may
          // not make sense to retry the task.
          RAY_UNUSED(task_finisher_->FailOrRetryPendingTask(
              task_id,
              is_actor ? rpc::ErrorType::ACTOR_DIED : rpc::ErrorType::WORKER_DIED,
              &status));
        } else {
          if (!priority_to_task_spec_[pri].GetMessage().retry_exceptions() || !reply.is_retryable_error() ||
              !task_finisher_->RetryTaskIfPossible(task_id)) {
            task_finisher_->CompletePendingTask(task_id, reply, addr.ToProto());
          }
		  		priority_to_task_spec_.erase(pri);
        }
      });
}

Status CoreWorkerDirectTaskSubmitter::CancelTask(TaskSpecification task_spec,
                                                 bool force_kill,
                                                 bool recursive) {
  RAY_LOG(INFO) << "Cancelling a task: " << task_spec.TaskId()
                << " force_kill: " << force_kill << " recursive: " << recursive;
	RAY_CHECK(false) << "Jae you should handle this function. Get tasks from priority queue not by scheduling key";
  const SchedulingKey scheduling_key(
      task_spec.GetSchedulingClass(),
      task_spec.GetDependencyIds(),
      task_spec.IsActorCreationTask() ? task_spec.ActorCreationId() : ActorID::Nil(),
      task_spec.GetRuntimeEnvHash());
  std::shared_ptr<rpc::CoreWorkerClientInterface> client = nullptr;
  {
    absl::MutexLock lock(&mu_);
    if (cancelled_tasks_.find(task_spec.TaskId()) != cancelled_tasks_.end() ||
        !task_finisher_->MarkTaskCanceled(task_spec.TaskId())) {
      return Status::OK();
    }

    auto &scheduling_key_entry = scheduling_key_entries_[scheduling_key];
    auto &current_queue = scheduling_key_entry.task_priority_queue;
    // This cancels tasks that have completed dependencies and are awaiting
    // a worker lease.
    if (!current_queue.empty()) {
      for (auto task_key_it = current_queue.begin(); task_key_it != current_queue.end(); task_key_it++) {
	    const auto &task_id = task_key_it->second;
        if (task_id == task_spec.TaskId()) {
          current_queue.erase(task_key_it);

          if (current_queue.empty()) {
            CancelWorkerLeaseIfNeeded(scheduling_key);
          }
          RAY_UNUSED(task_finisher_->FailOrRetryPendingTask(
              task_id, rpc::ErrorType::TASK_CANCELLED, nullptr));
          return Status::OK();
        }
	  }
    }

    // This will get removed either when the RPC call to cancel is returned
    // or when all dependencies are resolved.
    RAY_CHECK(cancelled_tasks_.emplace(task_spec.TaskId()).second);
    auto rpc_client = executing_tasks_.find(task_spec.TaskId());

    if (rpc_client == executing_tasks_.end()) {
      // This case is reached for tasks that have unresolved dependencies.
      // No executing tasks, so cancelling is a noop.
      if (scheduling_key_entry.CanDelete()) {
        // We can safely remove the entry keyed by scheduling_key from the
        // scheduling_key_entries_ hashmap.
        scheduling_key_entries_.erase(scheduling_key);
      }
      return Status::OK();
    }
    // Looks for an RPC handle for the worker executing the task.
    auto maybe_client = client_cache_->GetByID(rpc_client->second.worker_id);
    if (!maybe_client.has_value()) {
      // If we don't have a connection to that worker, we can't cancel it.
      // This case is reached for tasks that have unresolved dependencies.
      return Status::OK();
    }
    client = maybe_client.value();
  }

  RAY_CHECK(client != nullptr);

  auto request = rpc::CancelTaskRequest();
  request.set_intended_task_id(task_spec.TaskId().Binary());
  request.set_force_kill(force_kill);
  request.set_recursive(recursive);
  client->CancelTask(
      request,
      [this, task_spec, scheduling_key, force_kill, recursive](
          const Status &status, const rpc::CancelTaskReply &reply) {
        absl::MutexLock lock(&mu_);
        RAY_LOG(DEBUG) << "CancelTask RPC response received for " << task_spec.TaskId()
                       << " with status " << status.ToString();
        cancelled_tasks_.erase(task_spec.TaskId());

        // Retry is not attempted if !status.ok() because force-kill may kill the worker
        // before the reply is sent.
        if (!status.ok()) {
          RAY_LOG(ERROR) << "Failed to cancel a task due to " << status.ToString();
          return;
        }

        if (!reply.attempt_succeeded()) {
          if (reply.requested_task_running()) {
            // Retry cancel request if failed.
            if (cancel_retry_timer_.has_value()) {
              if (cancel_retry_timer_->expiry().time_since_epoch() <=
                  std::chrono::high_resolution_clock::now().time_since_epoch()) {
                cancel_retry_timer_->expires_after(boost::asio::chrono::milliseconds(
                    RayConfig::instance().cancellation_retry_ms()));
              }
              cancel_retry_timer_->async_wait(
                  boost::bind(&CoreWorkerDirectTaskSubmitter::CancelTask,
                              this,
                              task_spec,
                              force_kill,
                              recursive));
            } else {
              RAY_LOG(ERROR)
                  << "Failed to cancel a task which is running. Stop retrying.";
            }
          } else {
            RAY_LOG(ERROR) << "Attempt to cancel task " << task_spec.TaskId()
                           << " in a worker that doesn't have this task.";
          }
        }
      });
  return Status::OK();
}

Status CoreWorkerDirectTaskSubmitter::CancelRemoteTask(const ObjectID &object_id,
                                                       const rpc::Address &worker_addr,
                                                       bool force_kill,
                                                       bool recursive) {
  auto maybe_client = client_cache_->GetByID(rpc::WorkerAddress(worker_addr).worker_id);

  if (!maybe_client.has_value()) {
    return Status::Invalid("No remote worker found");
  }
  auto client = maybe_client.value();
  auto request = rpc::RemoteCancelTaskRequest();
  request.set_force_kill(force_kill);
  request.set_recursive(recursive);
  request.set_remote_object_id(object_id.Binary());
  client->RemoteCancelTask(request, nullptr);
  return Status::OK();
}

void CoreWorkerDirectTaskSubmitter::UpdateTaskPriorities(const absl::flat_hash_map<TaskID, Priority> &priorities) {
  absl::MutexLock lock(&mu_);
  for (const auto &pair : priorities) {
    const auto &task_id = pair.first;
    const auto &priority = pair.second;
    auto task_it = tasks_.find(task_id);
    if (task_it == tasks_.end()) {
      continue;
    }
    const auto &task_key = task_it->second.task_key;
    if (priority < task_key.first) {
      auto it = scheduling_key_entries_.find(task_it->second.key);
      RAY_CHECK(it != scheduling_key_entries_.end());
      RAY_CHECK(it->second.task_priority_queue.erase(task_key)) << task_id;

      task_it->second.task_key = std::make_pair(priority, task_id);
      RAY_CHECK(it->second.task_priority_queue.emplace(task_it->second.task_key).second);
      RAY_LOG(DEBUG) << "Updated task " << task_id << " priority to " << priority;
    }
  }
}

}  // namespace core
}  // namespace ray
