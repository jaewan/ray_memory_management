/// RSCODE: this whole file is new

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

#include "ray/object_manager/spill_remote_manager.h"

#include "ray/common/common_protocol.h"
#include "ray/stats/metric_defs.h"
#include "ray/util/util.h"

namespace ray {

void SpillRemoteManager::StartSpillRemote(const NodeID &dest_id,
                            const ObjectID &obj_id,
                            int64_t num_chunks,
                            std::function<void(int64_t)> send_chunk_fn) {
  auto spill_remote_id = std::make_pair(dest_id, obj_id);
  RAY_CHECK(num_chunks > 0);
  if (spill_remote_info_.contains(spill_remote_id)) {
    RAY_LOG(DEBUG) << "Duplicate spill remote request " << spill_remote_id.first << ", " << spill_remote_id.second
                   << ", resending all the chunks.";
    chunks_remaining_ += spill_remote_info_[spill_remote_id]->ResendAllChunks(send_chunk_fn);
  } else {
    chunks_remaining_ += num_chunks;
    spill_remote_info_[spill_remote_id].reset(new SpillRemoteState(num_chunks, send_chunk_fn));
  }
  ScheduleRemainingSpills();
}

void SpillRemoteManager::OnChunkComplete(const NodeID &dest_id, const ObjectID &obj_id, const std::function<void()> callback) {
  auto spill_remote_id = std::make_pair(dest_id, obj_id);
  chunks_in_flight_ -= 1;
  chunks_remaining_ -= 1;
  spill_remote_info_[spill_remote_id]->OnChunkComplete();
  if (spill_remote_info_[spill_remote_id]->AllChunksComplete()) {
    spill_remote_info_.erase(spill_remote_id);
    RAY_LOG(DEBUG) << "Spill remote for " << spill_remote_id.first << ", " << spill_remote_id.second
                   << " completed, remaining: " << NumPushesInFlight();

    RAY_LOG(INFO) << "About to call callback in OnChunkComplete";
    callback();
  }
  ScheduleRemainingSpills();
}

void SpillRemoteManager::ScheduleRemainingSpills() {
  bool keep_looping = true;
  // Loop over all active pushes for approximate round-robin prioritization.
  // TODO(ekl) this isn't the best implementation of round robin, we should
  // consider tracking the number of chunks active per-push and balancing those.
  while (chunks_in_flight_ < max_chunks_in_flight_ && keep_looping) {
    // Loop over each active push and try to send another chunk.
    auto it = spill_remote_info_.begin();
    keep_looping = false;
    while (it != spill_remote_info_.end() && chunks_in_flight_ < max_chunks_in_flight_) {
      auto spill_remote_id = it->first;
      auto &info = it->second;
      if (info->SendOneChunk()) {
        chunks_in_flight_ += 1;
        keep_looping = true;
        RAY_LOG(DEBUG) << "Sending chunk " << info->next_chunk_id << " of "
                       << info->num_chunks << " for spill remote " << spill_remote_id.first << ", "
                       << spill_remote_id.second << ", chunks in flight " << NumChunksInFlight()
                       << " / " << max_chunks_in_flight_
                       << " max, remaining chunks: " << NumChunksRemaining();
      }
      it++;
    }
  }
}

void SpillRemoteManager::RecordMetrics() const {
  /// RSTODO: Declare spill remote manager stats here later
  ray::stats::STATS_push_manager_in_flight_pushes.Record(NumPushesInFlight());
  ray::stats::STATS_push_manager_chunks.Record(NumChunksInFlight(), "InFlight");
  ray::stats::STATS_push_manager_chunks.Record(NumChunksRemaining(), "Remaining");
}

std::string SpillRemoteManager::DebugString() const {
  std::stringstream result;
  result << "SpillRemoteManager:";
  result << "\n- num pushes in flight: " << NumPushesInFlight();
  result << "\n- num chunks in flight: " << NumChunksInFlight();
  result << "\n- num chunks remaining: " << NumChunksRemaining();
  result << "\n- max chunks allowed: " << max_chunks_in_flight_;
  return result.str();
}

}  // namespace ray
