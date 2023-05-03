#pragma once

#include <cstddef>
#include <vector>
#include <limits.h>
#include <ostream>
#include <boost/container_hash/hash.hpp>

#include "ray/common/id.h"
#include "ray/util/logging.h"

#define MAX_PRIORITY LONG_MAX

namespace ray {

struct Priority {
 public:
  Priority() : Priority(0) {}

  Priority(int64_t depth) {
    extend(depth + 1);
  }

  Priority(const std::vector<int64_t> &s) : score(s) {}

  void extend(int64_t size) const;

  void shorten(int64_t size) const;

  void SetFromParentPriority(Priority &parent, int64_t);

  bool operator==(const Priority &rhs) const {
	if(score.size() != rhs.score.size())
	  return false;
	for(long unsigned int i=0; i<score.size(); i++)
	  if(score[i] != rhs.score[i])
	    return false;
    return true;
  }

  bool operator!=(const Priority &rhs) const {
    return !(*this == rhs);
  }

  bool operator<(const Priority &rhs) const;

  bool operator<=(const Priority &rhs) const;

  bool operator>(const Priority &rhs) const;

  bool operator>=(const Priority &rhs) const;

  int64_t GetScore(int64_t depth) const {
    extend(depth + 1);
    return score[depth];
  }

  int GetDepth() {
	return (int)score.size();
  }

  void Set(const std::vector<int64_t> &s){
		score = s;
  }

  //This is for ensemble serve patch. When setting task's current_task_priority_ (aggregator) 
  void Set(Priority &pri){
    size_t len = pri.GetDepth();
    score.resize(len);
    for (size_t i=0; i<len; i++){
      score[i] = pri.GetScore(i);
    }
  }

  void SetScore(int64_t depth, int64_t s) {
    extend(depth + 1);
    RAY_CHECK(score[depth] >= s);
    score[depth] = s;
  }

  size_t Hash() const {
    auto end_it = score.end();
    // Find the last non-null element in the vector.
    while (end_it != score.begin()) {
      end_it--;
      if (*end_it != MAX_PRIORITY) {
        // Advance iterator so that the hash includes the last non-null
        // element.
        end_it++;
        break;
      }
    }
    size_t seed = 0;
    for (auto it = score.begin(); it != end_it; it++) {
      boost::hash_combine(seed, *it);
    }
    return seed;
  }

  mutable std::vector<int64_t> score = {};
};

using TaskKey = std::pair<Priority, TaskID>;

std::ostream &operator<<(std::ostream &os, const Priority &p);
std::ostream &operator<<(std::ostream &os, const TaskKey &k);

}  // namespace ray

namespace std {

template <>
struct hash<::ray::Priority> {
  size_t operator()(const ::ray::Priority &priority) const { return priority.Hash(); }
};

//template <>
//struct hash<const ::ray::Priority> {
//  size_t operator()(const Priority &priority) const { return priority.Hash(); }
//};

}  // namespace std