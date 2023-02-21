#include "ray/common/task/task_priority.h"

namespace ray {

void Priority::extend(int64_t size) const {
  int64_t diff = size -  static_cast<int64_t>(score.size());
  if (diff > 0) {
    for (int64_t i = 0; i < diff; i++) {
      score.push_back(INT_MAX);
    }
  }
}

void Priority::shorten(int64_t size) const {
  if(size > 0)
		score.resize(score.size() - size);
}

void Priority::SetFromParentPriority(Priority &parent, int s){
  //param s id the last score to add
  if(parent.score.size() == 1 && parent.score[0] == INT_MAX){
		score[0] = s;
  }else{
		score = parent.score;
    score.push_back(s);
  }
}

bool Priority::operator<(const Priority &rhs) const {
  int64_t min_size = std::min(score.size(), rhs.score.size());
  for(int i=0; i<min_size; i++){
    if(score[i]<rhs.score[i])
      return true;
    else if(score[i]>rhs.score[i])
      return false;
  }

  return score.size() > rhs.score.size();
}

bool Priority::operator<=(const Priority &rhs) const {
  int64_t min_size = std::min(score.size(), rhs.score.size());
  for(int i=0; i<min_size; i++){
    if(score[i]<rhs.score[i])
      return true;
    else if(score[i]>rhs.score[i])
      return false;
  }
  return score.size() >= rhs.score.size();
}

bool Priority::operator>(const Priority &rhs) const {
  int64_t min_size = std::min(score.size(), rhs.score.size());
  for(int64_t i=0; i<min_size; i++){
    if(score[i]>rhs.score[i])
      return true;
    else if(score[i]<rhs.score[i])
      return false;
  }
  return score.size() < rhs.score.size();
}

bool Priority::operator>=(const Priority &rhs) const {
  int64_t min_size = std::min(score.size(), rhs.score.size());
  for(int i=0; i<min_size; i++){
    if(score[i]>rhs.score[i])
      return true;
    else if(score[i]<rhs.score[i])
      return false;
  }
  return score.size() <= rhs.score.size();
}

/*
bool Priority::operator<(const Priority &rhs) const {
  rhs.extend(score.size());
  extend(rhs.score.size());

  return score < rhs.score;
}

bool Priority::operator<=(const Priority &rhs) const {
  rhs.extend(score.size());
  extend(rhs.score.size());

  return score <= rhs.score;
}

bool Priority::operator>(const Priority &rhs) const {
  rhs.extend(score.size());
  extend(rhs.score.size());

  return score > rhs.score;
}

bool Priority::operator>=(const Priority &rhs) const {
  rhs.extend(score.size());
  extend(rhs.score.size());

  return score >= rhs.score;
}
*/

std::ostream &operator<<(std::ostream &os, const Priority &p) {
  os << "[ ";
  for (const auto &i : p.score) {
    os << i << " ";
  }
  os << "]";
  return os;
}

std::ostream &operator<<(std::ostream &os, const TaskKey &k) {
  return os << k.second << " " << k.first;
}

}  // namespace ray
