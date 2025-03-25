//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

const uint64_t INF = -1ULL;

LRUKNode::LRUKNode() : k_(0), fid_(0) {}

LRUKNode::LRUKNode(size_t k, frame_id_t fid) : k_(k), fid_(fid) {}

auto LRUKNode::size() -> size_t {
  return history_.size();
}

auto LRUKNode::end() -> size_t {
  return history_.back();
}

auto LRUKNode::getK() -> size_t {
  return k_;
}
auto LRUKNode::appendFront(size_t t) -> void{
  history_.push_front(t);
}

auto LRUKNode::pop() -> void {
  history_.pop_back();
}
auto LRUKNode::isEvictable() -> bool {
  return is_evictable_;
}
auto LRUKNode::setEvictable(bool e) -> void {
  is_evictable_ = e;
}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  // No page available in the buffer pool, return a null pointer
  if (curr_size_ == 0) {
    return std::nullopt;
  }
  size_t max = 0;
  frame_id_t evict_f;
  size_t last = INF;
  // For each frame in buffer pool,
  for (auto pair: node_store_) {
    auto fid = pair.first;
    auto &node = pair.second;

    // Skip frame if it is not evictable.
    if (!node.isEvictable()) {
      continue;
    }

    // Get k distance timestamp for current frame.
    auto kDist = node.size() == k_ ? current_timestamp_ - node.end(): INF;

    // Set current frame to be evicted if k distance timestamp is smaller.
    if (max < kDist) {
      max = kDist;
      evict_f = fid;
      last = node.end();
    } else if (kDist == max && max == INF && last > node.end()) {
      evict_f = fid;
      last = node.end();
    }
  }

  // Remove access history.
  Remove(evict_f);
  return evict_f;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  // Create a node if frame is not already in the buffer pool.
  if (node_store_.find(frame_id) == node_store_.end()) {
    node_store_.insert(std::make_pair(frame_id, LRUKNode(k_, frame_id)));
  }
  // Pop least recently timestamp from history if node is full.
  auto &node = node_store_[frame_id];
  if (node.size() == k_){
    node.pop();
  }
  // Insert current timestamp at the front.
  node.appendFront(current_timestamp_);
  current_timestamp_++;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  }
  auto &node = node_store_[frame_id];
  if (set_evictable ^ node.isEvictable()) {
    curr_size_ += (set_evictable == true ? 1 : -1);
    node.setEvictable(set_evictable);
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  }
  if (!node_store_[frame_id].isEvictable()) {
    throw Exception(
      fmt::format("Cannot evict frame {} evictable flag is false", frame_id),
      true
    );
  }
  node_store_.erase(frame_id);

  // Reduce number of evictable frame by 1.
  curr_size_ -= 1;
}

auto LRUKReplacer::Size() -> size_t {
  return curr_size_;
}

}  // namespace bustub
