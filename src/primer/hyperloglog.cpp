#include "primer/hyperloglog.h"

namespace bustub {

const double CONST = 0.79402 ;

std::mutex mu;

template <typename KeyType>
HyperLogLog<KeyType>::HyperLogLog(int16_t n_bits) : cardinality_(0) {
  n_bits_ = n_bits > 0 ? n_bits : 0;
  buckets_.resize(1 << n_bits_);
}

template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeBinary(const hash_t &hash) const -> std::bitset<BITSET_CAPACITY> {
  return {hash};
}

template <typename KeyType>
auto HyperLogLog<KeyType>::PositionOfLeftmostOne(const std::bitset<BITSET_CAPACITY> &bset) const -> uint64_t {
  for (int i = BITSET_CAPACITY - 1 - n_bits_; i > -1; i--) {
    if (bset[i]) {
      return BITSET_CAPACITY - i;
    }
  }
  return 0;
}

template <typename KeyType>
auto HyperLogLog<KeyType>::AddElem(KeyType val) -> void {
  auto h = CalculateHash(val);
  auto bset = std::move(ComputeBinary(h));
  auto msb = PositionOfLeftmostOne(bset);
  auto register_id = n_bits_ == 0 ? 0 : h >> (BITSET_CAPACITY - n_bits_);
  std::unique_lock lk(mu);
  buckets_[register_id] = std::max(buckets_[register_id], msb - n_bits_);
}

template <typename KeyType>
auto HyperLogLog<KeyType>::ComputeCardinality() -> void {
  auto size = buckets_.size();
  double sum = 0;
  for (uint64_t i = 0; i < size; i++) {
    sum += std::pow(2, - static_cast<int64_t>(buckets_[i]));
  }
  cardinality_ = std::floor(CONST * size * size / sum);
}

template class HyperLogLog<int64_t>;
template class HyperLogLog<std::string>;

}  // namespace bustub
