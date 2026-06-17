#include "bucket.hpp"
#include <cstring>
#include <cerrno>
#include <algorithm>
#include <sstream>
#include <iomanip>
#include <iostream>
#include <chrono>

BucketWriter::BucketWriter(int n_buckets, int zones_per_bucket, const std::string& bucket_dir,
			   int ring_size_mb, int n_flushers)
	: n_buckets_(n_buckets), zones_per_bucket_(zones_per_bucket),
	  n_flushers_(n_flushers), bucket_dir_(bucket_dir)
{
	buckets_.reserve(n_buckets);
	for (int b = 0; b < n_buckets; ++b) {
		auto bk = std::make_unique<Bucket>();
		std::ostringstream oss;
		oss << bucket_dir << "/bucket_" << std::setw(4) << std::setfill('0') << b << ".dat";
		bk->path = oss.str();
		bk->file = nullptr;
		size_t ring_bytes = static_cast<size_t>(ring_size_mb) * 1024 * 1024;
		bk->ring.seg_size = ring_bytes / 64;
		bk->ring.data = new uint8_t[ring_bytes];
		buckets_.push_back(std::move(bk));
	}
	int per_flusher = (n_buckets + n_flushers - 1) / n_flushers;
	for (int f = 0; f < n_flushers; ++f) {
		int start = f * per_flusher;
		int end = std::min(start + per_flusher, n_buckets);
		flushers_.emplace_back(&BucketWriter::flusher_loop, this, start, end);
	}
}

BucketWriter::~BucketWriter() {
	if (!finished_.load(std::memory_order_acquire)) finish();
}

void BucketWriter::push(const BucketRecord& rec) {
	int b = static_cast<int>(rec.zone) / zones_per_bucket_;
	if (b >= n_buckets_) b = n_buckets_ - 1;
	auto& ring = buckets_[b]->ring;

	uint64_t tail = ring.tail.load(std::memory_order_relaxed);
	const size_t mask = (64 * ring.seg_size) - 1;
	const size_t rec_size = sizeof(BucketRecord);

	int spins = 0;
	while (true) {
		uint64_t head = ring.head.load(std::memory_order_acquire);
		if (tail - head + rec_size + sizeof(uint32_t) <= 64 * ring.seg_size)
			break;
		if (++spins > 1000) {
			static std::atomic<bool> warned{false};
			if (!warned.exchange(true))
				std::cerr << "WARNING: ring buffer full (bucket " << b
					  << "), disk write may be too slow\n";
		}
		std::this_thread::sleep_for(std::chrono::microseconds(100));
	}

	size_t off = tail & mask;
	uint32_t sz = static_cast<uint32_t>(rec_size);
	std::memcpy(ring.data + off, &sz, sizeof(sz));
	std::memcpy(ring.data + ((off + sizeof(sz)) & mask), &rec, rec_size);
	ring.tail.store(tail + sizeof(sz) + rec_size, std::memory_order_release);
}

void BucketWriter::finish() {
	if (finished_.exchange(true)) return;
	for (auto& bk : buckets_)
		bk->done = true;
	for (auto& f : flushers_)
		if (f.joinable()) f.join();
	for (auto& bk : buckets_) {
		if (bk->file) { std::fclose(bk->file); bk->file = nullptr; }
		delete[] bk->ring.data;
	}
}

std::vector<std::string> BucketWriter::bucket_paths() const {
	std::vector<std::string> paths;
	for (const auto& bk : buckets_)
		paths.push_back(bk->path);
	return paths;
}

void BucketWriter::open_bucket_file(Bucket* bk) {
	if (bk->file) return;
	while (open_count_ >= MAX_OPEN_FILES)
		close_lru_file();
	bk->file = std::fopen(bk->path.c_str(), "ab");  // append mode
	if (!bk->file) {
		std::cerr << "ERROR: cannot open bucket file " << bk->path << ": " << std::strerror(errno) << "\n";
		std::exit(1);
	}
	open_count_++;
	bk->last_used = clock_++;
}

void BucketWriter::close_lru_file() {
	int oldest = 0x7fffffff;
	Bucket* victim = nullptr;
	for (auto& bk : buckets_) {
		if (bk->file && bk->last_used < oldest) {
			oldest = bk->last_used;
			victim = bk.get();
		}
	}
	if (victim) {
		std::fclose(victim->file);
		victim->file = nullptr;
		open_count_--;
	}
}

void BucketWriter::flusher_loop(int start_bucket, int end_bucket) {
	const size_t mask = (64 * buckets_[0]->ring.seg_size) - 1;
	uint8_t local_buf[65536];

	while (true) {
		bool all_empty = true;
		for (int bi = start_bucket; bi < end_bucket; ++bi) {
			auto& bk = buckets_[bi];
			auto& ring = bk->ring;
			uint64_t tail = ring.tail.load(std::memory_order_acquire);
			uint64_t head_val = ring.head.load(std::memory_order_relaxed);

			if (head_val < tail) {
				all_empty = false;
				open_bucket_file(bk.get());
			}

			while (head_val < tail) {
				size_t off = head_val & mask;
				uint32_t sz;
				std::memcpy(&sz, ring.data + off, sizeof(sz));
				size_t total = sizeof(sz) + sz;
				if (off + total <= 64 * ring.seg_size) {
					std::memcpy(local_buf, ring.data + off, total);
				} else {
					size_t first = 64 * ring.seg_size - off;
					std::memcpy(local_buf, ring.data + off, first);
					std::memcpy(local_buf + first, ring.data, total - first);
				}
				head_val += total;
				ring.head.store(head_val, std::memory_order_release);

				std::lock_guard<std::mutex> lock(bk->write_mutex);
				std::fwrite(local_buf + sizeof(uint32_t), 1, sz, bk->file);
			}
		}

		if (all_empty) {
			bool any_done = true;
			for (auto& bk : buckets_) {
				if (!bk->done) { any_done = false; break; }
			}
			if (any_done) break;
			std::this_thread::sleep_for(std::chrono::microseconds(100));
		}
	}
}
