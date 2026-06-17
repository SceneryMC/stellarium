// Thread-safe bucket writer: lock-free ring buffers + shared flusher thread per level.
// Multiple worker threads push BucketRecords into per-bucket ring buffers.
// One dedicated I/O thread per BucketWriter flushes all its buckets.

#pragma once
#include "types.hpp"
#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <mutex>
#include <memory>

class BucketWriter {
public:
	BucketWriter(int n_buckets, int zones_per_bucket, const std::string& bucket_dir,
		     int ring_size_mb = 4);
	~BucketWriter();

	void push(const BucketRecord& rec);
	void finish();
	std::vector<std::string> bucket_paths() const;
	int num_buckets() const { return n_buckets_; }

private:
	struct alignas(64) RingBuffer {
		std::atomic<uint64_t> head{0};
		std::atomic<uint64_t> tail{0};
		uint8_t* data = nullptr;
		size_t   seg_size = 0;
	};

	struct Bucket {
		RingBuffer  ring;
		std::string path;
		FILE*       file = nullptr;
		std::mutex  write_mutex;
		bool        done = false;
		int         last_used = -1;  // for LRU eviction
	};

	int n_buckets_;
	int zones_per_bucket_;
	std::string bucket_dir_;
	std::vector<std::unique_ptr<Bucket>> buckets_;
	std::thread flusher_;
	std::atomic<bool> finished_{false};
	int open_count_ = 0;
	static constexpr int MAX_OPEN_FILES = 256;
	int clock_ = 0;

	void flusher_loop();
	void open_bucket_file(Bucket* bk);
	void close_lru_file();
};
