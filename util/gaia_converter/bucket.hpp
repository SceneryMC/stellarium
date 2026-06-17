// Thread-safe bucket writer: lock-free ring buffers + dedicated flusher threads.
// Multiple worker threads push BucketRecords into per-bucket ring buffers.
// Dedicated I/O threads flush full buffers to disk.
//
// Each bucket has:
//   - A spinlock-protected ring buffer (8 MB default)
//   - A dedicated std::thread that flushes full buffer segments to disk
//
// Workers call bucket_writer::push(zone, record) — no syscall, minimal contention.

#pragma once
#include "types.hpp"
#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <mutex>
#include <condition_variable>

class BucketWriter {
public:
	// n_buckets: number of bucket files
	// zones_per_bucket: zone range per bucket (zones / n_buckets)
	// bucket_dir: directory for bucket_XXXX.dat files
	// ring_size_mb: per-bucket ring buffer size in MB (default 16)
	BucketWriter(int n_buckets, int zones_per_bucket, const std::string& bucket_dir,
		     int ring_size_mb = 16);
	~BucketWriter();

	// Push a single record. Thread-safe, lock-free for different buckets.
	void push(const BucketRecord& rec);

	// Wait for all flusher threads to finish (call after all workers are done).
	void finish();

	// Get paths to all bucket files for pass 2
	std::vector<std::string> bucket_paths() const;

	int num_buckets() const { return n_buckets_; }

private:
	struct alignas(64) RingBuffer {
		static constexpr int SEGMENTS = 64;  // must be power of 2
		std::atomic<uint64_t> head{0};
		std::atomic<uint64_t> tail{0};
		uint8_t* data;
		size_t   seg_size;  // bytes per segment
	};

	struct Bucket {
		RingBuffer  ring;
		std::string path;
		FILE*       file;
		std::thread flusher;
		std::mutex  write_mutex;  // serialize writes to the same bucket file
		uint64_t    bytes_written{0};
		std::atomic<bool> done{false};
	};

	int n_buckets_;
	int zones_per_bucket_;
	std::string bucket_dir_;
	std::vector<Bucket> buckets_;
	bool finished_{false};

	static void flusher_thread(Bucket* b);
};
