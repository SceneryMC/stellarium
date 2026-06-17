// Simple bucket writer: per-bucket mutex + FILE*
// Workers directly write to disk — no ring buffers, no flusher threads.
// Deadlocks and "too many open files" eliminated.

#pragma once
#include "types.hpp"
#include <string>
#include <vector>
#include <mutex>
#include <memory>

class BucketWriter {
public:
	BucketWriter(int n_buckets, int zones_per_bucket, const std::string& bucket_dir);
	~BucketWriter();

	void push(const BucketRecord& rec);
	void finish();
	std::vector<std::string> bucket_paths() const;
	int num_buckets() const { return n_buckets_; }

private:
	struct Bucket {
		std::string path;
		FILE*       file = nullptr;
		std::mutex  mtx;
	};

	int n_buckets_;
	int zones_per_bucket_;
	std::string bucket_dir_;
	std::vector<std::unique_ptr<Bucket>> buckets_;
};
