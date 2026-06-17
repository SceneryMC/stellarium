#include "bucket.hpp"
#include <cstring>
#include <algorithm>
#include <sstream>
#include <iomanip>
#include <iostream>

BucketWriter::BucketWriter(int n_buckets, int zones_per_bucket, const std::string& bucket_dir,
			   int ring_size_mb)
	: n_buckets_(n_buckets), zones_per_bucket_(zones_per_bucket), bucket_dir_(bucket_dir)
{
	buckets_.resize(n_buckets);
	for (int b = 0; b < n_buckets; ++b) {
		auto& bk = buckets_[b];
		// bucket file
		std::ostringstream oss;
		oss << bucket_dir << "/bucket_" << std::setw(4) << std::setfill('0') << b << ".dat";
		bk.path = oss.str();
		bk.file = std::fopen(bk.path.c_str(), "wb");
		if (!bk.file) {
			std::cerr << "ERROR: cannot open bucket file " << bk.path << "\n";
			std::exit(1);
		}
		// ring buffer
		size_t ring_bytes = static_cast<size_t>(ring_size_mb) * 1024 * 1024;
		bk.ring.seg_size = ring_bytes / RingBuffer::SEGMENTS;
		bk.ring.data = new uint8_t[ring_bytes];
		// start flusher
		bk.flusher = std::thread(flusher_thread, &bk);
	}
}

BucketWriter::~BucketWriter() {
	if (!finished_) finish();
}

void BucketWriter::push(const BucketRecord& rec) {
	int b = static_cast<int>(rec.zone) / zones_per_bucket_;
	if (b >= n_buckets_) b = n_buckets_ - 1;
	auto& bk = buckets_[b];
	auto& ring = bk.ring;

	uint64_t tail = ring.tail.load(std::memory_order_relaxed);
	uint64_t head;
	const size_t mask = (RingBuffer::SEGMENTS * ring.seg_size) - 1;
	const size_t rec_size = sizeof(BucketRecord);

	// Spin until space available
	while (true) {
		head = ring.head.load(std::memory_order_acquire);
		uint64_t used = tail - head;
		if (used + rec_size + sizeof(uint32_t) <= RingBuffer::SEGMENTS * ring.seg_size)
			break;
		std::this_thread::yield();
	}

	size_t off = tail & mask;
	// Write record size prefix
	uint32_t sz = static_cast<uint32_t>(rec_size);
	std::memcpy(ring.data + off, &sz, sizeof(sz));
	off = (off + sizeof(sz)) & mask;
	std::memcpy(ring.data + off, &rec, rec_size);
	ring.tail.store(tail + sizeof(sz) + rec_size, std::memory_order_release);
}

void BucketWriter::finish() {
	if (finished_) return;
	finished_ = true;
	for (auto& bk : buckets_) {
		bk.done.store(true, std::memory_order_release);
		if (bk.flusher.joinable())
			bk.flusher.join();
		std::fclose(bk.file);
		delete[] bk.ring.data;
	}
}

std::vector<std::string> BucketWriter::bucket_paths() const {
	std::vector<std::string> paths;
	for (const auto& bk : buckets_)
		paths.push_back(bk.path);
	return paths;
}

void BucketWriter::flusher_thread(Bucket* bk) {
	auto& ring = bk->ring;
	const size_t mask = (RingBuffer::SEGMENTS * ring.seg_size) - 1;
	uint8_t local_buf[65536];

	while (true) {
		uint64_t tail = ring.tail.load(std::memory_order_acquire);
		uint64_t head_val = ring.head.load(std::memory_order_relaxed);

		while (head_val < tail) {
			size_t off = head_val & mask;
			uint32_t sz;
			std::memcpy(&sz, ring.data + off, sizeof(sz));
			size_t total = sizeof(sz) + sz;
			// Copy out of ring buffer (may wrap)
			if (off + total <= RingBuffer::SEGMENTS * ring.seg_size) {
				std::memcpy(local_buf, ring.data + off, total);
			} else {
				size_t first = RingBuffer::SEGMENTS * ring.seg_size - off;
				std::memcpy(local_buf, ring.data + off, first);
				std::memcpy(local_buf + first, ring.data, total - first);
			}
			head_val += total;
			ring.head.store(head_val, std::memory_order_release);

			// Write record to disk
			std::lock_guard<std::mutex> lock(bk->write_mutex);
			std::fwrite(local_buf + sizeof(uint32_t), 1, sz, bk->file);
			bk->bytes_written += sz;
		}

		if (bk->done.load(std::memory_order_acquire) && head_val == ring.tail.load(std::memory_order_acquire))
			break;
		std::this_thread::sleep_for(std::chrono::microseconds(50));
	}
}
