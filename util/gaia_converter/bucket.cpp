#include "bucket.hpp"
#include <cstring>
#include <cerrno>
#include <sstream>
#include <iomanip>
#include <iostream>

BucketWriter::BucketWriter(int n_buckets, int zones_per_bucket, const std::string& bucket_dir)
	: n_buckets_(n_buckets), zones_per_bucket_(zones_per_bucket), bucket_dir_(bucket_dir)
{
	buckets_.reserve(n_buckets);
	for (int b = 0; b < n_buckets; ++b) {
		auto bk = std::make_unique<Bucket>();
		std::ostringstream oss;
		oss << bucket_dir << "/bucket_" << std::setw(4) << std::setfill('0') << b << ".dat";
		bk->path = oss.str();
		bk->file = nullptr;
		buckets_.push_back(std::move(bk));
	}
}

BucketWriter::~BucketWriter() {
	finish();
}

void BucketWriter::push(const BucketRecord& rec) {
	int b = static_cast<int>(rec.zone) / zones_per_bucket_;
	if (b >= n_buckets_) b = n_buckets_ - 1;
	auto& bk = *buckets_[b];

	std::lock_guard<std::mutex> lock(bk.mtx);
	if (!bk.file) {
		bk.file = std::fopen(bk.path.c_str(), "ab");
		if (!bk.file) {
			std::cerr << "ERROR: cannot open " << bk.path << ": " << std::strerror(errno) << "\n";
			std::exit(1);
		}
	}
	std::fwrite(&rec, sizeof(BucketRecord), 1, bk.file);
}

void BucketWriter::finish() {
	for (auto& bk : buckets_) {
		std::lock_guard<std::mutex> lock(bk->mtx);
		if (bk->file) {
			std::fclose(bk->file);
			bk->file = nullptr;
		}
	}
}

std::vector<std::string> BucketWriter::bucket_paths() const {
	std::vector<std::string> paths;
	for (const auto& bk : buckets_)
		paths.push_back(bk->path);
	return paths;
}
