// SkyChart Gaia .dat → Stellarium .cat converter
// Usage: gaia_converter --skychart <dir> --out-dir <dir> [--workers <n>]
//
// Pass 1: Scan .dat files in parallel → compute Vmag, B-V, zone → push to bucket files
// Pass 2: Sort each bucket in parallel → write .cat

#include "types.hpp"
#include "convert.hpp"
#include "geodesic.hpp"
#include "bucket.hpp"
#include "cat_writer.hpp"

#include <cstdio>
#include <cstring>
#include <cmath>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <iostream>
#include <algorithm>
#include <filesystem>

namespace fs = std::filesystem;

struct LevelConfig {
	std::string name;
	int         level;
	double      mag_lo;   // V magnitude lower bound
	double      mag_hi;   // V magnitude upper bound
	int         n_buckets;
};

struct Pass1Result {
	std::vector<uint32_t> counts;
	std::vector<std::string> bucket_paths;
};

// Scan one .dat file and push records to bucket writers
static void process_dat_file(
	const std::string& path,
	const std::vector<LevelConfig>& levels,
	std::vector<BucketWriter*>& bucket_writers,
	std::vector<std::vector<uint32_t>>& all_counts)
{
	FILE* f = std::fopen(path.c_str(), "rb");
	if (!f) return;

	std::fseek(f, 0, SEEK_END);
	size_t fsize = static_cast<size_t>(std::ftell(f));
	std::fseek(f, 0, SEEK_SET);

	std::vector<uint8_t> data(fsize);
	size_t nread = std::fread(data.data(), 1, fsize, f);
	std::fclose(f);
	if (nread != fsize) return;

	size_t n_rec = fsize / sizeof(SkyChartRecord);
	const auto* records = reinterpret_cast<const SkyChartRecord*>(data.data());

	for (size_t i = 0; i < n_rec; ++i) {
		const auto& r = records[i];
		if (r.gaia_id == 0) continue;

		double ra  = r.ra_raw  / 3600000.0;
		double dec = r.dec_raw / 3600000.0 - 90.0;
		double g   = r.g_mag / 1000.0;
		double bp  = r.bp_mag / 1000.0;
		double rp  = r.rp_mag / 1000.0;

		// BP/RP may be invalid for very bright or very faint stars
		bool have_color = (std::abs(r.bp_mag) < 30000 && std::abs(r.rp_mag) < 30000);
		double c  = have_color ? (bp - rp) : NAN;
		double v  = g_to_v(g, c);
		double bv = bp_rp_to_bv(c);

		// Route to correct level
		for (size_t li = 0; li < levels.size(); ++li) {
			const auto& lv = levels[li];
			if (v < lv.mag_lo || v >= lv.mag_hi) continue;

			double ra_rad  = ra  * M_PI / 180.0;
			double dec_rad = dec * M_PI / 180.0;
			double x = std::cos(ra_rad) * std::cos(dec_rad);
			double y = std::sin(ra_rad) * std::cos(dec_rad);
			double z = std::sin(dec_rad);

			int zone = zone_number(x, y, z, lv.level);

			// Update count
			all_counts[li][zone]++;

			// Build bucket record
			BucketRecord brec{};
			brec.zone    = static_cast<uint32_t>(zone);
			brec.vmag    = static_cast<int16_t>(std::round(v * 1000.0));
			brec.bv      = static_cast<int16_t>(std::round(bv * 1000.0));
			brec.ra_i    = static_cast<int32_t>(std::round(ra * 3600000.0));
			brec.dec_i   = static_cast<int32_t>(std::round(dec * 3600000.0));
			brec.gaia_id = r.gaia_id;
			brec.pmra_i  = static_cast<int32_t>(std::round(r.pmra * 1000.0));
			brec.pmdec_i = static_cast<int32_t>(std::round(r.pmdec * 1000.0));
			brec.plx_i   = static_cast<int32_t>(std::round(r.plx  * 100.0));

			bucket_writers[li]->push(brec);
			break;  // star belongs to exactly one level
		}
	}
}

int main(int argc, char** argv) {
	std::string skychart_dir;
	std::string out_dir;
	std::string work_dir;
	int n_workers = std::thread::hardware_concurrency();

	// Parse args
	for (int i = 1; i < argc; ++i) {
		std::string arg = argv[i];
		if (arg == "--skychart" && i+1 < argc) skychart_dir = argv[++i];
		else if (arg == "--out-dir" && i+1 < argc)  out_dir = argv[++i];
		else if (arg == "--work-dir" && i+1 < argc) work_dir = argv[++i];
		else if (arg == "--workers" && i+1 < argc)  n_workers = std::stoi(argv[++i]);
		else {
			std::cerr << "Usage: gaia_converter --skychart <dir> --out-dir <dir> [--workers <n>]\n";
			return 1;
		}
	}
	if (skychart_dir.empty() || out_dir.empty()) {
		std::cerr << "Usage: gaia_converter --skychart <dir> --out-dir <dir> [--workers <n>]\n";
		return 1;
	}
	if (work_dir.empty()) work_dir = out_dir;

	fs::create_directories(out_dir);
	fs::create_directories(work_dir);

	// Level configuration
	std::vector<LevelConfig> levels = {
		{"stars_8",  8, 16.75, 18.50, 256},
		{"stars_9",  9, 18.50, 20.25, 512},
		{"stars_10", 10, 20.25, 23.00, 512},
	};

	// Discover .dat files
	std::vector<std::string> dat_files;
	for (const auto& sub : {"gaia1", "gaia2", "gaia3", "gaia4"}) {
		auto subdir = fs::path(skychart_dir) / sub;
		if (!fs::is_directory(subdir)) continue;
		for (const auto& entry : fs::directory_iterator(subdir)) {
			if (entry.is_directory()) {
				for (const auto& f : fs::directory_iterator(entry.path())) {
					if (f.path().extension() == ".dat")
						dat_files.push_back(f.path().string());
				}
			}
		}
	}
	std::cout << "Found " << dat_files.size() << " SkyChart .dat files\n";

	// Per-level zone counts
	std::vector<std::vector<uint32_t>> all_counts;
	for (const auto& lv : levels) {
		all_counts.emplace_back(nr_of_zones(lv.level), 0);
	}

	// ── PASS 1: scan + bucket ──
	std::cout << "\n===== PASS 1: Scanning " << dat_files.size() << " files ("
		  << n_workers << " workers) =====\n";

	// Create bucket writers (one per level, buckets per level)
	std::vector<BucketWriter*> bucket_writers;
	for (size_t li = 0; li < levels.size(); ++li) {
		const auto& lv = levels[li];
		auto bucket_dir = fs::path(work_dir) / (lv.name + "_buckets");
		fs::create_directories(bucket_dir);
		int zones_per_bucket = (nr_of_zones(lv.level) + lv.n_buckets - 1) / lv.n_buckets;
		auto bw = new BucketWriter(lv.n_buckets, zones_per_bucket, bucket_dir.string(), 16);
		bucket_writers.push_back(bw);
	}

	// Thread pool for scanning
	std::mutex file_mutex;
	std::atomic<int> next_file{0};
	std::vector<std::thread> workers;
	for (int t = 0; t < n_workers; ++t) {
		workers.emplace_back([&]() {
			while (true) {
				int fi = next_file.fetch_add(1);
				if (fi >= static_cast<int>(dat_files.size())) break;
				process_dat_file(dat_files[fi], levels, bucket_writers, all_counts);
			}
		});
	}

	// Progress reporter
	std::thread progress([&]() {
		while (next_file.load() < static_cast<int>(dat_files.size())) {
			std::this_thread::sleep_for(std::chrono::seconds(2));
			int done = next_file.load();
			std::cout << "  [" << done << "/" << dat_files.size() << "] "
				  << (100.0*done/dat_files.size()) << "%\n";
		}
	});

	for (auto& w : workers) w.join();
	progress.join();

	// Finish bucket writers
	for (auto* bw : bucket_writers) bw->finish();
	std::cout << "PASS 1 complete.\n";

	// Print counts
	for (size_t li = 0; li < levels.size(); ++li) {
		const auto& lv = levels[li];
		uint64_t total = 0;
		int non_empty = 0;
		for (auto c : all_counts[li]) { total += c; if (c > 0) non_empty++; }
		std::cout << "  " << lv.name << ": " << total << " stars, " << non_empty << " non-empty zones\n";
	}

	// ── PASS 2: sort + write .cat ──
	for (size_t li = 0; li < levels.size(); ++li) {
		const auto& lv = levels[li];
		auto paths = bucket_writers[li]->bucket_paths();
		int mag_min = static_cast<int>(lv.mag_lo * 1000.0);
		std::string out_path = out_dir + "/" + lv.name + "_1v0_1.cat";

		write_cat(paths, all_counts[li], lv.level, mag_min, out_path, n_workers);

		// Clean up bucket files
		for (const auto& p : paths) fs::remove(p);
	}

	// Clean up
	for (auto* bw : bucket_writers) { bw->finish(); delete bw; }

	std::cout << "\nDone.\n";
	return 0;
}
