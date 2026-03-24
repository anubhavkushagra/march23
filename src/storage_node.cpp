/*
 * storage_node.cpp  —  RocksDB storage server
 *
 * One process per socket path.  Launch 4 instances:
 *   ./storage_node 0   →  /tmp/kv_0.sock  →  ./data/shard_0
 *   ./storage_node 1   →  /tmp/kv_1.sock  →  ./data/shard_1
 *   ./storage_node 2   →  /tmp/kv_2.sock  →  ./data/shard_2
 *   ./storage_node 3   →  /tmp/kv_3.sock  →  ./data/shard_3
 */
#include "kv.grpc.pb.h"

#include <atomic>
#include <cstdio>
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <memory>
#include <rocksdb/utilities/transaction_db.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <rocksdb/rate_limiter.h>
#include <rocksdb/slice.h>
#include <rocksdb/table.h>
#include <rocksdb/write_batch.h>
#include <string>
#include <thread>
#include <vector>

static std::atomic<long> total_batches{0};
static std::atomic<long> total_keys{0};

class StorageServiceImpl final : public kv::KVService::Service {
    rocksdb::TransactionDB* db_;
    rocksdb::WriteOptions    w_opts_;

public:
    explicit StorageServiceImpl(const std::string& data_dir) {
        rocksdb::Options opt;

        // ── Core setup ────────────────────────────────────────────────────
        opt.create_if_missing         = true;
        opt.error_if_exists           = false;

        // ── Parallelism ───────────────────────────────────────────────────
        // Use physical core count.  IncreaseParallelism sets both the
        // background thread pool and max_background_compactions/flushes.
        int cores = (int)std::thread::hardware_concurrency();
        opt.IncreaseParallelism(std::max(cores, 4));
        opt.max_background_jobs       = std::max(cores / 2, 4);

        // ── Write buffer (memtable) ────────────────────────────────────────
        // Large memtable → fewer flushes → fewer write stalls.
        // 6 buffers: while one flushes the other 5 absorb writes.
        opt.write_buffer_size                = 256ULL << 20;  // 256 MB each
        opt.max_write_buffer_number          = 6;
        opt.min_write_buffer_number_to_merge = 2;

        // ── LSM level sizing ──────────────────────────────────────────────
        // max_bytes_for_level_base = 10 × write_buffer_size is the RocksDB
        // recommended ratio to keep L0→L1 compaction cheap.
        opt.OptimizeLevelStyleCompaction(256ULL << 20);
        opt.max_bytes_for_level_base         = 2560ULL << 20; // 2.5 GB
        opt.max_bytes_for_level_multiplier   = 10;
        opt.target_file_size_base            = 64ULL << 20;   // 64 MB SST files

        // ── Compaction tuning ─────────────────────────────────────────────
        opt.disable_auto_compactions              = false;
        opt.level0_file_num_compaction_trigger    = 4;
        opt.level0_slowdown_writes_trigger        = 20;
        opt.level0_stop_writes_trigger            = 36;
        opt.compaction_style                      = rocksdb::kCompactionStyleLevel;

        // ── I/O ───────────────────────────────────────────────────────────
        opt.compression           = rocksdb::kLZ4Compression;   // fast + saves disk
        opt.bottommost_compression = rocksdb::kZSTD;            // cold data smaller
        opt.bytes_per_sync        = 1 << 20;                    // 1 MB sync chunks
        opt.wal_bytes_per_sync    = 1 << 20;

        // Rate-limit compaction I/O so it doesn't starve foreground writes.
        // Set to ~60% of your disk's sequential write speed.
        // NVMe SSD ~3 GB/s → 1800 MB/s limit.  SATA SSD ~500 MB/s → 300 MB/s.
        // Tune ROCKSDB_RATE_LIMIT_MB env var to match your hardware.
        long rate_mb = 1800;
        if (const char* e = std::getenv("ROCKSDB_RATE_LIMIT_MB"))
            rate_mb = std::atol(e);
        opt.rate_limiter.reset(rocksdb::NewGenericRateLimiter(
            rate_mb * 1024 * 1024LL, 100'000, 10));

        // ── ACID Safety vs Throughput ─────────────────────────────────────
        // unordered_write is RELAXED; we disable it for stricter ACID.
        opt.enable_pipelined_write = true;
        opt.unordered_write        = false;

        // ── Block cache & bloom filters ────────────────────────────────────
        rocksdb::BlockBasedTableOptions topt;
        topt.block_cache            = rocksdb::NewLRUCache(512ULL << 20); // 512 MB
        topt.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
        topt.cache_index_and_filter_blocks = true;
        topt.pin_l0_filter_and_index_blocks_in_cache = true;
        topt.block_size             = 16 << 10;  // 16 KB blocks
        opt.table_factory.reset(
            rocksdb::NewBlockBasedTableFactory(topt));

        rocksdb::TransactionDBOptions txn_opt;
        rocksdb::Status s = rocksdb::TransactionDB::Open(opt, txn_opt, data_dir, &db_);
        if (!s.ok()) {
            std::cerr << "TransactionDB::Open failed (" << data_dir << "): "
                      << s.ToString() << "\n";
            std::exit(1);
        }

        // ── Durability ───────────────────────────────────────────────────
        // disableWAL=false (default) + sync=true = Absolute Safety.
        // Once db->Write returns OK, it is physically written to disk.
        w_opts_.disableWAL = false;
        w_opts_.sync       = true;
        w_opts_.low_pri    = false; // High priority for safety-critical writes
    }

    ~StorageServiceImpl() { delete db_; }

    // ── Single (used only by direct clients, not by api_node) ─────────────
    grpc::Status ExecuteSingle(grpc::ServerContext*,
                               const kv::SingleRequest*  req,
                               kv::SingleResponse*       resp) override {
        rocksdb::Status s;
        if (req->type() == kv::GET) {
            std::string v;
            s = db_->Get(rocksdb::ReadOptions(), req->key(), &v);
            if (s.ok()) resp->set_value(std::move(v));
            resp->set_success(s.ok() || s.IsNotFound());
        } else if (req->type() == kv::PUT) {
            s = db_->Put(w_opts_, req->key(), req->value());
            resp->set_success(s.ok());
        } else if (req->type() == kv::DELETE) {
            s = db_->Delete(w_opts_, req->key());
            resp->set_success(s.ok());
        }
        return grpc::Status::OK;
    }

    // ── Batch (hot path from api_node) ────────────────────────────────────
    grpc::Status ExecuteBatch(grpc::ServerContext*,
                              const kv::BatchRequest*  req,
                              kv::BatchResponse*       resp) override {
        const int n = req->requests_size();
        if (n == 0) return grpc::Status::OK;

        // Pre-allocate all response slots once
        resp->mutable_responses()->Reserve(n);
        for (int i = 0; i < n; ++i) resp->add_responses();

        // ── Pass 1: MultiGet for all GETs ─────────────────────────────────
        // Collect into flat arrays for a single MultiGet call.
        // RocksDB's MultiGet batches bloom-filter lookups and block reads.
        thread_local std::vector<int>              get_idx;
        thread_local std::vector<rocksdb::Slice>   get_keys;
        thread_local std::vector<std::string>      get_vals;
        thread_local std::vector<rocksdb::Status>  get_st;

        get_idx.clear();  get_keys.clear();

        for (int i = 0; i < n; ++i) {
            if (req->requests(i).type() == kv::GET) {
                get_idx.push_back(i);
                get_keys.push_back(req->requests(i).key());
            }
        }
        if (!get_idx.empty()) {
            get_vals.resize(get_idx.size());
            get_st = db_->MultiGet(rocksdb::ReadOptions(), get_keys, &get_vals);
            for (size_t g = 0; g < get_idx.size(); ++g) {
                auto* r = resp->mutable_responses(get_idx[g]);
                if (get_st[g].ok()) r->set_value(std::move(get_vals[g]));
                r->set_success(get_st[g].ok() || get_st[g].IsNotFound());
            }
        }

        // Wrap the batch in a transaction for Atomicity & Isolation
        rocksdb::TransactionOptions txn_opts;
        txn_opts.set_snapshot    = true; // Repeatable Read
        txn_opts.deadlock_detect = true; // Safety in pessimistic locking
        
        std::unique_ptr<rocksdb::Transaction> txn(db_->BeginTransaction(w_opts_, txn_opts));
        if (!txn) return grpc::Status(grpc::StatusCode::INTERNAL, "Failed to begin transaction");

        for (int i = 0; i < n; ++i) {
            const auto& r = req->requests(i);
            if (r.type() == kv::PUT) {
                txn->Put(r.key(), r.value());
            } else if (r.type() == kv::DELETE) {
                txn->Delete(r.key());
            }
        }

        rocksdb::Status s = txn->Commit();
        bool ok = s.ok();
        for (int i = 0; i < n; ++i) {
            const auto& r = req->requests(i);
            if (r.type() == kv::PUT || r.type() == kv::DELETE)
                resp->mutable_responses(i)->set_success(ok);
        }

        if (!ok)
            return grpc::Status(grpc::StatusCode::INTERNAL, s.ToString());

        total_batches.fetch_add(1, std::memory_order_relaxed);
        total_keys.fetch_add(n,    std::memory_order_relaxed);
        return grpc::Status::OK;
    }
};

int main(int argc, char** argv) {
    int shard_id = (argc >= 2) ? std::atoi(argv[1]) : 0;

    std::string sock_path  = "/tmp/kv_" + std::to_string(shard_id) + ".sock";
    std::string data_dir   = "./data/shard_" + std::to_string(shard_id);

    // Create data directory
    std::string mkdir_cmd = "mkdir -p " + data_dir;
    std::system(mkdir_cmd.c_str());

    std::remove(sock_path.c_str());

    StorageServiceImpl service(data_dir);

    grpc::ServerBuilder builder;
    builder.SetMaxReceiveMessageSize(-1);
    builder.SetMaxSendMessageSize(-1);

    // The storage node does real CPU work per RPC (RocksDB ops).
    // Keep poller count proportional to concurrent batches expected:
    // 8 api_nodes × 2 shards-per-node-per-storage = 16 concurrent RPCs max.
    // Give headroom with 32 min pollers.
    builder.SetSyncServerOption(
        grpc::ServerBuilder::SyncServerOption::NUM_CQS, 2);
    builder.SetSyncServerOption(
        grpc::ServerBuilder::SyncServerOption::MIN_POLLERS, 16);
    builder.SetSyncServerOption(
        grpc::ServerBuilder::SyncServerOption::MAX_POLLERS, 64);

    builder.AddListeningPort("unix:///" + sock_path,
                             grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    auto server = builder.BuildAndStart();
    if (!server) {
        std::cerr << "Failed to start storage shard " << shard_id << "\n";
        return 1;
    }
    std::cout << ">>> STORAGE SHARD " << shard_id
              << "  sock=" << sock_path
              << "  data=" << data_dir << "\n";

    // TPS monitor
    std::thread([shard_id]() {
        long last_b = 0, last_k = 0;
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            long b = total_batches.load(), k = total_keys.load();
            long db = b - last_b, dk = k - last_k;
            if (dk > 0)
                std::cout << "[shard " << shard_id << "] "
                          << dk << " keys/s  "
                          << db << " batches/s  avg_batch="
                          << (db ? dk/db : 0) << std::endl;
            last_b = b; last_k = k;
        }
    }).detach();

    server->Wait();
    return 0;
}
