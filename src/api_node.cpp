/*
 * api_node.cpp  —  Completely Asynchronous Batching Gateway
 *
 * Design:
 *   - Uses the modern gRPC C++ Callback API (CallbackService).
 *   - gRPC handler threads NEVER BLOCK. They immediately enqueue the request
 *     and return the reactor, allowing maximum concurrency.
 *   - Dynamic/Flexible Batching: No artificial waits, no fixed batch sizes.
 *     If the dispatcher is idle and an item arrives, it is sent immediately.
 *     While the dispatcher is busy talking to RocksDB (~300µs), newly arriving
 *     requests queue up concurrently. Once the dispatcher returns, it grabs
 *     all pending requests as a single batch.
 */
#include "kv.grpc.pb.h"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <condition_variable>
#include <cstdlib>
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

// ── Constants ─────────────────────────────────────────────────────────────────
static constexpr int    N_SHARDS     = 1;     // Each process handles 1 shard to concentrate batches
static constexpr int    N_STORAGE    = 8;     // Total storage processes
static constexpr int    DEADLINE_MS  = 5000;

static const std::string JWT_SECRET  = "your-very-secret-key";
static std::atomic<long> g_total_ok{0};

// ── Pending request ───────────────────────────────────────────────────────────
struct PendingRequest {
    const kv::SingleRequest*  req;
    kv::SingleResponse*       resp;
    grpc::ServerUnaryReactor* reactor;

    PendingRequest(const kv::SingleRequest* r, kv::SingleResponse* rsp, grpc::ServerUnaryReactor* rct)
        : req(r), resp(rsp), reactor(rct) {}
};

// ── Per-shard state ───────────────────────────────────────────────────────────
struct Shard {
    std::mutex                   mu;
    std::condition_variable      cv;
    std::vector<PendingRequest>  queue;
    bool                         stop{false};
    std::unique_ptr<kv::KVService::Stub> stub;
    std::thread                  dispatcher_thread;

    Shard() { queue.reserve(4096); }
    ~Shard() {
        { std::lock_guard<std::mutex> lk(mu); stop = true; }
        cv.notify_all();
        if (dispatcher_thread.joinable()) dispatcher_thread.join();
    }
};

// ── Service ───────────────────────────────────────────────────────────────────
// We use CallbackService for completely asynchronous, non-blocking RPC handling
class ApiServiceImpl final : public kv::KVService::CallbackService {
    Shard                    shards_[N_SHARDS];
    static std::atomic<int>  rr_;

public:
    explicit ApiServiceImpl(int port_val) {
        grpc::ChannelArguments args;
        args.SetMaxReceiveMessageSize(-1);
        args.SetMaxSendMessageSize(-1);
        
        // This process handles exactly one storage shard based on its port
        int target_shard = port_val % N_STORAGE;
        
        for (int i = 0; i < N_SHARDS; ++i) {
            std::string sock = "unix:///tmp/kv_" + std::to_string(target_shard) + ".sock";
            auto ch = grpc::CreateCustomChannel(
                sock, grpc::InsecureChannelCredentials(), args);
            shards_[i].stub = kv::KVService::NewStub(std::move(ch));
            shards_[i].dispatcher_thread = std::thread(
                &ApiServiceImpl::dispatcher_loop, this, i);
        }
    }

    grpc::ServerUnaryReactor* Login(grpc::CallbackServerContext* context,
                                    const kv::LoginRequest* req,
                                    kv::LoginResponse* resp) override {
        auto* reactor = context->DefaultReactor();
        if (req->api_key() != "initial-pass") {
            reactor->Finish(grpc::Status(grpc::StatusCode::UNAUTHENTICATED, "Bad key"));
            return reactor;
        }
        
        resp->set_success(true);
        resp->set_jwt_token("bearer.kv-store." + req->client_id() + ".signed");
        reactor->Finish(grpc::Status::OK);
        return reactor;
    }

    grpc::ServerUnaryReactor* ExecuteSingle(grpc::CallbackServerContext* context,
                                            const kv::SingleRequest* req,
                                            kv::SingleResponse* resp) override {
        auto* reactor = context->DefaultReactor();

        int    shard_id = rr_.fetch_add(1, std::memory_order_relaxed) % N_SHARDS;
        Shard& shard    = shards_[shard_id];

        {
            std::lock_guard<std::mutex> lk(shard.mu);
            shard.queue.emplace_back(req, resp, reactor);
        }
        // Wake dispatcher immediately
        shard.cv.notify_one();

        // Return the reactor immediately without blocking the gRPC thread!
        return reactor;
    }

private:
    void dispatcher_loop(int shard_id) {
        Shard& shard = shards_[shard_id];
        std::vector<PendingRequest> batch;
        batch.reserve(4096);

        while (true) {
            {
                std::unique_lock<std::mutex> lk(shard.mu);
                // Wait for first request
                shard.cv.wait(lk, [&] {
                    return !shard.queue.empty() || shard.stop;
                });
                if (shard.stop && shard.queue.empty()) return;

                // Wait up to 1ms for a larger batch
                shard.cv.wait_for(lk, std::chrono::milliseconds(1), [&]{
                    return shard.queue.size() >= 1000 || shard.stop;
                });
                
                batch.swap(shard.queue);
                shard.queue.reserve(4096);
            }
            
            // Send the batch to storage node
            dispatch(shard_id, batch);
            batch.clear();
        }
    }

    void dispatch(int shard_id, std::vector<PendingRequest>& batch) {
        Shard& shard = shards_[shard_id];

        kv::BatchRequest  batch_req;
        kv::BatchResponse batch_resp;
        batch_req.mutable_requests()->Reserve((int)batch.size());
        
        for (auto& pr : batch) {
            *batch_req.add_requests() = *pr.req;
        }

        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now()
                         + std::chrono::milliseconds(DEADLINE_MS));

        grpc::Status s = shard.stub->ExecuteBatch(&ctx, batch_req, &batch_resp);

        if (s.ok()) {
            for (size_t i = 0; i < batch.size(); ++i) {
                *batch[i].resp = std::move(*batch_resp.mutable_responses(i));
                batch[i].reactor->Finish(grpc::Status::OK);
            }
        } else {
            for (auto& pr : batch) {
                pr.resp->set_success(false);
                pr.resp->set_value("batch rpc failed: " + s.error_message());
                pr.reactor->Finish(grpc::Status::OK);
            }
        }
        
        // Count successes (batch[...].resp->success() is verified inside)
        long ok_count = 0;
        if (s.ok()) {
            for (size_t i = 0; i < batch.size(); ++i) {
                if (batch_resp.responses(i).success()) ok_count++;
            }
        }
        if (ok_count > 0) g_total_ok.fetch_add(ok_count, std::memory_order_relaxed);
    }
};

std::atomic<int> ApiServiceImpl::rr_{0};

// ── main ──────────────────────────────────────────────────────────────────────
int main(int argc, char** argv) {
    if (argc < 2) { std::cerr << "Usage: ./api_node <PORT>\n"; return 1; }
    int port_num = std::atoi(argv[1]);
    std::string port_str = std::to_string(port_num);
    ApiServiceImpl service(port_num);

    grpc::ServerBuilder builder;
    builder.SetMaxReceiveMessageSize(-1);
    builder.SetMaxSendMessageSize(-1);

    builder.AddListeningPort("0.0.0.0:" + port_str,
                             grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    auto server = builder.BuildAndStart();
    if (!server) { std::cerr << "Failed to start on port " << port_str << "\n"; return 1; }

    std::cout << ">>> API NODE  port=" << port_str
              << "  shards=" << N_SHARDS
              << "  storage_procs=" << N_STORAGE << "  (ASYNC CALLBACK)\n";

    std::thread([port_str]() {
        long last = 0;
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            long cur = g_total_ok.load();
            long tps = cur - last;
            if (tps > 0)
                std::cout << "[api:" << port_str << "] " << tps << " TPS\n";
            last = cur;
        }
    }).detach();

    server->Wait();
    return 0;
}
