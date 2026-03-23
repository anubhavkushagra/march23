/*
 * load_client.cpp  —  Synchronous benchmark client
 *
 * Usage:  ./load_client <THREADS> <SECONDS> [SERVER_IP] [PAYLOAD_BYTES]
 *
 * Design:
 *   - Strictly unary synchronous RPCs (one blocking call per thread at a time)
 *   - One gRPC channel per api_node backend (8 independent channels)
 *   - JWT attached via ClientContext per call (no composite credential needed)
 *   - Keys pre-built before hot loop → zero allocations in benchmark path
 */
#include "kv.grpc.pb.h"

#include <atomic>
#include <chrono>
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cout << "Usage: ./load_client <THREADS> <SECONDS> [SERVER_IP]\n"
                  << "  e.g. ./load_client 400 60 192.168.1.10\n";
        return 0;
    }

    int         threads        = std::stoi(argv[1]);
    int         target_seconds = std::stoi(argv[2]);
    std::string server_ip      = (argc >= 4) ? argv[3] : "127.0.0.1";
    int         payload_bytes  = (argc >= 5) ? std::stoi(argv[4]) : 64;

    // ── Login ─────────────────────────────────────────────────────────────────
    auto plain_ch   = grpc::CreateChannel(
                          "ipv4:" + server_ip + ":50063",
                          grpc::InsecureChannelCredentials());
    auto login_stub = kv::KVService::NewStub(plain_ch);

    std::string jwt;
    {
        grpc::ClientContext ctx;
        kv::LoginRequest    req;
        kv::LoginResponse   resp;
        req.set_api_key("initial-pass");
        req.set_client_id("bench");
        auto st = login_stub->Login(&ctx, req, &resp);
        if (!st.ok() || resp.jwt_token().empty()) {
            std::cerr << "Login failed: " << st.error_message() << "\n";
            return 1;
        }
        jwt = resp.jwt_token();
        std::cout << "Login OK. JWT length=" << jwt.size() << "\n";
    }

    // ── Build stub pool ───────────────────────────────────────────────────────
    const int FIRST_PORT = 50063, LAST_PORT = 50070;
    const int CHANNELS_PER_PORT = 8; // bypass HTTP/2 stream limits per process
    
    std::vector<std::unique_ptr<kv::KVService::Stub>> stubs;
    for (int p = FIRST_PORT; p <= LAST_PORT; ++p) {
        for (int c = 0; c < CHANNELS_PER_PORT; ++c) {
            grpc::ChannelArguments args;
            args.SetMaxReceiveMessageSize(-1);
            args.SetMaxSendMessageSize(-1);
            args.SetInt(GRPC_ARG_USE_LOCAL_SUBCHANNEL_POOL, 1);
            auto ch = grpc::CreateCustomChannel("ipv4:" + server_ip + ":" + std::to_string(p),
                                                grpc::InsecureChannelCredentials(), args);
            stubs.push_back(kv::KVService::NewStub(ch));
        }
    }

    // ── Pre-generate keys ─────────────────────────────────────────────────────
    const long KEY_WINDOW = 100000L;
    std::cout << "Pre-generating " << (long)threads * KEY_WINDOW
              << " keys (" << threads << " threads × " << KEY_WINDOW << ")...\n";

    std::vector<std::string> all_keys((size_t)threads * KEY_WINDOW);
    for (int t = 0; t < threads; ++t) {
        long base = (long)t * KEY_WINDOW;
        for (long k = 0; k < KEY_WINDOW; ++k) {
            char buf[32];
            int len = std::snprintf(buf, sizeof(buf), "k%d_%ld", t, k);
            all_keys[base + k].assign(buf, len);
        }
    }

    std::string value(payload_bytes, 'x');

    // ── Benchmark ─────────────────────────────────────────────────────────────
    std::atomic<long> total_ok{0};
    std::atomic<long> total_fail{0};

    auto start = std::chrono::steady_clock::now();
    std::cout << "Benchmark: " << threads << " threads × "
              << target_seconds << "s  payload=" << payload_bytes << "B\n";

    std::vector<std::thread> pool;
    pool.reserve(threads);

    for (int t = 0; t < threads; ++t) {
        pool.emplace_back([&, t]() {
            long ok = 0, fail = 0, j = 0;
            auto* stub  = stubs[t % stubs.size()].get();
            long  base  = (long)t * KEY_WINDOW;

            while (true) {
                // Check stop deadline every 16 requests
                if ((j & 15) == 0) {
                    auto now = std::chrono::steady_clock::now();
                    if (std::chrono::duration_cast<std::chrono::seconds>(
                            now - start).count() >= target_seconds)
                        break;
                }

                grpc::ClientContext ctx;
                ctx.AddMetadata("authorization", jwt);

                kv::SingleRequest  req;
                kv::SingleResponse resp;
                req.set_type(kv::PUT);
                req.set_key(all_keys[base + (j % KEY_WINDOW)]);
                req.set_value(value);

                auto st = stub->ExecuteSingle(&ctx, req, &resp);
                if (st.ok() && resp.success()) ++ok;
                else                            ++fail;
                ++j;
            }

            total_ok.fetch_add(ok,   std::memory_order_relaxed);
            total_fail.fetch_add(fail, std::memory_order_relaxed);
        });
    }

    for (auto& th : pool) th.join();

    auto end = std::chrono::steady_clock::now();
    double elapsed = std::chrono::duration<double>(end - start).count();
    double rps     = (double)total_ok.load() / elapsed;

    std::cout << "\n════════════ Benchmark Results ════════════\n"
              << "  Threads:      " << threads       << "\n"
              << "  Duration:     " << elapsed       << " s\n"
              << "  Successful:   " << total_ok.load()  << "\n"
              << "  Failed:       " << total_fail.load() << "\n"
              << "  Throughput:   " << (long)rps     << " req/s\n"
              << "  Payload:      " << payload_bytes << " B\n"
              << "═══════════════════════════════════════════\n";
    return 0;
}
