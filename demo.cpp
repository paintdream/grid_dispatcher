#include "grid_dispatcher.h"
#include <iostream>
using namespace grid;

static void simple_explosion();
static void garbage_collection();

int main(void) {
	simple_explosion();
	garbage_collection();

	return 0;
}

typedef warp_t<demo_async_worker_t> demo_warp_t;

void simple_explosion(void) {
	static const size_t thread_count = 4;
	static const size_t warp_count = 8;

	demo_async_worker_t worker(thread_count);
	std::vector<demo_warp_t> warps;
	warps.reserve(warp_count);
	for (size_t i = 0; i < warp_count; i++) {
		warps.emplace_back(worker);
	}

	srand((unsigned int)time(nullptr));
	std::cout << "[[ demo for grid dispatcher : simple_explosion ]] " << std::endl;

	static int32_t warp_data[warp_count] = { 0 };
	static size_t split_count = 4;
	static size_t terminate_factor = 100;
	static size_t parallel_factor = 11;
	static size_t parallel_count = 6;

	std::function<void()> explosion;

	// queue tasks randomly to test if dispatcher could handle them correctly.
	explosion = [&warps, &explosion, &worker]() {
		demo_warp_t& current_warp = demo_warp_t::get_current_warp();
		size_t warp_index = &current_warp - &warps[0];
		warp_data[warp_index]++;

		// simulate working
		std::this_thread::sleep_for(std::chrono::milliseconds(rand() % 40));
		warp_data[warp_index]++;

		if (rand() % terminate_factor == 0) {
			// randomly terminates
			worker.terminate();
		}

		warp_data[warp_index]++;
		// randomly dispatch to warp
		for (size_t i = 0; i < split_count; i++) {
			warps[rand() % warp_count].queue_routine(std::function<void()>(explosion));
		}

		warp_data[warp_index] -= 3;

		if (rand() % parallel_factor == 0) {
			// read-write lock example: multiple reading blocks writing
			std::shared_ptr<std::atomic<int32_t>> shared_value = std::make_shared<std::atomic<int32_t>>(-0x7fffffff);
			for (size_t i = 0; i < parallel_count; i++) {
				current_warp.queue_routine_parallel([shared_value, warp_index]() {
					// only read operations
					std::this_thread::sleep_for(std::chrono::milliseconds(rand() % 40));
					int32_t v = shared_value->exchange(warp_data[warp_index]);
					assert(v == warp_data[warp_index] || v == -0x7fffffff);
				});
			}
		}
	};

	// invoke explosion from external thread (current thread is external to the threads in thread pool)
	warps[0].queue_routine_external(std::function<void()>(explosion));
	worker.join();

	// finished!
	demo_warp_t::join(warps.begin(), warps.end());

	std::cout << "after: " << std::endl;
	for (size_t k = 0; k < warp_count; k++) {
		std::cout << "warp " << k << " : " << warp_data[k] << std::endl;
	}
}

void garbage_collection() {
	static const size_t thread_count = 8;
	static const size_t warp_count = 16;
	demo_async_worker_t worker(thread_count);
	std::vector<demo_warp_t> warps;
	warps.reserve(warp_count);
	for (size_t i = 0; i < warp_count; i++) {
		warps.emplace_back(worker);
	}

	srand((unsigned int)time(nullptr));
	std::cout << "[[ demo for grid dispatcher : garbage_collection ]] " << std::endl;
	struct node_t {
		size_t warp_index = 0;
		size_t visit_count = 0; // we do not use std::atomic<> here.
		std::vector<size_t> references;
	};

	struct graph_t {
		std::vector<node_t> nodes;
	};

	// randomly initialize connections.
	const size_t node_count = 4096;
	const size_t max_node_connection = 5;
	const size_t extra_node_connection_root = 20;

	graph_t graph;
	graph.nodes.reserve(node_count);

	for (size_t i = 0; i < node_count; i++) {
		node_t node;
		node.warp_index = rand() % warp_count;

		size_t connection = rand() % max_node_connection;
		node.references.reserve(connection);

		for (size_t k = 0; k < connection; k++) {
			node.references.emplace_back(rand() % node_count); // may connected to it self
		}

		graph.nodes.emplace_back(std::move(node));
	}

	// select random root
	size_t root_index = rand() % node_count;

	// ok now let's start collect from root!
	std::function<void(size_t)> collector;
	std::atomic<size_t> collecting_count;
	collecting_count.store(0, std::memory_order_release);

	collector = [&warps, &collector, &worker, &graph, &collecting_count](size_t node_index) {
		demo_warp_t& current_warp = demo_warp_t::get_current_warp();
		size_t warp_index = &current_warp - &warps[0];

		node_t& node = graph.nodes[node_index];
		assert(node.warp_index == warp_index);

		if (node.visit_count == 0) {
			node.visit_count++;

			for (size_t i = 0; i < node.references.size(); i++) {
				size_t next_node_index = node.references[i];
				size_t next_node_warp = graph.nodes[next_node_index].warp_index;
				collecting_count.fetch_add(1, std::memory_order_acquire);
				warps[next_node_warp].queue_routine(std::bind(collector, next_node_index));
			}
		}

		if (collecting_count.fetch_sub(1, std::memory_order_release) == 1) {
			// all work finished.
			size_t collected_count = 0;
			for (size_t k = 0; k < graph.nodes.size(); k++) {
				node_t& node = graph.nodes[k];
				assert(node.visit_count < 2);
				collected_count += node.visit_count;
				node.visit_count = 0;
			}

			std::cout << "garbage_collection finished. " << collected_count << " of " << graph.nodes.size() << " collected." << std::endl;
			worker.terminate();
		}
	};

	// invoke explosion from external thread (current thread is external to the threads in thread pool)
	collecting_count.fetch_add(1, std::memory_order_acquire);
	// add more references to root
	for (size_t j = 0; j < extra_node_connection_root; j++) {
		graph.nodes[root_index].references.emplace_back(rand() % node_count);
	}

	warps[graph.nodes[root_index].warp_index].queue_routine_external(std::bind(collector, root_index));
	worker.join();

	// finished!
	demo_warp_t::join(warps.begin(), warps.end());
}
