/*
Grid-based Task Dispatcher System

This software is a C++ 14 Header-Only reimplementation of Kernel.h from Project PaintsNow.

The MIT License (MIT)

Copyright (c) 2014-2020 PaintDream

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

*/

#pragma once

#include <cstdint>
#include <functional>
#include <thread>
#include <vector>
#include <atomic>
#include <mutex>
#include <cassert>

namespace grid {
	// simple kfifo like queue. allowing one-thread read (pop) and one-thread write (push).
	template <class T, size_t K = 6>
	class queue_t {
	public:
		queue_t() : push_index(0), pop_index(0) {}

		// N = capacity + 1
		static constexpr size_t N = 1 << K;
		static constexpr size_t MASK = N - 1;

		template <class D>
		bool push(D&& t) {
			size_t next_index = (push_index + 1) & MASK;
			if (next_index == pop_index) {
				return false; // this queue is full, push failed
			}

			ring_buffer[push_index] = std::forward<D>(t);

			// place thread_fence here to ensure that change of ring_buffer[push_index]
			//   must be visible to other threads after push_index updates. 
			std::atomic_thread_fence(std::memory_order_release);
			push_index = next_index;
			return true;
		}

		T& top() {
			std::atomic_thread_fence(std::memory_order_acquire);
			assert(!empty());
			return ring_buffer[pop_index];
		}

		const T& top() const {
			std::atomic_thread_fence(std::memory_order_acquire);
			assert(!empty());
			return ring_buffer[pop_index];
		}

		void pop() {
			pop_index = (pop_index + 1) & MASK;
		}

		bool empty() const {
			return pop_index == push_index;
		}

		size_t count() const {
			return (pop_index + MASK - push_index) % MASK;
		}

	protected:
		size_t push_index; // write index
		size_t pop_index; // read index
		T ring_buffer[N];
	};

	// chain kfifos to make variant capacity.
	template <class T, size_t K = 6>
	class queue_list_t {
	public:
		typedef queue_t<T, K> sub_queue;
		class node : public sub_queue {
		public:
			node() : next(nullptr) {}
			node* next; // chain next queue
		};

	public:
		// do not copy this class, only to move
		queue_list_t(const queue_list_t& rhs) = delete;
		queue_list_t& operator = (const queue_list_t& rhs) = delete;

		queue_list_t() {
			push_head = pop_head = new node();
		}

		queue_list_t(queue_list_t&& rhs) noexcept {
			push_head = rhs.push_head;
			pop_head = rhs.pop_head;

			rhs.push_head = nullptr;
			rhs.pop_head = nullptr;
		}

		queue_list_t& operator = (queue_list_t&& rhs) {
			// just swap pointers.
			std::swap(push_head, rhs.push_head);
			std::swap(pop_head, rhs.pop_head);

			return *this;
		}

		~queue_list_t() {
			if (pop_head != nullptr) {
				node* q = pop_head;
				while (q != nullptr) {
					node* p = q;
					q = q->next;
					delete p;
				}
			}
		}

		template <class D>
		void push(D&& t) {
			if (!push_head->push(std::forward<D>(t))) { // sub queue full?
				node* p = new node(); // allocate new node.
				p->push(std::forward<D>(t)); // must success.

				// chain new node at head.
				push_head->next = p;
				std::atomic_thread_fence(std::memory_order_release);
				push_head = p;
			}
		}

		T& top() {
			return pop_head->top();
		}

		const T& top() const {
			return pop_head->top();
		}

		void pop() {
			pop_head->pop();

			// current queue is empty, remove it from list.
			if (pop_head->empty() && pop_head != push_head) {
				node* p = pop_head;
				pop_head = pop_head->next;
				std::atomic_thread_fence(std::memory_order_release);
				delete p;
			}
		}

		bool empty() const {
			return pop_head->empty();
		}

		size_t count() const {
			size_t counter = 0;
			// sum up all sub queues
			for (node* p = pop_head; p != nullptr; p = p->next) {
				counter += p->count();
			}

			return counter;
		}

	protected:
		node* push_head = nullptr;
		node* pop_head = nullptr; // pop_head is always prior to push_head.
	};

	template <typename queue_buffer_t, bool>
	struct storage_t {
		storage_t() {}
		storage_t(storage_t&& rhs) noexcept {
			queue_buffer = std::move(rhs.queue_buffer);
		}

		storage_t& operator = (storage_t&& rhs) noexcept {
			queue_buffer = std::move(rhs.queue_buffer);
			return *this;
		}

		queue_buffer_t queue_buffer;
		std::mutex mutex;
	};

	template <typename queue_buffer_t>
	struct storage_t<queue_buffer_t, false> {
		std::vector<queue_buffer_t> queue_buffers;
	};

	// dispatch routines:
	//     1. from warp to warp. (queue_routine/queue_routine_post).
	//     2. from external thread to warp (queue_routine_external).
	//     3. from warp to external in parallel (queue_routine_parallel).
	// you can select implemention from warp/strand via 'strand' template parameter.
	template <typename async_worker_t, bool strand = false, size_t K = 6>
	class warp_t {
	public:
		using queue_buffer = queue_list_t<std::function<void()>, K>;

		// do not copy this class, only to move
		warp_t(const warp_t& rhs) = delete;
		warp_t& operator = (const warp_t& rhs) = delete;

		template <bool s>
		typename std::enable_if<s>::type init_buffers(size_t thread_count) {}
		template <bool s>
		typename std::enable_if<!s>::type init_buffers(size_t thread_count) {
			storage.queue_buffers.resize(thread_count);
		}

		warp_t(async_worker_t& worker, size_t thread_count) : async_worker(worker) {
			init_buffers<strand>(thread_count);

			thread_warp.store(nullptr, std::memory_order_relaxed);
			suspend_count.store(0, std::memory_order_relaxed);
			queueing.store(0, std::memory_order_release);
		}

		warp_t(warp_t&& rhs) noexcept : async_worker(rhs.async_worker) {
			storage = std::move(rhs.storage);

			thread_warp.store(rhs.thread_warp.load(std::memory_order_relaxed), std::memory_order_relaxed);
			suspend_count.store(rhs.suspend_count.load(std::memory_order_relaxed), std::memory_order_relaxed);
			queueing.store(rhs.queueing.load(std::memory_order_relaxed), std::memory_order_relaxed);
			rhs.thread_warp.store(nullptr, std::memory_order_relaxed);
			rhs.suspend_count.store(0, std::memory_order_relaxed);
			rhs.queueing.store(0, std::memory_order_release);
		}

		// take execution atomically, returns true on success.
		bool preempt() {
			warp_t** expected = nullptr;
			if (thread_warp.compare_exchange_strong(expected, &get_current_warp_internal(), std::memory_order_acq_rel)) {
				get_current_warp_internal() = this;
				return true;
			} else {
				return get_current_warp_internal() == this;
			}
		}

		// yield execution atomically, returns true on success.
		bool yield() {
			warp_t** exp = &get_current_warp_internal();
			if (thread_warp.compare_exchange_strong(exp, nullptr, std::memory_order_release)) {
				get_current_warp_internal() = nullptr;
				if (queueing.exchange(0, std::memory_order_relaxed) == 1) {
					flush();
				}

				return true;
			} else {
				return false;
			}
		}

		// blocks all tasks preemptions, stacked with internally counting.
		void suspend() {
			suspend_count.fetch_add(1, std::memory_order_acquire);
		}

		// allows all tasks preemptions, stacked with internally counting.
		// returns true on final resume.
		bool resume() {
			bool ret = suspend_count.fetch_sub(1, std::memory_order_release) == 1;

			if (ret) {
				// all suspend requests removed, try to flush me
				queueing.store(0, std::memory_order_relaxed);
				flush();
			}

			return ret;
		}

		// send task to this warp. call it directly if we are on warp.
		template <typename F>
		void queue_routine(F&& func) {
			size_t thread_index = async_worker.get_current_thread_index();
			assert(thread_index != ~(size_t)0);
			assert(get_current_warp_internal() != nullptr);

			// can be executed immediately?
			if (get_current_warp_internal() == this
				&& thread_warp.load(std::memory_order_relaxed) == &get_current_warp_internal()
				&& suspend_count.load(std::memory_order_acquire) == 0) {
				func();
			} else {
				// send to current thread slot of current warp.
				push<strand>(std::forward<F>(func));
			}
		}

		// send task to warp indicated by warp. always post it to queue.
		template <typename F>
		void queue_routine_post(F&& func) {
			// always send to current thread slot of current warp.
			push<strand>(std::forward<F>(func));
		}

		// queue external routine from non-warp/yielded warp
		template <typename F>
		void queue_routine_external(F&& func) {
			assert(async_worker.get_current_thread_index() == ~(size_t)0);
			async_worker.queue([this, func = std::forward<F>(func)]() mutable {
				queue_routine_post(std::forward<F>(func));
			});
		}

		// queue task parallelly to async_worker, blocking the execution of current warp at the same time
		// it is useful to implement read-lock affairs
		template <typename F>
		void queue_routine_parallel(F&& func) {
			assert(get_current_warp_internal() == this);
			suspend();
			async_worker.queue([this, func = std::forward<F>(func)]() mutable {
				get_current_warp_internal() = this; // force reset warp index
				func();
				resume();
			});
		}

		// clear the dispatcher, pass true to 'execute_remaining' to make sure all tasks are executed finally.
		template <bool execute_remaining = true, typename T = warp_t*>
		static void join(T begin, T end) {
			// suspend all warps so we can take over tasks
			for (T p = begin; p != end; ++p) {
				(*p).suspend();
			}

			// do cleanup
			for (T p = begin; p != end; ++p) {
				while (!(*p).preempt()) {
					std::this_thread::yield();
				}

				// execute remaining
				if (execute_remaining) {
					(*p).template execute<strand>();
				}

				(*p).yield();
			}

			// resume warps
			for (T p = begin; p != end; ++p) {
				(*p).resume();
			}
		}

		static warp_t& get_current_warp() {
			warp_t* ptr = get_current_warp_internal();
			assert(ptr != nullptr);
			return *ptr;
		}

	protected:
		// get current warp index (saved in thread_local storage)
		static warp_t*& get_current_warp_internal() {
			static thread_local warp_t* current_warp = nullptr;
			return current_warp;
		}

		// execute all tasks scheduled at once.
		template <bool s>
		typename std::enable_if<s>::type execute() {
			if (suspend_count.load(std::memory_order_acquire) == 0) {
				if (preempt()) {
					// mark for queueing, avoiding flush me more than once.
					queueing.store(2, std::memory_order_relaxed);
					queue_buffer& buffer = storage.queue_buffer;
					while (!buffer.empty()) {
						buffer.top()();
						buffer.pop();

						if (suspend_count.load(std::memory_order_acquire) != 0
							|| thread_warp.load(std::memory_order_relaxed) != &get_current_warp_internal()
							|| get_current_warp_internal() != this) {
							break;
						}
					}

					if (!yield()) {
						// already yielded? try to repost me to process remaining tasks.
						flush();
					}
				}
			}
		}

		template <bool s>
		typename std::enable_if<!s>::type execute() {
			if (suspend_count.load(std::memory_order_acquire) == 0) {
				// try to acquire execution, if it fails, there must be another thread doing the same thing
				// and it's ok to return immediatly.
				if (preempt()) {
					// mark for queueing, avoiding flush me more than once.
					queueing.store(2, std::memory_order_relaxed);
					std::vector<queue_buffer>& queue_buffers = storage.queue_buffers;

					for (size_t i = 0; i < queue_buffers.size(); i++) {
						queue_buffer& buffer = queue_buffers[i];
						while (!buffer.empty()) {
							buffer.top()();
							buffer.pop();

							if (suspend_count.load(std::memory_order_acquire) != 0
								|| thread_warp.load(std::memory_order_relaxed) != &get_current_warp_internal()
								|| get_current_warp_internal() != this) {
								i = queue_buffers.size();
								break;
							}
						}
					}

					if (!yield()) {
						// already yielded? try to repost me to process remaining tasks.
						flush();
					}
					
					// otherwise all tasks are executed, safe to exit.
				}
			}
		}

		// commit execute request to specified thread pool.
		void flush() {
			if (queueing.exchange(1, std::memory_order_relaxed) == 0) {
				async_worker.queue(std::bind(&warp_t::template execute<strand>, this));
			}
		}

		// queue task from specified thread.
		template <bool s, typename F>
		typename std::enable_if<s>::type push(F&& func) {
			do {
				std::lock_guard<std::mutex> guard(storage.mutex);
				storage.queue_buffer.push(std::forward<F>(func));
			} while (false);

			flush();
		}

		template <bool s, typename F>
		typename std::enable_if<!s>::type push(F&& func) {
			size_t thread_index = async_worker.get_current_thread_index();
			std::vector<queue_buffer>& queue_buffers = storage.queue_buffers;
			assert(thread_index < queue_buffers.size());
			queue_buffer& buffer = queue_buffers[thread_index];
			buffer.push(std::forward<F>(func));

			flush();
		}

	protected:
		async_worker_t& async_worker;
		std::atomic<warp_t**> thread_warp; // save the running thread warp address.
		std::atomic<size_t> suspend_count;
		std::atomic<size_t> queueing; // is flush request sent to async_worker? 0 : not yet, 1 : yes, 2 : is to flush right away.
		storage_t<queue_buffer, strand> storage;
	};
}
