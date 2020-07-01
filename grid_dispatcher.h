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
#include <mutex>
#include <atomic>
#include <cassert>
// #include <iostream>

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

	protected:
		node* push_head = nullptr;
		node* pop_head = nullptr; // pop_head is always prior to push_head.

	public:
		queue_list_t() {
			push_head = pop_head = new node();
		}

		queue_list_t(queue_list_t&& rhs) {
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
	};

	// grid dispatcher, dispatch routines:
	//     1. from warp to warp. (dispatcher_t::queue_routine/queue_routine_post).
	//     2. from external thread to warp (dispatcher_t::queue_routine_external).
	//     3. from warp to external in parallel (dispathcer_t::queue_routine_parallel).
	template <typename async_worker_t, size_t K = 10>
	class dispatcher_t {
	protected:
		// task queue collection for a specified destination warp from all source threads.
		class task_queue_t {
		public:
			using queue_buffer = queue_list_t<std::function<void(size_t)>, K>;

			task_queue_t(dispatcher_t& host_, size_t thread_count) : host(host_) {
				queue_buffers.resize(thread_count);
				thread_warp.store(nullptr, std::memory_order_relaxed);
				suspend_count.store(0, std::memory_order_relaxed);
				queueing.store(0, std::memory_order_release);
			}

			task_queue_t(task_queue_t&& rhs) : host(rhs.host) {
				queue_buffers = std::move(rhs.queue_buffers);
				thread_warp.store(rhs.thread_warp.load(std::memory_order_relaxed), std::memory_order_relaxed);
				suspend_count.store(rhs.suspend_count.load(std::memory_order_relaxed), std::memory_order_relaxed);
				queueing.store(rhs.queueing.load(std::memory_order_relaxed), std::memory_order_relaxed);
				rhs.thread_warp.store(nullptr, std::memory_order_relaxed);
				rhs.suspend_count.store(0, std::memory_order_relaxed);
				rhs.queueing.store(0, std::memory_order_release);
			}

			// do not copy this class, only to move
			task_queue_t(const task_queue_t& rhs) = delete;
			task_queue_t& operator = (const task_queue_t& rhs) = delete;

			// take execution atomically, returns true on success.
			bool preempt_execution() {
				size_t* expected = nullptr;
				size_t this_warp_index = this - &host.task_queue_grids[0];
				if (thread_warp.compare_exchange_strong(expected, &get_current_warp_index(), std::memory_order_acq_rel)) {
					get_current_warp_index() = this_warp_index;
					return true;
				} else {
					return expected == &get_current_warp_index();
				}
			}

			// yield execution atomically, returns true on success.
			bool yield_execution() {
				size_t* exp = &get_current_warp_index();
				if (thread_warp.compare_exchange_strong(exp, nullptr, std::memory_order_release)) {
					get_current_warp_index() = ~(size_t)0;
					if (queueing.exchange(0, std::memory_order_relaxed) == 1) {
						flush();
					}

					return true;
				} else {
					return false;
				}
			}

			// execute all tasks scheduled at once.
			void execute() {
				if (suspend_count.load(std::memory_order_acquire) == 0) {
					// try to acquire execution, if it fails, there must be another thread doing the same thing
					// and it's ok to return immediatly.
					if (preempt_execution()) {
						// mark for queueing, avoiding flush me more than once.
						queueing.store(2, std::memory_order_relaxed);
						size_t warp_index = this - &host.task_queue_grids[0];

						/*
						static std::mutex mutex;
						{
							std::lock_guard<std::mutex> g(mutex);
							std::cout << "thread " << host.async_worker.get_current_thread_index() << " takes " << warp_index << std::endl;
						}*/

						for (size_t i = 0; i < queue_buffers.size(); i++) {
							queue_buffer& buffer = queue_buffers[i];
							while (!buffer.empty()) {
								buffer.top()(warp_index);
								buffer.pop();

								if (suspend_count.load(std::memory_order_acquire) != 0
									|| thread_warp.load(std::memory_order_relaxed) != &get_current_warp_index()
									|| get_current_warp_index() != warp_index) {
									i = queue_buffers.size();
									break;
								}
							}
						}

						/*
						{
							std::lock_guard<std::mutex> g(mutex);
							std::cout << "thread " << host.async_worker.get_current_thread_index() << " leaves " << warp_index << std::endl;
						}*/

						if (!yield_execution()) {
							// already yielded? try to repost me to process remaining tasks.
							flush();
						} // otherwise all tasks are executed, safe to exit.
					}
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

			// commit execute request to specified thread pool.
			void flush() {
				if (queueing.exchange(1, std::memory_order_relaxed) == 0) {
					host.async_worker.queue(std::bind(&task_queue_t::execute, this));
				}
			}

			// queue task from specified thread.
			template <typename F>
			void push(size_t thread_index, F&& func) {
				assert(thread_index < queue_buffers.size());
				queue_buffer& buffer = queue_buffers[thread_index];
				buffer.push(std::forward<F>(func));

				flush();
			}

		protected:
			friend class dispatcher_t;

			dispatcher_t& host;
			std::atomic<size_t*> thread_warp; // save the running thread warp address.
			std::atomic<size_t> suspend_count;
			std::atomic<size_t> queueing; // is flush request sent to async_worker? 0 : not yet, 1 : yes, 2 : is to flush right away.
			std::vector<queue_buffer> queue_buffers; // task queues, each thread send tasks to its own slot of queue_buffers.
		};

	public:
		dispatcher_t(async_worker_t& async_worker_, size_t warp_count) : async_worker(async_worker_) {
			task_queue_grids.reserve(warp_count);
			size_t thread_count = async_worker.get_thread_count();

			// make grids with warp_count lines by thread_count columns
			for (size_t i = 0; i < warp_count; i++) {
				task_queue_grids.emplace_back(*this, thread_count);
			}
		}

		~dispatcher_t() { clear(); }

		// get current warp index (saved in thread_local storage)
		static size_t& get_current_warp_index() {
			static thread_local size_t current_warp_index = ~(size_t)0;
			return current_warp_index;
		}

		// yield current running warp, it indicates that you are detached from the current warp,
		//     after yield, the detached warp could be taken by another thread to run the remaining tasks.
		// you can only call this function within a attached(preempted) warp.
		void yield_current_warp() {
			assert(get_current_warp_index() != ~(size_t)0);
			task_queue_grids[get_current_warp_index()].yield_execution();
		}

		// suspend current running warp, you can call it on any thread
		void suspend_warp(size_t warp_index) {
			task_queue_grids[warp_index].suspend();
		}

		// resume current running warp, you can call it on any thread
		bool resume_warp(size_t warp_index) {
			return task_queue_grids[warp_index].resume();
		}

		size_t get_warp_count() const { return task_queue_grids.size(); }

		// clear the dispatcher, pass true to 'execute_remaining' to make sure all tasks are executed finally.
		void clear(bool execute_remaining = false) {
			// suspend all warps so we can take over tasks
			for (size_t i = 0; i < get_warp_count(); i++) {
				suspend_warp(i);
			}

			size_t thread_index = async_worker.get_current_thread_index();
			thread_index = thread_index == ~(size_t)0 ? 0 : thread_index;

			// do cleanup
			for (size_t j = 0; j < get_warp_count(); j++) {
				task_queue_t& q = task_queue_grids[j];

				while (!q.preempt_execution()) {
					std::this_thread::yield();
				}
		
				// execute remaining
				if (execute_remaining) {
					q.execute();
				}

				q.yield_execution();
			}

			// resume warps
			for (size_t k = 0; k < get_warp_count(); k++) {
				resume_warp(k);
			}
		}

		// send task to warp indicated by warp_index. call it directly if we are on warp_index.
		template <typename F>
		void queue_routine(size_t warp_index, F&& func) {
			size_t thread_index = async_worker.get_current_thread_index();
			assert(thread_index != ~(size_t)0);
			assert(get_current_warp_index() != ~(size_t)0);
			task_queue_t& q = task_queue_grids[warp_index];

			// can be executed immediately?
			if (get_current_warp_index() == warp_index
				&& q.thread_warp.load(std::memory_order_relaxed) == &get_current_warp_index()
				&& q.suspend_count.load(std::memory_order_acquire) == 0) {
				func(warp_index);
			} else {
				// send to current thread slot of current warp.
				q.push(thread_index, std::forward<F>(func));
			}
		}

		// send task to warp indicated by warp_index. always post it to queue.
		template <typename F>
		void queue_routine_post(size_t warp_index, F&& func) {
			size_t thread_index = async_worker.get_current_thread_index();
			assert(thread_index != ~(size_t)0);
			task_queue_t& q = task_queue_grids[warp_index];

			// always send to current thread slot of current warp.
			q.push(thread_index, std::forward<F>(func));
		}

		// queue external routine from non-warp/yielded warp
		template <typename F>
		void queue_routine_external(size_t warp_index, F&& func) {
			size_t thread_index = async_worker.get_current_thread_index();
			assert(thread_index == ~(size_t)0);
			async_worker.queue([this, warp_index, func = std::forward<F>(func)]() mutable {
				queue_routine_post(warp_index, std::forward<F>(func));
			});
		}

		// queue task parallelly to async_worker, blocking the execution of current warp at the same time
		// it is useful to implement read-lock affairs
		template <typename F>
		void queue_routine_parallel(F&& func) {
			size_t warp_index = get_current_warp_index();
			assert(warp_index != ~(size_t)0);
			suspend_warp(warp_index);
			async_worker.queue([this, func = std::forward<F>(func), warp_index]() mutable {
				get_current_warp_index() = warp_index; // force reset warp index
				func(warp_index);
				resume_warp(warp_index);
			});
		}

	protected:
		friend class task_queue_t;
		async_worker_t& async_worker;
		std::vector<task_queue_t> task_queue_grids;
	};
}
