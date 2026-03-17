#include "rix/core/timer.hpp"

namespace rix {
namespace core {

Timer::Timer(const rix::util::Duration &duration, Callback callback) : duration_(duration), callback_(callback) {
    event_.last_expected = event_.last_real = rix::util::Time(0.0);
    event_.current_expected = event_.current_real = rix::util::Time::now();
    event_.last_duration = rix::util::Duration(0.0);
}

Timer::~Timer() {}

bool Timer::ok() const { return !shutdown_flag_; }

void Timer::shutdown() { shutdown_flag_ = true; }

void Timer::spin_once() {
    event_.current_real = rix::util::Time::now();
    if (event_.current_real - event_.last_real > duration_) {
        std::lock_guard<std::mutex> guard(callback_mutex_);
        event_.current_expected = event_.last_expected + duration_;
        event_.last_duration = event_.current_real - event_.last_real;
        callback_(event_);
        event_.last_real = event_.current_real;
        event_.last_expected = event_.current_expected;
    }
}

void Timer::set_callback(Callback callback) { callback_ = callback; }

Timer::Callback Timer::get_callback() const { return callback_; }

}  // namespace core
}  // namespace rix