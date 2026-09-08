#pragma once

#include <cassert>
#include <cstddef>
#include <limits>
#include <memory>
#include <type_traits>
#include <utility>

#ifndef NDEBUG
#include <thread>
#endif

namespace keylane {
namespace local_shared_detail {

template <typename T>
struct Control {
  std::size_t references_ = 1;
  void (*destroy_)(Control*) noexcept;
#ifndef NDEBUG
  const std::thread::id owner_ = std::this_thread::get_id();
#endif
  T value_;

  template <typename... Args>
  explicit Control(void (*destroy)(Control*) noexcept, Args&&... args)
      : destroy_(destroy), value_(std::forward<Args>(args)...) {}

  void AssertOwner() const noexcept {
#ifndef NDEBUG
    assert(owner_ == std::this_thread::get_id() &&
           "LocalSharedPtr used outside its owner thread");
#endif
  }
};

// The erased destructor retains the allocator that owns this single combined
// allocation. There is no weak count or separately allocated control block.
template <typename T, typename Allocator>
struct Allocation final : Control<T> {
  using ReboundAllocator = typename std::allocator_traits<
      Allocator>::template rebind_alloc<Allocation>;
  using Traits = std::allocator_traits<ReboundAllocator>;

  [[no_unique_address]] ReboundAllocator allocator_;

  template <typename... Args>
  explicit Allocation(const ReboundAllocator& allocator, Args&&... args)
      : Control<T>(&Destroy, std::forward<Args>(args)...),
        allocator_(allocator) {}

  static void Destroy(Control<T>* control) noexcept {
    auto* allocation = static_cast<Allocation*>(control);
    // Copy before destroying the stored allocator along with the object.
    ReboundAllocator allocator(allocation->allocator_);
    Traits::destroy(allocator, allocation);
    Traits::deallocate(allocator, allocation, 1);
  }
};

}  // namespace local_shared_detail

// Shared ownership confined to the allocating thread, including copies,
// moves, access and final destruction. Coroutine suspension is safe only if
// the coroutine resumes on that same owner. Cross-thread callers must retain
// an independently thread-safe handle that routes work AND cleanup to it.
// Debug builds assert affinity before touching the non-atomic count.
//
// This deliberately supports only objects and T -> const T conversion, not
// arrays, base-class conversions, aliasing, weak references or raw adoption.
// That restriction keeps each handle one pointer wide without intrusive
// fields in T. Objects must be created through Allocate/MakeLocalShared.
template <typename T>
class LocalSharedPtr {
  static_assert(std::is_object_v<T> && !std::is_array_v<T> &&
                !std::is_volatile_v<T>);
  using Object = std::remove_const_t<T>;
  using Control = local_shared_detail::Control<Object>;

  template <typename U>
  static constexpr bool kCompatible =
      std::is_same_v<Object, std::remove_const_t<U>> &&
      std::is_convertible_v<U*, T*>;

 public:
  constexpr LocalSharedPtr() noexcept = default;
  constexpr LocalSharedPtr(std::nullptr_t) noexcept {}

  LocalSharedPtr(const LocalSharedPtr& other) noexcept
      : control_(other.CheckedControl()) {
    Retain();
  }
  template <typename U>
    requires kCompatible<U>
  LocalSharedPtr(const LocalSharedPtr<U>& other) noexcept
      : control_(other.CheckedControl()) {
    Retain();
  }

  LocalSharedPtr(LocalSharedPtr&& other) noexcept
      : control_(other.CheckedControl()) {
    other.control_ = nullptr;
  }
  template <typename U>
    requires kCompatible<U>
  LocalSharedPtr(LocalSharedPtr<U>&& other) noexcept
      : control_(other.CheckedControl()) {
    other.control_ = nullptr;
  }

  ~LocalSharedPtr() { reset(); }

  LocalSharedPtr& operator=(const LocalSharedPtr& other) noexcept {
    LocalSharedPtr(other).swap(*this);
    return *this;
  }
  template <typename U>
    requires kCompatible<U>
  LocalSharedPtr& operator=(const LocalSharedPtr<U>& other) noexcept {
    LocalSharedPtr(other).swap(*this);
    return *this;
  }
  LocalSharedPtr& operator=(LocalSharedPtr&& other) noexcept {
    LocalSharedPtr(std::move(other)).swap(*this);
    return *this;
  }
  template <typename U>
    requires kCompatible<U>
  LocalSharedPtr& operator=(LocalSharedPtr<U>&& other) noexcept {
    LocalSharedPtr(std::move(other)).swap(*this);
    return *this;
  }
  LocalSharedPtr& operator=(std::nullptr_t) noexcept {
    reset();
    return *this;
  }

  void reset() noexcept {
    auto* control = CheckedControl();
    control_ = nullptr;
    if (control != nullptr && --control->references_ == 0)
      control->destroy_(control);
  }
  void swap(LocalSharedPtr& other) noexcept {
    (void)CheckedControl();
    (void)other.CheckedControl();
    std::swap(control_, other.control_);
  }

  T* get() const noexcept {
    auto* control = CheckedControl();
    return control != nullptr ? std::addressof(control->value_) : nullptr;
  }
  T& operator*() const noexcept {
    assert(control_ != nullptr);
    return *get();
  }
  T* operator->() const noexcept {
    assert(control_ != nullptr);
    return get();
  }
  explicit operator bool() const noexcept {
    return CheckedControl() != nullptr;
  }
  std::size_t use_count() const noexcept {
    const auto* control = CheckedControl();
    return control != nullptr ? control->references_ : 0;
  }
  friend bool operator==(const LocalSharedPtr& pointer,
                         std::nullptr_t) noexcept {
    return !pointer;
  }

 private:
  template <typename U>
  friend class LocalSharedPtr;
  template <typename U, typename Allocator, typename... Args>
  friend LocalSharedPtr<U> AllocateLocalShared(const Allocator&, Args&&...);

  explicit LocalSharedPtr(Control* control) noexcept : control_(control) {}
  Control* CheckedControl() const noexcept {
    if (control_ != nullptr) control_->AssertOwner();
    return control_;
  }
  void Retain() noexcept {
    if (control_ == nullptr) return;
    assert(control_->references_ != std::numeric_limits<std::size_t>::max());
    ++control_->references_;
  }

  Control* control_ = nullptr;
};

// Constructs T and its non-atomic control block in one allocator-owned
// allocation. Rebinding preserves stateful allocator accounting. Constructor
// failure returns that allocation before propagating the exception; allocator
// failure follows the supplied allocator's policy. Fancy pointers are not
// supported. T's destructor and allocator destruction/deallocation must not
// throw, and the supplied allocator must outlive any resources it borrows.
template <typename T, typename Allocator, typename... Args>
LocalSharedPtr<T> AllocateLocalShared(const Allocator& allocator,
                                      Args&&... args) {
  using Block =
      local_shared_detail::Allocation<std::remove_const_t<T>, Allocator>;
  using Traits = typename Block::Traits;
  static_assert(std::is_same_v<typename Traits::pointer, Block*>);
  typename Block::ReboundAllocator rebound(allocator);
  auto* block = Traits::allocate(rebound, 1);
  try {
    Traits::construct(rebound, block, rebound, std::forward<Args>(args)...);
  } catch (...) {
    Traits::deallocate(rebound, block, 1);
    throw;
  }
  return LocalSharedPtr<T>(block);
}

// Convenience factory with the standard allocator and the same owner-thread
// lifetime contract as AllocateLocalShared.
template <typename T, typename... Args>
LocalSharedPtr<T> MakeLocalShared(Args&&... args) {
  return AllocateLocalShared<T>(std::allocator<std::remove_const_t<T>>{},
                                std::forward<Args>(args)...);
}

}  // namespace keylane
