#pragma once

#include <vector>
#include <algorithm>
#include <cstddef>
#include <utility>

/**
 * AutoExpandVector<T>
 * - Acts like a vector with array-like operator[].
 * - Non-const operator[] auto-expands geometrically (doubling) and fills new slots with a user-specified value.
 * - Const operator[] returns the fill value when idx is out of range (no UB).
 *
 * Example:
 *   AutoExpandVector<int> v(10, -1);
 *   int x = v[50];      // returns -1; size grows to >= 51
 *   v[50] = 42;         // sets index 50
 */
template <typename T>
class AutoExpandVector {
public:
    using value_type      = T;
    using size_type       = std::size_t;
    using reference       = value_type&;
    using const_reference = const value_type&;
    using iterator        = typename std::vector<T>::iterator;
    using const_iterator  = typename std::vector<T>::const_iterator;

    explicit AutoExpandVector(size_type initial_size = 0, const T& fill_value = T())
        : data_(initial_size, fill_value), fill_value_(fill_value) {}

    // Non-const access: expands as needed (geometric growth: double or idx+1)
    reference operator[](size_type idx) {
        if (idx >= data_.size()) {
            size_type new_size = std::max(idx + 1, data_.size() * 2);
            if (new_size == 0) new_size = 1; // handle empty start
            data_.resize(new_size, fill_value_);
        }
        return data_[idx];
    }

    // Const access: returns fill_value if out-of-range
    value_type operator[](size_type idx) const {
        return (idx < data_.size()) ? data_[idx] : fill_value_;
    }

    // Vector-like API
    void clear()                { data_.clear(); }
    void shrink_to_fit()        { data_.shrink_to_fit(); }
    void reserve(size_type n)   { data_.reserve(n); }
    size_type size() const      { return data_.size(); }
    size_type capacity() const  { return data_.capacity(); }
    bool empty() const          { return data_.empty(); }

    // Fill value controls
    const T& fill_value() const { return fill_value_; }
    void set_fill_value(const T& v) { fill_value_ = v; }

    // Iterators
    iterator begin()            { return data_.begin(); }
    iterator end()              { return data_.end(); }
    const_iterator begin() const{ return data_.begin(); }
    const_iterator end() const  { return data_.end(); }
    const_iterator cbegin() const { return data_.cbegin(); }
    const_iterator cend() const { return data_.cend(); }

    // Raw data access
    T* data()                   { return data_.data(); }
    const T* data() const       { return data_.data(); }

private:
    std::vector<T> data_;
    T fill_value_;
};

