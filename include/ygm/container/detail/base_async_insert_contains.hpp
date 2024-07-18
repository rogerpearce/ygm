// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <tuple>
#include <utility>

namespace ygm::container::detail {

template <typename derived_type, typename for_all_args>
struct base_async_insert_contains {

  template<typename Function, typename... FuncArgs>
  void async_insert_contains(const std::tuple_element<0, for_all_args>::type& value,
                             Function fn, const FuncArgs&... args) {

    derived_type* derived_this = static_cast<derived_type*>(this);

    int dest = derived_this->partitioner.owner(value);

    auto lambda = [](auto pcont,
                      const std::tuple_element<0, for_all_args>::type& value,
                      const FuncArgs&... args) {

      Function* fn = nullptr;
      bool contains = static_cast<bool>(pcont->local_count(value));
      if (!contains) {
        pcont->local_insert(value);
      } 

      ygm::meta::apply_optional(*fn, std::make_tuple(pcont),
                                std::forward_as_tuple(contains, value, args...));
    };

    derived_this->comm().async(dest, lambda, derived_this->get_ygm_ptr(),
                              value, args...);
  }
};

}  // namespace ygm::container::detail