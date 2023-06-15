// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <ygm/comm.hpp>
#include <ygm/container/map.hpp>
#include <ygm/random.hpp>
#include <ygm/utility.hpp>

#include <vector>

int main(int argc, char **argv) {
  ygm::comm world(&argc, &argv);
  world.welcome();

  size_t num_vertices = 1e8;
  size_t num_edges    = num_vertices * 16;
  world.cout0("Number of vertices = ", num_vertices);
  world.cout0("Number of edges = ", num_edges);

  //
  // Allocate adjacency list
  ygm::container::map<size_t, std::vector<size_t>> adj_list(world);

  //
  // Generate Random Edges
  ygm::default_random_engine<>    prng(world, 8675309);
  std::uniform_int_distribution<> vgen(0, num_vertices - 1);
  for (size_t i = 0; i < num_edges / world.size(); ++i) {
    size_t a = vgen(prng);
    size_t b = vgen(prng);

    adj_list.async_visit(
        a,
        [](size_t key, std::vector<size_t> &adj, size_t v) {
          adj.push_back(v);
        },
        b);
    adj_list.async_visit(
        b,
        [](size_t key, std::vector<size_t> &adj, size_t v) {
          adj.push_back(v);
        },
        a);
  }
  world.barrier();
  world.cout0("Completed Adjacency List");

  ygm::timer timer;
  {
    ygm::container::map<size_t, size_t> map_cc(world);
    ygm::container::map<size_t, size_t> active(world);
    ygm::container::map<size_t, size_t> next_active(world);

    static auto &s_next_active = next_active;
    static auto &s_map_cc      = map_cc;
    static auto &s_adj_list    = adj_list;

    //
    // Init map_cc
    adj_list.for_all(
        [&map_cc, &active](const size_t &vertex, const std::vector<size_t> &) {
          map_cc.async_insert(vertex, vertex);
          active.async_insert(vertex, vertex);
        });
    world.barrier();

    while (active.size() > 0) {
      world.cout0("active.size() = ", active.size());
      active.for_all([](const size_t &vertex, const size_t &cc_id) {
        s_adj_list.async_visit(
            vertex,
            [](const size_t &vertex, const std::vector<size_t> &adj,
               const size_t &cc_id) {
              for (const auto &neighbor : adj) {
                if (cc_id < neighbor) {
                  s_map_cc.async_visit(
                      neighbor,
                      [](const size_t &n, size_t &ncc, const size_t &cc_id) {
                        if (cc_id < ncc) {
                          ncc = cc_id;
                          s_next_active.async_reduce(
                              n, cc_id, [](const size_t &a, const size_t &b) {
                                return std::min(a, b);
                              });
                        }
                      },
                      cc_id);
                }
              }
            },
            cc_id);
      });
      world.barrier();
      active.clear();
      active.swap(next_active);
    }
  }
  world.cout0("CC time = ", timer.elapsed());

  return 0;
}