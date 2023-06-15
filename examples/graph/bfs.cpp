// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <ygm/comm.hpp>
#include <ygm/container/map.hpp>
#include <ygm/container/set.hpp>
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
    ygm::container::set<size_t> visited(world);
    ygm::container::set<size_t> current_level(world);
    ygm::container::set<size_t> next_level(world);
    size_t                      level_number = 0;

    static auto &s_next_level = next_level;
    static auto &s_visited    = visited;

    // Start from vertex 0
    current_level.async_insert(0);

    while (current_level.size() > 0) {
      world.cout0("BFS Level ", level_number, " size = ", current_level.size());
      current_level.for_all([&adj_list](const size_t vertex) {
        adj_list.async_visit(
            vertex, [](size_t vertex, std::vector<size_t> &adj) {
              for (auto neighbor : adj) {
                s_visited.async_insert_exe_if_missing(
                    neighbor, [](size_t n) { s_next_level.async_insert(n); });
              }
            });
      });
      world.barrier();
      current_level.clear();
      next_level.swap(current_level);
      level_number++;
    }
  }

  return 0;
}