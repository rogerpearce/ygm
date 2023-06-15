// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <set>
#include <ygm/collective.hpp>
#include <ygm/comm.hpp>
#include <ygm/container/map.hpp>
#include <ygm/random.hpp>
#include <ygm/utility.hpp>

int main(int argc, char **argv) {
  ygm::comm world(&argc, &argv);
  world.welcome();

  size_t num_vertices = 1e8;
  size_t num_edges    = num_vertices * 16;
  size_t kcore        = 10;
  world.cout0("Number of vertices = ", num_vertices);
  world.cout0("Number of edges = ", num_edges);
  world.cout0("kcore = ", kcore);

  //
  // Allocate adjacency list
  ygm::container::map<size_t, std::set<size_t>> adj_set(world);

  //
  // Generate Random Edges
  ygm::default_random_engine<>    prng(world, 8675309);
  std::uniform_int_distribution<> vgen(0, num_vertices - 1);
  for (size_t i = 0; i < num_edges / world.size(); ++i) {
    size_t a = vgen(prng);
    size_t b = vgen(prng);

    adj_set.async_visit(
        a, [](size_t key, std::set<size_t> &adj, size_t v) { adj.insert(v); },
        b);
    adj_set.async_visit(
        b, [](size_t key, std::set<size_t> &adj, size_t v) { adj.insert(v); },
        a);
  }
  world.barrier();
  world.cout0("Completed Adjacency Set");

  //
  // Compute K-core
  ygm::timer timer;
  size_t     total_locally_pruned = 0;
  size_t     locally_pruned       = 0;
  do {
    locally_pruned = 0;
    adj_set.for_all(
        [kcore, &adj_set, &locally_pruned](size_t vert, std::set<size_t> &adj) {
          if (!adj.empty() && adj.size() < kcore) {
            //
            // Found vertex to prune, go tell all neighbors of my demise
            for (size_t neighbor : adj) {
              adj_set.async_visit(
                  neighbor,
                  [](size_t, std::set<size_t> &adj, size_t v) { adj.erase(v); },
                  vert);
            }
            adj.clear();
            ++locally_pruned;
          }
        });
    world.barrier();
    total_locally_pruned += locally_pruned;
  } while (ygm::sum(locally_pruned, world) > 0);
  world.cout0("K-Core time = ", timer.elapsed());
  world.cout0("Pruned ", ygm::sum(total_locally_pruned, world), " vertices.");

  return 0;
}