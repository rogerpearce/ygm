// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#include <ygm/comm.hpp>
#include <ygm/container/bag.hpp>
#include <ygm/container/disjoint_set.hpp>
#include <ygm/random.hpp>
#include <ygm/utility.hpp>

#include <vector>

int main(int argc, char** argv) {
  ygm::comm world(&argc, &argv);
  world.welcome();

  size_t num_vertices = 1e8;
  size_t num_edges    = num_vertices * 16;
  world.cout0("Number of vertices = ", num_vertices);
  world.cout0("Number of edges = ", num_edges);

  //
  // Allocate edge list
  std::vector<std::pair<size_t, size_t>> local_edge_list;

  //
  // Generate Random Edges
  ygm::default_random_engine<>    prng(world, 8675309);
  std::uniform_int_distribution<> vgen(0, num_vertices - 1);
  local_edge_list.reserve(num_edges / world.size());
  for (size_t i = 0; i < num_edges / world.size(); ++i) {
    size_t a = vgen(prng);
    size_t b = vgen(prng);
    local_edge_list.push_back({a, b});
  }
  world.barrier();

  //
  // OPTIONAL: This helps current disjoint set implementation
  std::sort(local_edge_list.begin(), local_edge_list.end());

  world.cout0("Completed Edge List");

  // Calculate number of components using union find.
  ygm::container::disjoint_set<size_t> dset(world);

  ygm::timer timer;
  for (size_t i = 0; i < local_edge_list.size(); ++i) {
    dset.async_union(local_edge_list[i].first, local_edge_list[i].second);
    if (i % size_t(1e4) == 0) {
      dset.all_compress();
    }
  }
  world.barrier();
  double elapsed = timer.elapsed();

  world.cout0("Number of components = ", dset.num_sets());
  world.cout0("Elapsed time = ", elapsed, " seconds.");

  return 0;
}