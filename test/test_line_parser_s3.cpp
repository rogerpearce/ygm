// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#undef NDEBUG

#include <filesystem>
#include <ygm/comm.hpp>
#include <ygm/container/counting_set.hpp>
#include <ygm/io/line_parser.hpp>

namespace fs = std::filesystem;

void test_line_parser_files_s3(ygm::comm&, const std::vector<std::string>&,
                               const std::vector<std::string>&);
// void test_line_parser_directory(ygm::comm&, const std::string&, size_t);

int main(int argc, char** argv) {
  ygm::comm world(&argc, &argv);

  {
    test_line_parser_files_s3(world, {"s3://metalldata-test/data/short.txt"},
                              {"data/short.txt"});
    test_line_parser_files_s3(
        world, {"s3://metalldata-test/data/loremipsum/"},
        {"data/loremipsum/loremipsum_0.txt", "data/loremipsum/loremipsum_1.txt",
         "data/loremipsum/loremipsum_2.txt", "data/loremipsum/loremipsum_3.txt",
         "data/loremipsum/loremipsum_4.txt"});
    test_line_parser_files_s3(
        world, {"s3://metalldata-test/data/loremipsum_large.txt"},
        {"data/loremipsum_large.txt"});
    // test_line_parser_files(world, {"data/loremipsum/loremipsum_0.txt",
    //                          "data/loremipsum/loremipsum_1.txt"});
    // test_line_parser_files(world, {"data/loremipsum/loremipsum_0.txt",
    //                          "data/loremipsum/loremipsum_1.txt",
    //                          "data/loremipsum/loremipsum_2.txt"});
    // test_line_parser_files(world, {"data/loremipsum/loremipsum_0.txt",
    //                          "data/loremipsum/loremipsum_1.txt",
    //                          "data/loremipsum/loremipsum_2.txt",
    //                          "data/loremipsum/loremipsum_3.txt"});
    // test_line_parser_files(
    //     world,
    //     {"data/loremipsum/loremipsum_0.txt",
    //     "data/loremipsum/loremipsum_1.txt",
    //      "data/loremipsum/loremipsum_2.txt",
    //      "data/loremipsum/loremipsum_3.txt",
    //      "data/loremipsum/loremipsum_4.txt"});
    // test_line_parser_files(world, {"data/loremipsum_large.txt"});
    // test_line_parser_files(
    //     world,
    //     {"data/loremipsum/loremipsum_0.txt",
    //     "data/loremipsum/loremipsum_1.txt",
    //      "data/loremipsum/loremipsum_2.txt",
    //      "data/loremipsum/loremipsum_3.txt",
    //      "data/loremipsum/loremipsum_4.txt", "data/loremipsum_large.txt"});

    // test_line_parser_directory(world, "data/loremipsum", 270);
    // test_line_parser_directory(world, "data/loremipsum/", 270);
  }

  return 0;
}

void test_line_parser_files_s3(ygm::comm&                      comm,
                               const std::vector<std::string>& s3uris,
                               const std::vector<std::string>& tocheck) {
  //
  // Read in each line into a distributed set
  ygm::container::counting_set<std::string> line_set_to_test(comm);
  ygm::io::line_parser                      bfr(comm, s3uris);
  bfr.for_all([&line_set_to_test](const std::string& line) {
    line_set_to_test.async_insert(line);
  });

  //
  // Read each line sequentially
  ygm::container::counting_set<std::string> line_set(comm);
  std::set<std::string>                     line_set_sequential;
  for (const auto& f : tocheck) {
    std::ifstream ifs(f.c_str());
    ASSERT_RELEASE(ifs.good());
    std::string line;
    while (std::getline(ifs, line)) {
      line_set.async_insert(line);
      line_set_sequential.insert(line);
    }
  }
  comm.barrier();

  ASSERT_RELEASE(line_set.size() == line_set_sequential.size());
  comm.cout0(line_set.size(), " =? ", line_set_to_test.size());
  ASSERT_RELEASE(line_set.size() == line_set_to_test.size());
  // ASSERT_RELEASE(line_set == line_set_to_test);
}
