// Copyright 2019-2021 Lawrence Livermore National Security, LLC and other YGM
// Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: MIT

#pragma once

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>
#include <ygm/io/detail/aws_s3.hpp>

namespace ygm::io {

namespace fs = std::filesystem;

/**
 * @brief Distributed text file parsing.
 *
 */
class line_parser {
 public:
  /**
   * @brief Construct a new line parser object
   *
   * @param comm
   * @param stringpaths
   * @param node_local_filesystem True if paths are to a node-local filesystem
   * @param recursive True if directory traversal should be recursive
   */
  line_parser(ygm::comm& comm, const std::vector<std::string>& stringpaths,
              bool node_local_filesystem = false, bool recursive = false)
      : m_comm(comm), m_node_local_filesystem(node_local_filesystem) {
    if (node_local_filesystem) {
      ASSERT_RELEASE(false);
      check_paths(stringpaths, recursive);
    } else {
      if (m_comm.rank0()) {
        check_paths(stringpaths, recursive);
      }
    }
  }

  /**
   * @brief Executes a user function for every line in a set of files.
   *
   * @tparam Function
   * @param fn User function to execute
   */
  template <typename Function>
  void for_all(Function fn) {
    if (m_node_local_filesystem) {
      ASSERT_RELEASE(false);
      if (m_paths_sizes.empty()) return;
    } else {
      static std::vector<std::tuple<std::string, size_t, size_t>> my_file_paths;
      static std::string& s_s3_bucket = m_s3_bucket;
      //
      //  Splits files over ranks by file size.   8MB is smallest granularity.
      //  This approach could be improved by having rank_layout information.
      m_comm.barrier();
      if (m_comm.rank0()) {
        if (not m_s3_bucket.empty()) {
          m_comm.async_bcast([](std::string s) { s_s3_bucket = s; },
                             m_s3_bucket);
        }
        std::vector<std::tuple<std::string, size_t, size_t>> remaining_files(
            m_paths_sizes.size());
        size_t total_size{0};
        for (size_t i = 0; i < m_paths_sizes.size(); ++i) {
          total_size += m_paths_sizes[i].second;
          remaining_files[i] = std::make_tuple(
              m_paths_sizes[i].first, size_t(0), m_paths_sizes[i].second);
        }

        if (total_size > 0) {
          size_t bytes_per_rank = std::max((total_size / m_comm.size()) + 1,
                                           size_t(8 * 1024 * 1024));
          for (int rank = 0; rank < m_comm.size(); ++rank) {
            size_t remaining_budget = bytes_per_rank;
            while (remaining_budget > 0 && !remaining_files.empty()) {
              size_t file_remaining = std::get<2>(remaining_files.back()) -
                                      std::get<1>(remaining_files.back());
              size_t& cur_position = std::get<1>(remaining_files.back());
              if (file_remaining > remaining_budget) {
                m_comm.async(
                    rank,
                    [](const std::string& fname, size_t bytes_begin,
                       size_t bytes_end) {
                      my_file_paths.push_back(
                          {fs::path(fname), bytes_begin, bytes_end});
                    },
                    (std::string)std::get<0>(remaining_files.back()),
                    cur_position, cur_position + remaining_budget);
                cur_position += remaining_budget;
                remaining_budget = 0;
              } else if (file_remaining <= remaining_budget) {
                m_comm.async(
                    rank,
                    [](const std::string& fname, size_t bytes_begin,
                       size_t bytes_end) {
                      my_file_paths.push_back(
                          {fs::path(fname), bytes_begin, bytes_end});
                    },
                    (std::string)std::get<0>(remaining_files.back()),
                    cur_position, std::get<2>(remaining_files.back()));
                remaining_budget -= file_remaining;
                remaining_files.pop_back();
              }
            }
          }
        }
      }
      m_comm.barrier();

      //
      // Each rank process locally assigned files.
      if (m_s3_bucket.empty()) {
        for (const auto& fname : my_file_paths) {
          // m_comm.cout("Opening: ", std::get<0>(fname), " ",
          // std::get<1>(fname),
          //             " ", std::get<2>(fname));
          std::ifstream ifs(std::get<0>(fname));
          // Note: Current process is responsible for reading up to *AND
          // INCLUDING* bytes_end
          size_t bytes_begin = std::get<1>(fname);
          size_t bytes_end   = std::get<2>(fname);
          ASSERT_RELEASE(ifs.good());
          // ifs.imbue(std::locale::classic());
          std::string line;
          // Throw away line containing bytes_begin as it was read by the
          // previous process (unless it corresponds to the beginning of a file)
          if (bytes_begin > 0) {
            ifs.seekg(bytes_begin);
            std::getline(ifs, line);
          }
          // Keep reading until line containing bytes_end is read
          while (ifs.tellg() <= bytes_end && std::getline(ifs, line)) {
            fn(line);
            // if(ifs.tellg() > bytes_end) break;
          }
        }
        my_file_paths.clear();

      } else {
        // s3 from here!
        for (const auto& fname : my_file_paths) {
          std::string             object        = std::get<0>(fname);
          size_t                  bytes_begin   = std::get<1>(fname);
          size_t                  bytes_end     = std::get<2>(fname);
          size_t                  bytes_to_read = bytes_end - bytes_begin;
          detail::aws_line_reader alr(m_s3_bucket, object, bytes_begin);
          std::string             line;
          while (alr.bytes_read() <= bytes_to_read && alr.getline(line)) {
            fn(line);
          }
        }
        my_file_paths.clear();
      }
    }
  }

 private:
  /**
   * @brief Check readability of paths and iterates through directories
   *
   * @param stringpaths
   * @param recursive
   */
  void check_paths(const std::vector<std::string>& stringpaths,
                   bool                            recursive) {
    //
    //
    for (const std::string& strp : stringpaths) {
      if (strp.rfind("s3://", 0) == 0) {
        // AWS S3 path
        bool found_bucket_end = false;
        for (size_t i = 5; i < strp.size(); ++i) {
          if (found_bucket_end) {
            m_s3_obj_prefix.push_back(strp[i]);
          } else {
            if (strp[i] != '/') {
              m_s3_bucket.push_back(strp[i]);
            } else {
              found_bucket_end = true;
            }
          }
        }
        std::cout << "S3 Bucket: " << m_s3_bucket << std::endl;
        std::cout << "S3 prefix: " << m_s3_obj_prefix << std::endl;
        m_paths_sizes = detail::aws_list_objects(m_s3_bucket, m_s3_obj_prefix);
        for (const auto& ps : m_paths_sizes) {
          std::cout << ps.first << " " << ps.second << std::endl;
        }
      } else {
        fs::path p(strp);
        if (fs::exists(p)) {
          if (fs::is_regular_file(p)) {
            if (is_file_good(p)) {
              m_paths_sizes.push_back({p, fs::file_size(p)});
            }
          } else if (fs::is_directory(p)) {
            if (recursive) {
              //
              // If a directory & user requested recursive
              const std::filesystem::recursive_directory_iterator end;
              for (std::filesystem::recursive_directory_iterator itr{p};
                   itr != end; itr++) {
                if (fs::is_regular_file(itr->path())) {
                  if (is_file_good(itr->path())) {
                    m_paths_sizes.push_back(
                        {itr->path(), fs::file_size(itr->path())});
                  }
                }
              }
            } else {
              //
              // If a directory & user requested recursive
              const std::filesystem::directory_iterator end;
              for (std::filesystem::directory_iterator itr{p}; itr != end;
                   itr++) {
                if (fs::is_regular_file(itr->path())) {
                  if (is_file_good(itr->path())) {
                    m_paths_sizes.push_back(
                        {itr->path(), fs::file_size(itr->path())});
                  }
                }
              }
            }
          }
        }
      }
    }

    //
    // Remove duplicate paths
    std::sort(m_paths_sizes.begin(), m_paths_sizes.end());
    m_paths_sizes.erase(std::unique(m_paths_sizes.begin(), m_paths_sizes.end()),
                        m_paths_sizes.end());
  }

  /**
   * @brief Checks if file is readable
   *
   * @param p
   * @return true
   * @return false
   */
  bool is_file_good(const fs::path& p) {
    std::ifstream ifs(p);
    bool          good = ifs.good();
    if (!good) {
      m_comm.cout("WARNING: unable to open: ", p);
    }
    return good;
  }
  ygm::comm                                   m_comm;
  std::vector<std::pair<std::string, size_t>> m_paths_sizes;
  std::string                                 m_s3_bucket;
  std::string                                 m_s3_obj_prefix;
  bool                                        m_node_local_filesystem;
};

}  // namespace ygm::io
