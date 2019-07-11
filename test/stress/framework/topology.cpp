#include "topology.h"

#include <cassert>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <algorithm>

#include <sched.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <stdlib.h>

extern "C" {
static int is_directory(char const *const path)
{
    static struct stat info;
    if (stat(path, &info) != 0) {
        return 0;
    }
    if (info.st_mode & S_IFDIR) {
        return 1;
    }
    return 0;
}
}

namespace cds_test {
namespace utils {
namespace topology {

    std::ostream &operator<<(std::ostream &os, const core_id &c)
    {
        return os << '{' << c.logical << ',' << c.socket << ',' << c.physical << '}';
    }

    Topology::Topology(std::size_t threads_num) : m_threads_num(threads_num)
    {
        auto cpus = read_cpus();
        for (const auto &cpu : cpus) {
            m_info[cpu.socket][cpu.core].insert(cpu.id);
        }
        m_max_threads = cpus.size();
        std::size_t socket_size = m_info.begin()->second.size();
        for (const auto &element : m_info) {
            if (socket_size != element.second.size()) {
                throw std::runtime_error("Unequall socket sizes");
            }
        }
        if (m_threads_num > m_max_threads) {
            std::ostringstream err;
            err << "Too many threads. Use " << m_max_threads << " threads.";
            throw std::runtime_error(err.str());
        }
        make_mapping();
    }

    std::size_t read_size(const std::string &base, char const *const file)
    {
        std::ostringstream fname;
        fname << base;
        fname << file;
        std::ifstream f(fname.str());
        if (!f) {
            throw std::runtime_error("Can't find file " + fname.str());
        }
        std::size_t ret;
        f >> ret;
        return ret;
    }

    auto Topology::read_cpus() -> std::vector<CPUInfo>
    {
        std::vector<CPUInfo> ret;
        size_t id = 0;
        std::string base("/sys/devices/system/cpu/cpu");
        while (true) {
            std::ostringstream dir;
            dir << base;
            dir << id;
            if (!is_directory(dir.str().c_str())) {
                break;
            }
            dir << "/topology/";
            std::size_t core = read_size(dir.str(), "core_id");
            std::size_t socket = read_size(dir.str(), "physical_package_id");
            ret.push_back(CPUInfo{id, core, socket});
            ++id;
        }
        return ret;
    }

    void Topology::make_mapping()
    {
        bool packed = getenv("PACKED") != nullptr;
        bool spread = getenv("SPREAD") != nullptr;
        bool numa = getenv("NUMA") != nullptr;
        if (!packed && !spread) {
          make_default_mapping();
          return;
        }
        assert(packed != spread);
        make_specific_mapping(packed, numa);
    }

    void Topology::make_default_mapping()
    {
        m_sockets = 0;
        std::size_t populus = 0;
        // Take the smallest amount of sockets.
        for (const auto &element : m_info) {
            m_sockets++;
            for(auto& e: element.second) {
              populus += e.second.size();
            }
            if (populus >= m_threads_num) {
                break;
            }
        }
        assert(sockets != 0);
        std::size_t step_size = m_threads_num % m_sockets;
        std::size_t common_size = m_threads_num / m_sockets;
        auto per_socket = [&step_size,
                           &common_size](const std::size_t s) -> std::size_t {
            return common_size + ((s < step_size) ? 1 : 0);
        };
        m_socket_boundary = per_socket(0);
        auto info_pos = m_info.begin();
        for (std::size_t s = 0; s < m_sockets; ++s, ++info_pos) {
            std::size_t end = per_socket(s);
            m_node_info.emplace_back(end);
            auto socket_pos = info_pos->second.begin();
            size_t c = 0;
            while(c < end) {
              for(auto& e: socket_pos->second) {
                m_mapping.emplace_back(c, info_pos->first, e);
                ++c;
              }
              ++socket_pos;
            }
        }
    }

    const std::size_t &Topology::socket_boundary() const {
      if(m_sockets > 2) {
        throw std::logic_error("Can't have socket boundary for that many sockets");
      }
      return m_socket_boundary;
    }

    void Topology::make_specific_mapping(bool packed, bool numa)
    {
      m_sockets = numa ? m_info.size() : 1;
      auto info_pos = m_info.begin();
      for(size_t s = 0; s < m_sockets; ++s, ++info_pos) {
        size_t core = 0;
        auto& socket = info_pos->second;
        for(auto socket_pos = socket.begin(); socket_pos != socket.end(); ++core, ++socket_pos) {
          for(auto& physical: socket_pos->second) {
            m_mapping.emplace_back(core, info_pos->first, physical);
          }
        }
      }
      m_socket_boundary = m_threads_num / m_sockets;
      if (m_threads_num > m_mapping.size()) {
          std::ostringstream err;
          err << "Too many threads. For this amount of sockets, use " << m_mapping.size() << " threads.";
          throw std::runtime_error(err.str());
      }
      // Sorts by (hypercore, socket, logical_core)
      std::sort(m_mapping.begin(), m_mapping.end(), [](const core_id& lhs, const core_id& rhs) {
        return std::less<size_t>{}(lhs.physical, rhs.physical);
      });
      if(packed) {
        // Sorts by (logical core, hypercore, socket)
        std::stable_sort(m_mapping.begin(), m_mapping.end(), [](const core_id& lhs, const core_id& rhs) {
          return std::less<size_t>{}(lhs.logical, rhs.logical);
        });
      }
      m_mapping.erase(std::next(m_mapping.begin(), m_threads_num), m_mapping.end());
      m_node_info.resize(m_sockets);
      std::fill_n(m_node_info.begin(), m_sockets, 0);
      for(auto& c: m_mapping) {
        m_node_info[c.socket]++;
      }
    }

    void Topology::pin_thread(std::size_t thread_num) const
    {
        if (thread_num >= m_mapping.size()) {
            std::cerr << "Invalid tid " << thread_num;
            return;
        }
        cpu_set_t cpu_set;
        CPU_ZERO(&cpu_set);
        CPU_SET(mapping()[thread_num].physical, &cpu_set);
        sched_setaffinity(0 /*this thread*/, sizeof(cpu_set), &cpu_set);
    }

    void Topology::verify_pin(std::size_t thread_num) const
    {
        std::size_t cpu = sched_getcpu();
        if (cpu != mapping()[thread_num].physical) {
            std::terminate();
        }
    }

    std::ostream& operator<<(std::ostream& os, const Topology& topology) {
      os << '{';
      auto& mapping = topology.mapping();
      for(size_t i = 0; i < mapping.size(); ++i) {
        os << i << ':' << mapping[i].physical << ',';
      }
      os << '}';
    }

}
}
} // namespace cds_test::utils::topology
