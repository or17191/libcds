#include "topology.h"

#include <cassert>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>

#include <sched.h>
#include <sys/stat.h>
#include <sys/types.h>

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
        m_max_threads = 0;
        std::size_t socket_size = m_info.begin()->second.size();
        for (const auto &element : m_info) {
            m_max_threads += element.second.size();
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
        std::size_t sockets = 0;
        std::size_t populus = 0;
        // Take the smallest amount of sockets.
        for (const auto &element : m_info) {
            sockets++;
            populus += element.second.size();
            if (populus >= m_threads_num) {
                break;
            }
        }
        assert(sockets != 0);
        std::size_t step_size = m_threads_num % sockets;
        std::size_t common_size = m_threads_num / sockets;
        auto per_socket = [&step_size,
                           &common_size](const std::size_t s) -> std::size_t {
            return common_size + ((s < step_size) ? 1 : 0);
        };
        auto info_pos = m_info.begin();
        for (std::size_t s = 0; s < sockets; ++s, ++info_pos) {
            std::size_t end = per_socket(s);
            m_node_info.emplace_back(end);
            auto socket_pos = info_pos->second.begin();
            for (std::size_t c = 0; c < end; ++c, ++socket_pos) {
                m_mapping.emplace_back(c, info_pos->first, *socket_pos->second.begin());
            }
        }
    }

    void Topology::pin_thread(std::size_t thread_num) const
    {
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

}
}
} // namespace cds_test::utils::topology
