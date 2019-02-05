// Copyright (c) 2006-2018 Maxim Khizhinsky
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef CDSTEST_TOPOLOGY_H
#define CDSTEST_TOPOLOGY_H

#include <cstdint>
#include <map>
#include <ostream>
#include <set>
#include <vector>

namespace cds_test {
namespace utils {
namespace topology {

    struct core_id
    {
        std::size_t logical;
        std::size_t socket;
        std::size_t physical;

        core_id(std::size_t l, std::size_t s, std::size_t p)
            : logical(l), socket(s), physical(p) {}
    };

    std::ostream &operator<<(std::ostream &os, const core_id &c);

    class Topology
    {
    public:
        using mapping_type = std::vector<core_id>;
        using node_info_type = std::vector<std::size_t>;

        explicit Topology(std::size_t threads_num);

        const std::size_t &max_threads() const { return m_max_threads; }
        bool is_valid() const { return m_threads_num <= m_max_threads; }
        const mapping_type &mapping() const { return m_mapping; }
        const node_info_type &node_info() const { return m_node_info; }
        const std::size_t &threads_num() const { return m_threads_num; }

        void pin_thread(std::size_t thread_num) const;
        void verify_pin(std::size_t thread_num) const;

    private:
        using core_info = std::set<std::size_t>;
        using socket_info = std::map<size_t, core_info>;
        using info_type = std::map<size_t, socket_info>;
        struct CPUInfo
        {
            std::size_t id;
            std::size_t core;
            std::size_t socket;
        };
        static std::vector<CPUInfo> read_cpus();
        void make_mapping();
        void make_default_mapping();
        void make_specific_mapping(bool packed, bool numa);

        std::size_t m_threads_num;
        std::size_t m_max_threads;
        info_type m_info;
        mapping_type m_mapping;
        node_info_type m_node_info;
    };

    std::ostream& operator<<(std::ostream& os, const Topology&);

}
}
} // namespace cds_test::utils::topology

#endif // CDSTEST_TOPOLOGY_H
