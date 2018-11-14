#ifndef CDSTEST_CHECK_BASKETS_H
#define CDSTEST_CHECK_BASKETS_H

#include <map>
#include <string>
#include <sstream>
#include <utility>
#include <unordered_map>

#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/variance.hpp>
#include <boost/accumulators/statistics/weighted_mean.hpp>
#include <boost/accumulators/statistics/weighted_variance.hpp>

namespace cds_test {

struct BasketsChecker {
  size_t null_basket_count;
  std::map<size_t, size_t> distribution;

  template <class It, class F>
  static BasketsChecker make(It first, It last, F&& extractor) {
    BasketsChecker checker;
    std::unordered_map<cds::uuid_type, size_t> baskets;
    for(; first != last; ++first) {
      auto node_ptr = extractor(*first);
      baskets[node_ptr->m_basket_id]++;
    }
    {
      auto it = baskets.find(0);
      if (it != baskets.end()) {
        checker.null_basket_count = it->second;
        baskets.erase(it);
      } else {
        checker.null_basket_count = 0;
      }
    }
    for(const auto& basket: baskets) {
      checker.distribution[basket.second]++;
    }
    return checker;
  }

  std::string distribution_str() const {
    std::stringstream s;
    for(const auto& cell: distribution) {
      s << '(' << cell.first << ',' << cell.second << ')';
    }
    return s.str();
  }

  std::pair<double, double> mean_std() const {
    using namespace boost::accumulators;
    accumulator_set<double, features<tag::mean, tag::variance>, size_t> acc;
    for_each(distribution.begin(), distribution.end(),
      [&acc](const std::pair<size_t, size_t> cell) {
        acc(cell.first, weight = cell.second);
      }
    );
    return std::pair<double, double>(mean(acc), std::sqrt(variance(acc)));
  }
};

} // namespace cds_test

#endif // #ifndef CDSTEST_CHECK_BASKETS_H
