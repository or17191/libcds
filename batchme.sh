#! /usr/bin/env bash

set -eux

ORIGINAL_ROOT=$(pwd)
cd $(dirname $0)
cd build-release

HW_COUNT=$(cat /proc/cpuinfo | grep '^processor' | wc -l)
SOCKET_COUNT=$(grep 'physical id' /proc/cpuinfo | sort | uniq | wc -l)
HW_COUNT=$(( HW_COUNT/SOCKET_COUNT*2 ))
tnums=(1 2 $(seq 4 4 ${HW_COUNT}))
N=5
TIMOUT=600 # 10 mins

## stress sync
# make stress-sync
# for n in $tnums; do
#   DIR="sync_${n}";
#   echo "$DIR";
#   [ -d "$DIR" ] || mkdir $DIR;
# 
#   sed "s/###/${n}/" "test.conf.template" > "test_${n}.conf";
# 
#   GFILTER="counter_inc.*"
# 
#   for i in {1..${N}}; do
#     echo "${i}";
#     # echo "bin/stress-sync --cfg=test_${n}.conf --gtest_filter=${GFILTER} --gtest_output=xml:${DIR}/res${i}.xml > ${DIR}/out${i}.txt"
#     bin/stress-sync --cfg="test_${n}.conf" --gtest_filter="${GFILTER}" --gtest_output=xml:"${DIR}/res${i}.xml" > "${DIR}/out${i}.txt";
#   done
# done 

# export NUMA=

function test_executable() {
  test_name=$1
  executable="bin/${test_name}"
  make $test_name
  tests=$(
    $executable --gtest_list_tests |
    ../discover_tests.py |
    grep 'sb' |
    grep -v -i 'DHP\|ic\|stat\|intrusive')
   
  echo "Testing ${test_name}"
  echo "${tests}"
  local force_numa=${FORCE_NUMA:-}
  echo "FORCE_NUMA=${force_numa}"
  local noht=${noht:-}
  echo "NOHT=${noht}"

  for n in ${tnums[@]}; do
    DIR="memkind_all_${n}";
    echo "$DIR";
    if [ -d "$DIR" ] && [ "$(ls "$DIR")" ]; then
      rm "${DIR}"/*
    elif [ ! -d "$DIR" ]; then
      mkdir "$DIR"
    fi

    if [ "$test_name" == "stress-queue-push-pop" ]; then
      if [ $n -eq 1 ]; then
        continue;
      fi
    else
      if [ $n -gt $(( HW_COUNT/2 ))  ] && [ -z "${force_numa}" ]; then
        continue;
      fi
    fi
    echo "TNUMS=${n}"
    cp "../test.conf.template" "test.conf";
    sed -i.bak "s/###/${n}/" "test.conf";
    sed -i.bak "s/@@@/$(( n/2 ))/" "test.conf";

    set +e

    while read "test"; do
      echo "${test}"
      if [[ "${test}" == *"Vanilla"* ]] && [ "${n}" -gt 10 ]; then
        continue;
      fi
      for i in $(seq 1 ${N}); do
        echo "${i}";
        # echo "${executable}" --gtest_filter="${test}" --gtest_output=xml:"${DIR}/res_${test}_${i}.xml" > "${DIR}/out_${test}_${i}.txt";
        timeout "${TIMEOUT}" "${executable}" --gtest_filter="${test}" --gtest_output=xml:"${DIR}/res_${test}_${i}.xml" > "${DIR}/out_${test}_${i}.txt";
      done
    done <<< "$tests"

    set -e
  done 
}

unset NOHT
unset FORCE_NUMA

test_executable "stress-queue-push"
tar cvf "${ORIGINAL_ROOT}/results.push.tar" memkind_*
test_executable "stress-queue-pop"
tar cvf "${ORIGINAL_ROOT}/results.pop.tar" memkind_*

export FORCE_NUMA=1
# test_executable "stress-queue-push"
# tar cvf results.push-numa.tar memkind_*
test_executable "stress-queue-push-pop"
tar cvf "${ORIGINAL_ROOT}/results.push-pop.tar" memkind_*

unset FORCE_NUMA
