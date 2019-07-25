#! /usr/bin/env zsh

source ${HOME}/.zshrc

set +x

# tnums=(1 2 4 8 12 16 20 24 28 32 36 40)
tnums=(1 2 $(seq 4 4 80))
# N=30
N=5

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
    grep '\.WF\|SB' |
    grep -v -i 'DHP\|ic\|stat\|intrusive')
   
  echo "Testing ${test_name}"
  echo "${tests}"
  if [ $test_name = "stress-queue-push-pop" ]; then
    # Pushers and poppers in different sockets
    export FORCE_NUMA=
    unset NOHT
  else
    # Only one type of thread, both in the same socket
    unset FORCE_NUMA
    unset NOHT
  fi

  for n in $tnums; do
    if [ $test_name = "stress-queue-push-pop" ]; then
      if [ $n -eq 1 ]; then
        continue;
      fi
    else
      if [ $n -gt 44 ]; then
        continue;
      fi
    fi
    echo "TNUMS=${n}"
    cp "test.conf.template" "test.conf";
    sed -i.bak "s/###/${n}/" "test.conf";
    sed -i.bak "s/@@@/$(( n/2 ))/" "test.conf";

    DIR="memkind_all_${n}";
    echo "$DIR";
    [ -d "$DIR" ] || mkdir $DIR;
    rm -f "${DIR}"/*

    for test in ${(@f)tests}; do
      echo "${test}"
      for i in {1..${N}}; do
        echo "${i}";
        yes | "${executable}" --gtest_filter="${test}" --gtest_output=xml:"${DIR}/res_${test}_${i}.xml" > "${DIR}/out_${test}_${i}.txt";
      done
    done
  done 

  unset FORCE_NUMA
  unset NOHT
}

test_executable "stress-queue-$1"
