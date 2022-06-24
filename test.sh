#!/bin/bash
set -ex
SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";

dir=`mktemp -d`
cd $dir
cmake -DCMAKE_BUILD_TYPE=$1 -Dgcl_build_tests=ON $SCRIPT_DIR
cmake --build . -j 4
ctest --verbose
cd -
rm -rf $dir

