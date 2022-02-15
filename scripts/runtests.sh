#!/usr/bin/env bash
##
## run tests in multiple environments
##    @script.name [option]
##
## This runs the project tests inside multiple docker images and collects
## results.
##
## How it works:
##
## For each image listed in test_images.txt,
##
## 1. run the docker container, downloading the image if not cached yet
## 2. install the project (pip install -e)
## 3. install any additional dependencies, if listed for the image
## 4. run the tests
## 5. freeze pip packages (for reproducing and reference)
## 6. collect test results (creates a tgz for each run)
##
## Finally, print a test summary and exist with non-zero if any one of the
## tests failed.
##
# script setup to parse options
script_dir=$(dirname "$0")
script_dir=$(realpath $script_dir)
source $script_dir/easyoptions || exit

# location of project source under test (matches in-container $test_base)
sources_dir=$script_dir/..
# all images we want to test, and list of tests
test_images=$script_dir/docker/test_images.txt
# in-container location of project source under test
test_base=/var/project
# host log files
test_logbase=/tmp/testlogs
# host test rc file
test_logrc=$test_logbase/tests_rc.log

# start clean
rm -rf $test_logbase
mkdir -p $test_logbase
# images to test against
while IFS=';' read -r image tests extras pipreq label; do
  test_label=${label:-${tests//[^[:alnum:]]/_}}
  # host name of log directory for this test
  test_logdir=$test_logbase/$(dirname $image)/$(basename $image)/$test_label
  # host name of log file
  test_logfn=$test_logdir/$(basename $image).log
  # host name of pip freeze output file
  test_pipfn=$test_logdir/pip-requirements.lst
  # host name of final results tar
  test_logtar=$test_logbase/$(dirname $image)_$(basename $image)_$test_label.tgz
  # start test container
  mkdir -p $test_logdir
  extras=${extras:-dev}
  pipreq=${pipreq:-pip}
  docker rm -f omegaml-test
  docker run --network host \
             --name omegaml-test \
             -dt \
             -e TESTS="$tests" \
             -e EXTRAS="dev,$extras"\
             -e PIPREQ="'$pipreq'" \
             -v $sources_dir:$test_base \
             -w $test_base $image \
             bash
  # run commands, collect results, cleanup
  docker exec omegaml-test bash -c 'make install test; echo $? > /tmp/test.status' 2>&1 | tee -a $test_logfn
  docker exec omegaml-test bash -c "cat /tmp/test.status" | xargs -I RC echo "$test_logdir==RC" >> $test_logrc
  docker exec omegaml-test bash -c "pip list --format freeze" | tee -a ${test_pipfn}
  tar -czf $test_logtar $test_logdir --remove-files
  docker kill omegaml-test
done < <(cat $test_images | grep -v "#")

# print summary
echo "All Tests Summary (==return code)"
echo "================="
cat $test_logrc
echo "-----------------"
# man grep: exit status is 0 if a line is selected, 1 if no lines were selected
# -- if at least one line does not have ==0 => grep rc 0 => return rc 1
rc=$([[ ! $(grep -v "==0" $test_logrc) ]])
exit $rc
