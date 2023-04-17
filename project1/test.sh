#!/bin/bash

cp testcases/testcase4/input-0.dat INPUT
#cat testcases/testcase3/input-1.dat >> INPUT
#cat testcases/testcase3/input-2.dat >> INPUT
#cat testcases/testcase3/input-3.dat >> INPUT
#cat testcases/testcase3/input-4.dat >> INPUT
#cat testcases/testcase3/input-5.dat >> INPUT
#cat testcases/testcase3/input-6.dat >> INPUT
#cat testcases/testcase3/input-7.dat >> INPUT
#cat testcases/testcase3/input-8.dat >> INPUT
#cat testcases/testcase3/input-9.dat >> INPUT
#cat testcases/testcase3/input-10.dat >> INPUT
#cat testcases/testcase3/input-11.dat >> INPUT
#cat testcases/testcase3/input-12.dat >> INPUT
#cat testcases/testcase3/input-13.dat >> INPUT
#cat testcases/testcase3/input-14.dat >> INPUT
#cat testcases/testcase3/input-15.dat >> INPUT


cp testcases/testcase4/output-0.dat OUTPUT
#cat testcases/testcase3/output-1.dat >> OUTPUT
#cat testcases/testcase3/output-2.dat >> OUTPUT
#cat testcases/testcase3/output-3.dat >> OUTPUT  
#cat testcases/testcase3/output-4.dat >> OUTPUT
#cat testcases/testcase3/output-5.dat >> OUTPUT
#cat testcases/testcase3/output-6.dat >> OUTPUT
#cat testcases/testcase3/output-7.dat >> OUTPUT
#cat testcases/testcase3/output-8.dat >> OUTPUT
#cat testcases/testcase3/output-9.dat >> OUTPUT
#cat testcases/testcase3/output-10.dat >> OUTPUT  
#cat testcases/testcase3/output-11.dat >> OUTPUT
#cat testcases/testcase3/output-12.dat >> OUTPUT
#cat testcases/testcase3/output-13.dat >> OUTPUT
#cat testcases/testcase3/output-14.dat >> OUTPUT
#cat testcases/testcase3/output-15.dat >> OUTPUT

bin/sort INPUT REF_OUTPUT
diff REF_OUTPUT OUTPUT > testcases/testcase3/difference.txt
