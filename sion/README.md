<!--
Copyright (c) 2014 - 2015 YCSB contributors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You
may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See accompanying
LICENSE file.
-->

## Quick Start

This section describes how to run YCSB on Sion. 

### 1. Start Sion

### 2. Install Java and Maven

### 3. Set Up YCSB

Git clone YCSB and compile:

    git clone http://github.com/zhangjyr/YCSB.git
    cd YCSB
    mvn -pl site.ycsb:sion-binding -am clean package

### 4. Provide Sion Connection Parameters
    
Set host, port, password, and cluster mode in the workload you plan to run. 

- `sion.host`
- `sion.port`

Or, you can set configs with the shell command, EG:

    ./bin/ycsb load sion -s -P workloads/workloada -p "sion.host=127.0.0.1" -p "sion.port=6378" > outputLoad.txt

### 5. Load data and run tests

Load the data:

    ./bin/ycsb load sion -s -P workloads/workloada > outputLoad.txt

Run the workload test:

    ./bin/ycsb run sion -s -P workloads/workloada > outputRun.txt

