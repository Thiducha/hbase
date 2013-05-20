#!/bin/bash
##
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# Build the wrapper and set the owner as root, as well as the sticky bit.

gcc it_blockmachine_wrapper.c -o it_blockmachine_wrapper
gcc it_unblockmachine_wrapper.c -o it_unblockmachine_wrapper

sudo chown root it_blockmachine_wrapper
sudo chown root it_unblockmachine_wrapper

sudo chmod oug+r-w+s+x it_blockmachine_wrapper
sudo chmod oug+r-w+s+x it_unblockmachine_wrapper
