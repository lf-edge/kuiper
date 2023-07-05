#!/bin/bash
#
# Copyright 2021-2023 EMQ Technologies Co., Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e

test/start_kuiper.sh

chmod +x test/build_edgex_mock.sh
chmod +x test/can_send.sh
test/build_edgex_mock.sh

# Check if the can0 interface is already up
if ifconfig can0 | grep -q "UP"; then
    echo "can0 interface is already enabled."
    exit 0
fi

## Load the SocketCAN module
#modprobe vcan

# Create the virtual can0 interface
ip link add dev can0 type vcan

# Bring up the can0 interface
ifconfig can0 up

echo "can0 interface enabled."