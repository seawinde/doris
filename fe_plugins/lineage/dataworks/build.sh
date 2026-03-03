#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -eo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

export DORIS_HOME="${ROOT}/../../.."

. "${DORIS_HOME}/env.sh"

cd "${ROOT}"
"${MVN_CMD}" clean package -DskipTests

echo "Install dataworks lineage..."

LINEAGE_OUTPUT="${ROOT}/output"
rm -rf "${LINEAGE_OUTPUT}"
mkdir -p "${LINEAGE_OUTPUT}"
cp "${ROOT}/target/dataworks-lineage.zip" "${LINEAGE_OUTPUT}"/

echo "Build dataworks lineage Finished"
