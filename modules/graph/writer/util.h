/** Copyright 2020-2023 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef MODULES_GRAPH_WRITER_UTILS_H_
#define MODULES_GRAPH_WRITER_UTILS_H_

#include <string>
#include <memory>

#include "graph/fragment/graph_schema.h"
#include "gar/graph_info.h"
#include "gar/util/file_type.h"

namespace GAR = GraphArchive;

namespace vineyard {

std::shared_ptr<GraphArchive::GraphInfo> generate_graph_info_with_schema(
    const PropertyGraphSchema& schema, const std::string& graph_name,
    const std::string& path, 
    int64_t vertex_block_size, int64_t edge_block_size, 
    GAR::FileType file_type);

} // namespace vineyard

#endif  // MODULES_GRAPH_WRITER_UTILS_H_