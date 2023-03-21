/** Copyright 2020-2023 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#include <stdio.h>

#include <fstream>
#include <string>

#include "client/client.h"
#include "common/util/logging.h"

#include "graph/fragment/arrow_flattened_fragment.grin.h"
#include "graph/fragment/graph_schema.h"
#include "graph/loader/arrow_fragment_loader.h"

using namespace vineyard;  // NOLINT(build/namespaces)

using GraphType = ArrowFragment<property_graph_types::OID_TYPE,
                                property_graph_types::VID_TYPE>;
using LabelType = typename GraphType::label_id_t;

void Traverse(vineyard::Client& client, const grape::CommSpec& comm_spec,
              vineyard::ObjectID fragment_group_id) {
  LOG(INFO) << "Loaded graph to vineyard: " << fragment_group_id;

  GRIN_PARTITIONED_GRAPH pg = get_partitioned_graph_by_object_id(client, fragment_group_id);

  GRIN_PARTITION_LIST local_partitions = grin_get_local_partition_list(pg);
  vineyard::ArrowFlattenedFragment<property_graph_types::OID_TYPE, property_graph_types::VID_TYPE, int64_t, int64_t>
    gaf(pg, local_partitions, "value1", "value1");
  auto iv = gaf.InnerVertices();
  for (auto v : iv) {
    auto al = gaf.GetOutgoingAdjList(v);
    property_graph_types::OID_TYPE src, dst;
    CHECK(gaf.GetId(v, src));
    for (auto it : al) {
      auto neighbor = it.get_neighbor();
      // auto edge = it.get_edge();
      CHECK(gaf.GetId(neighbor, dst));
      std::cout << src << " " << dst << "\n";
    }
  }
}


namespace detail {

std::shared_ptr<arrow::ChunkedArray> makeInt64Array() {
  std::vector<int64_t> data = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  arrow::Int64Builder builder;
  CHECK_ARROW_ERROR(builder.AppendValues(data));
  std::shared_ptr<arrow::Array> out;
  CHECK_ARROW_ERROR(builder.Finish(&out));
  return arrow::ChunkedArray::Make({out}).ValueOrDie();
}

std::shared_ptr<arrow::Schema> attachMetadata(
    std::shared_ptr<arrow::Schema> schema, std::string const& key,
    std::string const& value) {
  std::shared_ptr<arrow::KeyValueMetadata> metadata;
  if (schema->HasMetadata()) {
    metadata = schema->metadata()->Copy();
  } else {
    metadata = std::make_shared<arrow::KeyValueMetadata>();
  }
  metadata->Append(key, value);
  return schema->WithMetadata(metadata);
}

std::vector<std::shared_ptr<arrow::Table>> makeVTables() {
  auto schema = std::make_shared<arrow::Schema>(
      std::vector<std::shared_ptr<arrow::Field>>{
          arrow::field("id", arrow::int64()),
          arrow::field("value1", arrow::int64()),
          arrow::field("value2", arrow::int64()),
          arrow::field("value3", arrow::int64()),
          arrow::field("value4", arrow::int64()),
      });
  schema = attachMetadata(schema, "label", "person");
  auto table = arrow::Table::Make(
      schema, {makeInt64Array(), makeInt64Array(), makeInt64Array(),
               makeInt64Array(), makeInt64Array()});
  return {table};
}

std::vector<std::vector<std::shared_ptr<arrow::Table>>> makeETables() {
  auto schema = std::make_shared<arrow::Schema>(
      std::vector<std::shared_ptr<arrow::Field>>{
          arrow::field("src_id", arrow::int64()),
          arrow::field("dst_id", arrow::int64()),
          arrow::field("value1", arrow::int64()),
          arrow::field("value2", arrow::int64()),
          arrow::field("value3", arrow::int64()),
          arrow::field("value4", arrow::int64()),
      });
  schema = attachMetadata(schema, "label", "knows");
  schema = attachMetadata(schema, "src_label", "person");
  schema = attachMetadata(schema, "dst_label", "person");
  auto table = arrow::Table::Make(
      schema, {makeInt64Array(), makeInt64Array(), makeInt64Array(),
               makeInt64Array(), makeInt64Array(), makeInt64Array()});
  return {{table}};
}
}  // namespace detail

int main(int argc, char** argv) {
  if (argc < 2) {
    printf("usage: ./arrow_fragment_test <ipc_socket> [directed]\n");
    return 1;
  }
  int index = 1;
  std::string ipc_socket = std::string(argv[index++]);

  int directed = 1;
  if (argc > index) {
    directed = atoi(argv[index]);
  }

  auto vtables = ::detail::makeVTables();
  auto etables = ::detail::makeETables();

  vineyard::Client client;
  VINEYARD_CHECK_OK(client.Connect(ipc_socket));

  LOG(INFO) << "Connected to IPCServer: " << ipc_socket;

  grape::InitMPIComm();

  {
    grape::CommSpec comm_spec;
    comm_spec.Init(MPI_COMM_WORLD);

    {
      auto loader =
          std::make_unique<ArrowFragmentLoader<property_graph_types::OID_TYPE,
                                               property_graph_types::VID_TYPE>>(
              client, comm_spec, vtables, etables, directed != 0, true, true);
      vineyard::ObjectID fragment_group_id =
          loader->LoadFragmentAsFragmentGroup().value();
      Traverse(client, comm_spec, fragment_group_id);
    }
  }
  grape::FinalizeMPIComm();

  LOG(INFO) << "Passed arrow fragment test...";

  return 0;
}
