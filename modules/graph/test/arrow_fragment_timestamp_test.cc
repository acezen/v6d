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

#include "graph/fragment/arrow_fragment.h"
#include "graph/fragment/graph_schema.h"
#include "graph/loader/arrow_fragment_loader.h"

#include "graph/tools/graph_loader.h"

using namespace vineyard;  // NOLINT(build/namespaces)

using GraphType = ArrowFragment<property_graph_types::OID_TYPE,
                                property_graph_types::VID_TYPE>;
using LabelType = typename GraphType::label_id_t;

namespace detail {

std::shared_ptr<arrow::ChunkedArray> makeInt64Array() {
  std::vector<int64_t> data = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  arrow::Int64Builder builder;
  CHECK_ARROW_ERROR(builder.AppendValues(data));
  std::shared_ptr<arrow::Array> out;
  CHECK_ARROW_ERROR(builder.Finish(&out));
  return arrow::ChunkedArray::Make({out}).ValueOrDie();
}

std::shared_ptr<arrow::ChunkedArray> makeTimestampArray() {
  std::vector<arrow::TimestampType::c_type> data = {1711355740, 1711355741, 1711355742, 1711355743, 1711355744, 1711355745, 1711355746, 1711355747, 1711355748, 1711355749};
  arrow::TimestampBuilder builder(arrow::timestamp(arrow::TimeUnit::MILLI, "UTC"), arrow::default_memory_pool());
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
          arrow::field("value5", arrow::timestamp(arrow::TimeUnit::MILLI, "UTC")),
      });
  schema = attachMetadata(schema, "label", "knows");
  schema = attachMetadata(schema, "src_label", "person");
  schema = attachMetadata(schema, "dst_label", "person");
  auto table = arrow::Table::Make(
      schema, {makeInt64Array(), makeInt64Array(), makeInt64Array(),
               makeInt64Array(), makeInt64Array(), makeInt64Array(), makeTimestampArray()});
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
              client, comm_spec, vtables, etables, directed != 0);
      vineyard::ObjectID fragment_group_id =
          loader->LoadFragmentAsFragmentGroup().value();
      auto fg = std::dynamic_pointer_cast<ArrowFragmentGroup>(
          client.GetObject(fragment_group_id));
      auto fid = comm_spec.WorkerToFrag(comm_spec.worker_id());
      auto frag_id = fg->Fragments().at(fid);
      auto arrow_frag =
          std::static_pointer_cast<GraphType>(client.GetObject(frag_id));
      // WriteOut(client, comm_spec, fragment_group_id);
      LOG(INFO) << "Loaded fragment, schema: " << arrow_frag->schema().ToJSONString();
      auto vertices = arrow_frag->InnerVertices(0);
      // get the timestamp value of each edge
      for (auto v : vertices) {
        for (auto& e : arrow_frag->GetOutgoingAdjList(v, 0)) {
          LOG(INFO) << "  edge " << arrow_frag->GetId(v) << " " << arrow_frag->GetId(e.neighbor())
                    << " timestamp " << e.get_data<arrow::TimestampType::c_type>(4);
        }
      }
    }
  }
  grape::FinalizeMPIComm();

  LOG(INFO) << "Passed arrow fragment test...";

  return 0;
}
