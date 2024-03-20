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

#include <algorithm>
#include <fstream>
#include <string>

#include "client/client.h"
#include "arrow/io/api.h"
#include "arrow/filesystem/api.h"
#include "parquet/arrow/writer.h"

#include "common/util/functions.h"
#include "common/util/uuid.h"
#include "graph/loader/arrow_fragment_loader.h"
#include "graph/loader/fragment_loader_utils.h"

using namespace vineyard;  // NOLINT(build/namespaces)

using GraphType = ArrowFragment<property_graph_types::OID_TYPE,
                                property_graph_types::VID_TYPE>;
// using GraphType = ArrowFragment<std::string, property_graph_types::VID_TYPE>;
using LabelType = typename GraphType::label_id_t;

void WriteNullTest(std::shared_ptr<arrow::Table> table) {
    std::vector<std::shared_ptr<arrow::Array>> columns;
    for (int i = 0; i < table->num_columns(); ++i) {
      auto type = table->field(i)->type();
      auto ret = arrow::MakeArrayOfNull(type, 256);
      if (!ret.ok()) {
        LOG(FATAL) << "Failed to create null array: " << ret.status().message();
      }
      auto nulls = ret.ValueOrDie();
      columns.push_back(nulls);
    }
    std::shared_ptr<arrow::RecordBatch> record_batch =
        arrow::RecordBatch::Make(table->schema(), 256, columns);
    std::vector<std::shared_ptr<arrow::Table>> tables = {table, arrow::Table::FromRecordBatches({record_batch}).ValueOrDie()};
    auto st4 = tables[1]->Validate();
    if (!st4.ok()) {
      LOG(FATAL) << "Failed to validate table: " << st4.message();
    }
    arrow::Result<std::shared_ptr<arrow::Table>> concatenated_table_result = arrow::ConcatenateTables(tables);
    auto new_table = concatenated_table_result.ValueOrDie();
    std::string path = "/tmp/test_comment";
    std::shared_ptr<arrow::fs::FileSystem> fs;
    auto ret =  arrow::fs::FileSystemFromUriOrPath(path);
    if (ret.ok()) {
      fs = ret.ValueOrDie();
    } else {
      LOG(FATAL) << "Failed to get file system: " << ret.status().message();
    }
    std::shared_ptr<arrow::io::OutputStream> output_stream;
    auto ret2 = fs->OpenOutputStream(path);
    if (ret2.ok()) {
      output_stream = ret2.ValueOrDie();
    } else {
      LOG(FATAL) << "Failed to open output stream: " << ret2.status().message();
    }
    parquet::WriterProperties::Builder builder;
    builder.compression(arrow::Compression::type::ZSTD);  // enable compression
    LOG(INFO) << "Writing table to parquet file: " << path << " table size: " << new_table->num_rows() << " schema: " << new_table->schema()->ToString() << " colum 1 chunk number: " << new_table->column(1)->num_chunks();  
    auto st3 = new_table->Validate();
    if (!st3.ok()) {
      LOG(FATAL) << "Failed to validate table: " << st3.message();
    }

    auto st = parquet::arrow::WriteTable(
      *new_table, arrow::default_memory_pool(), output_stream, 64 * 1024 * 1024,
      builder.build(), parquet::default_arrow_writer_properties());
    if (!st.ok()) {
      LOG(FATAL) << "Failed to write table to parquet file: " << st.message();
    }
}

int main(int argc, char** argv) {
  if (argc < 2) {
    printf(
        "usage: ./arrow_fragment_label_data_extend <ipc_socket> [vdata_path] "
        "[edata_path]\n");
    return 1;
  }
  int index = 1;
  std::string ipc_socket = std::string(argv[index++]);
  std::string v_file_path = vineyard::ExpandEnvironmentVariables(argv[index++]);
  std::string e_file_path = vineyard::ExpandEnvironmentVariables(argv[index++]);

  std::string v_file_suffix = ".csv#header_row=true&label=comment#delimiter=|";
  std::string e_file_suffix =
      ".csv#header_row=true&label=rePly&src_label=comment&dst_label=comment#delimiter=|";

  vineyard::Client client;
  VINEYARD_CHECK_OK(client.Connect(ipc_socket));

  LOG(INFO) << "Connected to IPCServer: " << ipc_socket;

  using loader_t =
      ArrowFragmentLoader<property_graph_types::OID_TYPE, property_graph_types::VID_TYPE>;

  grape::InitMPIComm();
  {
    grape::CommSpec comm_spec;
    comm_spec.Init(MPI_COMM_WORLD);
    vineyard::ObjectID true_frag_group;

    // first construct a basic graph
    {
      MPI_Barrier(comm_spec.comm());
      std::string vfile = v_file_path + v_file_suffix;
      std::string efile = e_file_path + e_file_suffix;
      auto loader = std::make_unique<loader_t>(
          client, comm_spec, std::vector<std::string>{efile},
          std::vector<std::string>{vfile}, /* directed */ 1,
          /*generate_eid*/ false);
      true_frag_group = loader->LoadFragmentAsFragmentGroup().value();
    }
    {
      auto fg = std::dynamic_pointer_cast<ArrowFragmentGroup>(
          client.GetObject(true_frag_group));
      auto fid = comm_spec.WorkerToFrag(comm_spec.worker_id());
      auto frag_id = fg->Fragments().at(fid);
      auto arrow_frag =
          std::static_pointer_cast<GraphType>(client.GetObject(frag_id));
      WriteNullTest(arrow_frag->vertex_data_table(0));
    }
  }
  grape::FinalizeMPIComm();

  return 0;
}
