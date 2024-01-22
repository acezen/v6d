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

#ifdef ENABLE_GAR

#include "graph/fragment/arrow_fragment.h"
#include "graph/fragment/graph_schema.h"
#include "graph/loader/arrow_fragment_loader.h"
#include "graph/writer/arrow_fragment_writer.h"

using namespace vineyard;  // NOLINT(build/namespaces)
namespace bl = boost::leaf;

using GraphType = ArrowFragment<property_graph_types::OID_TYPE,
                                property_graph_types::VID_TYPE>;
using LabelType = typename GraphType::label_id_t;

using oid_t = vineyard::property_graph_types::OID_TYPE;
using vid_t = vineyard::property_graph_types::VID_TYPE;

void traverse_graph(std::shared_ptr<GraphType> graph, const std::string& path) {
  LabelType e_label_num = graph->edge_label_num();
  LabelType v_label_num = graph->vertex_label_num();

  for (LabelType v_label = 0; v_label != v_label_num; ++v_label) {
    std::ofstream fout(path + "_v_" + std::to_string(v_label),
                       std::ios::binary);
    auto iv = graph->InnerVertices(v_label);
    for (auto v : iv) {
      auto id = graph->GetId(v);
      fout << id << std::endl;
    }
    fout.flush();
    fout.close();
  }
  for (LabelType e_label = 0; e_label != e_label_num; ++e_label) {
    std::ofstream fout(path + "_e_" + std::to_string(e_label),
                       std::ios::binary);
    for (LabelType v_label = 0; v_label != v_label_num; ++v_label) {
      auto iv = graph->InnerVertices(v_label);
      for (auto v : iv) {
        auto src_id = graph->GetId(v);
        auto oe = graph->GetOutgoingAdjList(v, e_label);
        for (auto& e : oe) {
          fout << src_id << " " << graph->GetId(e.neighbor()) << "\n";
        }
      }
    }
    fout.flush();
    fout.close();
  }
}

boost::leaf::result<int> write_out_to_gar(const grape::CommSpec& comm_spec,
                                          std::shared_ptr<GraphType> graph,
                                          const std::string& graph_yaml_path) {
  auto writer = std::make_unique<ArrowFragmentWriter<GraphType>>(
      graph, comm_spec, graph_yaml_path);
  BOOST_LEAF_CHECK(writer->WriteFragment());
  LOG(INFO) << "[worker-" << comm_spec.worker_id() << "] generate GAR files...";
  return 0;
}

void LoadGraphFromCsv(int argc, char** argv) {
  LOG(INFO) << "Loading Graph From Csv.";
  if (argc < 6) {
    printf(
        "usage: ./run_grin_app <cmd_type> <ipc_socket> <e_label_num> <efiles...> "
        "<v_label_num> <vfiles...> [directed]\n");
    return;
  }
  int index = 1;
  std::string ipc_socket = std::string(argv[index++]);

  long int id = 0; 
  id = atol(argv[index++]);
  int edge_label_num = atoi(argv[index++]);
  std::vector<std::string> efiles;
  LOG(INFO) << "edge_label_num: " << edge_label_num;
  for (int i = 0; i < edge_label_num; ++i) {
    efiles.push_back(argv[index++]);
    LOG(INFO) << "efile: " << efiles.back();
  }

  int vertex_label_num = atoi(argv[index++]);
  LOG(INFO) << "vertex_label_num: " << vertex_label_num;
  std::vector<std::string> vfiles;
  for (int i = 0; i < vertex_label_num; ++i) {
    vfiles.push_back(argv[index++]);
    LOG(INFO) << "vfile: " << vfiles.back();
  }

  int directed = 1;
  if (argc > index) {
    directed = atoi(argv[index++]);
  }

  {
    grape::CommSpec comm_spec;
    comm_spec.Init(MPI_COMM_WORLD);

    vineyard::Client client;
    VINEYARD_CHECK_OK(client.Connect(ipc_socket));

    LOG(INFO) << "Connected to IPCServer: " << ipc_socket;
    vineyard::ObjectID fragment_id = vineyard::InvalidObjectID();
    if (id != 0) {
      fragment_id = id;
    } else {

    {
      if (!vfiles.empty()) {
        std::cout << "Loading vertex files..." << std::endl;
      auto loader = std::make_unique<vineyard::ArrowFragmentLoader<oid_t, vid_t>>(
          client, comm_spec, efiles, vfiles, directed != 0,
          /* generate_eid */ false, /* retain_oid */ false);
      fragment_id =
          bl::try_handle_all([&loader]() { return loader->LoadFragmentAsFragmentGroup(); },
                             [](const vineyard::GSError& e) {
                               LOG(FATAL) << e.error_msg;
                               return 0;
                             },
                             [](const bl::error_info& unmatched) {
                               LOG(FATAL) << "Unmatched error " << unmatched;
                               return 0;
                             });
      } else {
      auto loader = std::make_unique<vineyard::ArrowFragmentLoader<oid_t, vid_t>>(
          client, comm_spec, efiles, directed != 0,
          /* generate_eid */ false, /* retain_oid */ true);
      fragment_id =
          bl::try_handle_all([&loader]() { return loader->LoadFragmentAsFragmentGroup(); },
                             [](const vineyard::GSError& e) {
                               LOG(FATAL) << e.error_msg;
                               return 0;
                             },
                             [](const bl::error_info& unmatched) {
                               LOG(FATAL) << "Unmatched error " << unmatched;
                               return 0;
                             });
      }
    }
    }

    LOG(INFO) << "[worker-" << comm_spec.worker_id()
              << "] loaded graph to vineyard ... " << fragment_id;

    MPI_Barrier(comm_spec.comm());
    auto fg = std::dynamic_pointer_cast<vineyard::ArrowFragmentGroup>(
      client.GetObject(fragment_id));
    auto fid = comm_spec.WorkerToFrag(comm_spec.worker_id());
    auto frag_id = fg->Fragments().at(fid);
    auto arrow_frag = std::static_pointer_cast<GraphType>(client.GetObject(frag_id));
    /*
    LOG(INFO) << "Consolidating ...";
    std::vector<std::string> consolidate_columns;
    for (int i = 0; i < 3; ++i) {
      consolidate_columns.emplace_back("prop_" + std::to_string(i));
    }
    vineyard::ObjectID new_fragment_id = vineyard::InvalidObjectID();
    {
    new_fragment_id =
          bl::try_handle_all([&client, &arrow_frag, &consolidate_columns]() { return arrow_frag->ConsolidateVertexColumns(client, 0, consolidate_columns, "vprops_column"); },
                             [](const vineyard::GSError& e) {
                               LOG(FATAL) << e.error_msg;
                               return 0;
                             },
                             [](const bl::error_info& unmatched) {
                               LOG(FATAL) << "Unmatched error " << unmatched;
                               return 0;
                             });
    }
    LOG(INFO) << "[worker-" << comm_spec.worker_id()
              << "] consolidate graph to vineyard ... " << new_fragment_id;
    // auto fg2 = std::dynamic_pointer_cast<vineyard::ArrowFragmentGroup>(
    //   client.GetObject(new_fragment_id));
    // auto new_frag_id = fg2->Fragments().at(fid);
    arrow_frag = std::static_pointer_cast<FragType>(client.GetObject(new_fragment_id));
    */
    LOG(INFO) << "Writing graph to graphar ...";
    std::string graph_info = std::string(argv[index++]);
    auto writer = std::make_unique<vineyard::ArrowFragmentWriter<GraphType>>(
        arrow_frag, comm_spec, graph_info);
    // writer->WriteFragment();
    bl::try_handle_all([&writer]() { return writer->WriteFragment(); },
                             [](const vineyard::GSError& e) {
                               LOG(FATAL) << e.error_msg;
                               return;
                             },
                             [](const bl::error_info& unmatched) {
                               LOG(FATAL) << "Unmatched error " << unmatched;
                               return;
                             });
    MPI_Barrier(comm_spec.comm());
    LOG(INFO) << "Writing graph to graphar done.";
  }
}

int main(int argc, char** argv) {
  if (argc < 3) {
    printf(
        "usage: ./arrow_fragment_test <ipc_socket> <graph_yaml_path>"
        "[directed]\n");
    return 1;
  }
  grape::InitMPIComm();
  LoadGraphFromCsv(argc, argv);
  grape::FinalizeMPIComm();

  LOG(INFO) << "Passed arrow fragment gar test...";

  return 0;
}

#else

int main(int argc, char** argv) {
  LOG(INFO) << "Arrow fragment gar test is disabled...";
  return 0;
}

#endif  // ENABLE_GAR
