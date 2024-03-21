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

#ifdef ENABLE_GAR

#include "graph/writer/arrow_fragment_writer.h"

#include <memory>
#include <set>
#include <vector>

#include "arrow/api.h"
#include "arrow/compute/api.h"

#include "graph/utils/error.h"

namespace vineyard {

void FinishArrowArrayBuilders(
    std::vector<std::shared_ptr<arrow::ArrayBuilder>>& builders,
    std::vector<std::shared_ptr<arrow::Array>>& columns) {
  for (size_t i = 0; i < builders.size(); i++) {
    ARROW_CHECK_OK(builders[i]->Finish(&columns[i]));
  }
}

void InitializeArrayArrayBuilders(
    std::vector<std::shared_ptr<arrow::ArrayBuilder>>& builders,
    const std::set<property_graph_types::LABEL_ID_TYPE>& property_ids,
    const property_graph_types::LABEL_ID_TYPE edge_label,
    const PropertyGraphSchema& graph_schema) {
  builders.resize(property_ids.size() + 2);
  builders[0] = std::make_shared<arrow::Int64Builder>();  // vertex index column
  builders[1] = std::make_shared<arrow::Int64Builder>();  // vertex index column
  int col_id = 2;
  for (auto& pid : property_ids) {
    auto prop_type = graph_schema.GetEdgePropertyType(edge_label, pid);
    if (arrow::boolean()->Equals(prop_type)) {
      builders[col_id] = std::make_shared<arrow::BooleanBuilder>();
    } else if (arrow::int32()->Equals(prop_type)) {
      builders[col_id] = std::make_shared<arrow::Int32Builder>();
    } else if (arrow::int64()->Equals(prop_type)) {
      builders[col_id] = std::make_shared<arrow::Int64Builder>();
    } else if (arrow::float32()->Equals(prop_type)) {
      builders[col_id] = std::make_shared<arrow::FloatBuilder>();
    } else if (arrow::float64()->Equals(prop_type)) {
      builders[col_id] = std::make_shared<arrow::DoubleBuilder>();
    } else if (arrow::utf8()->Equals(prop_type)) {
      builders[col_id] = std::make_shared<arrow::StringBuilder>();
    } else if (arrow::large_utf8()->Equals(prop_type)) {
      builders[col_id] = std::make_shared<arrow::LargeStringBuilder>();
    } else if (arrow::date32()->Equals(prop_type)) {
      builders[col_id] = std::make_shared<arrow::Date32Builder>();
    } else if (arrow::date64()->Equals(prop_type)) {
      builders[col_id] = std::make_shared<arrow::Date64Builder>();
    } else if (prop_type->id() == arrow::Type::TIME32) {
      builders[col_id] = std::make_shared<arrow::Time32Builder>(
          prop_type, arrow::default_memory_pool());
    } else if (prop_type->id() == arrow::Type::TIME64) {
      builders[col_id] = std::make_shared<arrow::Time64Builder>(
          prop_type, arrow::default_memory_pool());
    } else if (prop_type->id() == arrow::Type::TIMESTAMP) {
      builders[col_id] = std::make_shared<arrow::TimestampBuilder>(
          prop_type, arrow::default_memory_pool());
    } else {
      LOG(FATAL) << "Unsupported property type: " << prop_type->ToString();
    }
    ++col_id;
  }
}

boost::leaf::result<std::shared_ptr<arrow::Table>> AppendNullsToArrowTable(
    const std::shared_ptr<arrow::Table>& table, int64_t num_rows_to_append) {
  std::vector<std::shared_ptr<arrow::Array>> columns;
  for (int i = 0; i < table->num_columns(); ++i) {
    auto type = table->field(i)->type();
    std::shared_ptr<arrow::Array> null_array;
    ARROW_OK_ASSIGN_OR_RAISE(null_array, arrow::MakeArrayOfNull(type, num_rows_to_append));
    columns.push_back(null_array);
  }

  auto null_table = arrow::Table::Make(table->schema(), columns);
  std::shared_ptr<arrow::Table> concatenated_table;
  ARROW_OK_ASSIGN_OR_RAISE(concatenated_table, arrow::ConcatenateTables({table, null_table}));

  return concatenated_table;
}

}  // namespace vineyard

#endif  // ENABLE_GAR
