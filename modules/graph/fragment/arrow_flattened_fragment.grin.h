/** Copyright 2020 Alibaba Group Holding Limited.
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

#ifndef ANALYTICAL_ENGINE_CORE_FRAGMENT_ARROW_FLATTENED_FRAGMENT_GRIN_H_
#define ANALYTICAL_ENGINE_CORE_FRAGMENT_ARROW_FLATTENED_FRAGMENT_GRIN_H_

#include <cstddef>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "graph/grin/include/topology/structure.h"
#include "graph/grin/include/topology/vertexlist.h"
#include "graph/grin/include/topology/adjacentlist.h"

#include "graph/grin/include/partition/partition.h"
#include "graph/grin/include/partition/topology.h"
#include "graph/grin/include/property/topology.h"
#include "graph/grin/include/partition/reference.h"

#include "graph/grin/include/property/type.h"
#include "graph/grin/include/property/property.h"
#include "graph/grin/include/property/propertylist.h"
#include "graph/grin/include/property/propertytable.h"
#include "graph/grin/include/property/propertytable.h"
#include "graph/grin/include/property/topology.h"

#include "graph/grin/src/predefine.h"

namespace grape {
class CommSpec;
}

namespace vineyard {

namespace arrow_flattened_fragment_impl {

struct Vertex {
  Vertex(GRIN_GRAPH g, GRIN_VERTEX v): g_(g), grin_v(v) {}
  ~Vertex() {
    grin_destroy_vertex(g_, grin_v);
  }

  GRIN_GRAPH g_;
  GRIN_VERTEX grin_v;
};

struct Nbr {
 public:
  Nbr() : g_{nullptr}, al_(nullptr), cur_(0) {}
  Nbr(GRIN_GRAPH g, GRIN_ADJACENT_LIST al, size_t cur, const std::vector<GRIN_EDGE_PROPERTY_TABLE>& epts,
      const std::vector<size_t>& agg_nums)
    : g_{g}, al_(al), cur_(cur), epts_(epts), agg_nums_(agg_nums) {}
  Nbr(Nbr& rhs) : g_{rhs.g_}, al_(rhs.al_), cur_(rhs.cur_), epts_(rhs.epts_), agg_nums_(rhs.agg_nums_) {}

  Nbr& operator=(const Nbr& rhs) {
    g_ = rhs.g_;
    al_ = rhs.al_;
    cur_ = rhs.cur_;
    epts_ = rhs.epts_;
    return *this;
  }

  Nbr& operator=(Nbr&& rhs) {
    g_ = rhs.g_;
    al_ = rhs.al_;
    cur_ = rhs.cur_;
    epts_ = rhs.epts_;
    return *this;
  }

  Vertex neighbor() {
    return Vertex(g_, grin_get_neighbor_from_adjacent_list(g_, al_, cur_));
  }

  Vertex get_neighbor() {
    return Vertex(g_, grin_get_neighbor_from_adjacent_list(g_, al_, cur_));
  }

  GRIN_EDGE get_edge() {
    return grin_get_edge_from_adjacent_list(g_, al_, cur_);
  }

  /*
  template <typename T>
  T get_data(GRIN_EDGE_PROPERTY prop) const {
    auto _e = grin_get_edge_from_adjacent_list(g_, al_, cur_);
    auto value = grin_get_value_from_edge_property_table(g_, ept_, _e, prop);
    return property_graph_utils::ValueGetter<T>::Value(value, 0);
  }

  std::string get_str(GRIN_EDGE_PROPERTY prop) const {
    auto _e = grin_get_edge_from_adjacent_list(g_, al_, cur_);
    auto value = grin_get_value_from_edge_property_table(g_, ept_, _e, prop);
    return property_graph_utils::ValueGetter<std::string>::Value(value, 0);
  }

  double get_double(GRIN_EDGE_PROPERTY prop) const {
    auto _e = grin_get_edge_from_adjacent_list(g_, al_, cur_);
    auto value = grin_get_value_from_edge_property_table(g_, ept_, _e, prop);
    return property_graph_utils::ValueGetter<double>::Value(value, 0);
  }

  int64_t get_int(GRIN_EDGE_PROPERTY prop) const {
    auto _e = grin_get_edge_from_adjacent_list(g_, al_, cur_);
    auto value = grin_get_value_from_edge_property_table(g_, ept_, _e, prop);
    return property_graph_utils::ValueGetter<int64_t>::Value(value, 0);
  }
  */

  inline Nbr& operator++() {
    cur_++;
    return *this;
  }

  inline Nbr operator++(int) {
    cur_++;
    return *this;
  }

  inline Nbr& operator--() {
    cur_--;
    return *this;
  }

  inline Nbr operator--(int) {
    cur_--;
    return *this;
  }

  inline bool operator==(const Nbr& rhs) const {
    return al_ == rhs.al_ && cur_ == rhs.cur_;
  }
  inline bool operator!=(const Nbr& rhs) const {
    return al_ != rhs.al_ || cur_ != rhs.cur_;
  }

  inline bool operator<(const Nbr& rhs) const {
    return al_ == rhs.al_ && cur_ < rhs.cur_;
  }

  inline Nbr& operator*() { return *this; }

 private:
  GRIN_GRAPH g_;
  GRIN_ADJACENT_LIST al_;
  size_t cur_;
  std::vector<GRIN_EDGE_PROPERTY_TABLE> epts_;
  std::vector<size_t> agg_nums_;
};


class AdjList {
 public:
  AdjList(): g_(nullptr), adj_list_(nullptr), begin_(0), end_(0) {}
  AdjList(GRIN_GRAPH g, GRIN_ADJACENT_LIST adj_list, const std::vector<GRIN_EDGE_PROPERTY_TABLE>& epts,
          const std::vector<size_t>& agg_nums, size_t begin, size_t end)
    : g_{g}, adj_list_(adj_list), epts_(epts), agg_nums_(agg_nums), begin_(begin), end_(end) {}

  ~AdjList() {
    grin_destroy_adjacent_list(g_, adj_list_);
    for (auto& ept : epts_) {
      grin_destroy_edge_property_table(g_, ept);
    }
  }

  inline Nbr begin() const {
    return Nbr(g_, adj_list_, begin_, epts_, agg_nums_);
  }

  inline Nbr end() const {
    return Nbr(g_, adj_list_, end_, epts_, agg_nums_);
  }

  inline size_t Size() const { return end_ - begin_; }

  inline bool Empty() const { return begin_ == end_; }

  inline bool NotEmpty() const { return begin_ < end_; }

  size_t size() const { return end_ - begin_; }

 private:
  GRIN_GRAPH g_;
  GRIN_ADJACENT_LIST adj_list_;
  std::vector<GRIN_EDGE_PROPERTY_TABLE> epts_;
  std::vector<size_t> agg_nums_;
  size_t begin_;
  size_t end_;
};

class VertexRange {
 public:
  VertexRange() {}
  VertexRange(GRIN_GRAPH g, GRIN_VERTEX_LIST vl, const size_t begin, const size_t end)
      : g_(g), vl_(vl), begin_(begin), end_(end) {}
  VertexRange(const VertexRange& r) : g_(r.g_), vl_(r.vl_), begin_(r.begin_), end_(r.end_) {}

   ~VertexRange() {
    grin_destroy_vertex_list(g_, vl_);
  }

  class iterator {
    using reference_type = Vertex;

   private:
    GRIN_GRAPH g_;
    GRIN_VERTEX_LIST vl_;
    size_t cur_;

   public:
    iterator() noexcept : g_(nullptr), vl_(nullptr), cur_() {}
    explicit iterator(GRIN_GRAPH g, GRIN_VERTEX_LIST vl, size_t idx) noexcept : g_(g), vl_(vl), cur_(idx) {}

    reference_type operator*() noexcept { return Vertex(g_, grin_get_vertex_from_list(g_, vl_, cur_)); }

    iterator& operator++() noexcept {
      ++cur_;
      return *this;
    }

    iterator operator++(int) noexcept {
      return iterator(g_, vl_, cur_ + 1);
    }

    iterator& operator--() noexcept {
      --cur_;
      return *this;
    }

    iterator operator--(int) noexcept {
      return iterator(g_, vl_, cur_--);
    }

    iterator operator+(size_t offset) const noexcept {
      return iterator(g_, vl_, cur_ + offset);
    }

    bool operator==(const iterator& rhs) const noexcept {
      return cur_ == rhs.cur_;
    }

    bool operator!=(const iterator& rhs) const noexcept {
      return cur_ != rhs.cur_;
    }

    bool operator<(const iterator& rhs) const noexcept {
      return vl_ == rhs.vl_ && cur_ < rhs.cur_;
    }
  };

  iterator begin() const { return iterator(g_, vl_, begin_); }

  iterator end() const { return iterator(g_, vl_, end_); }

  size_t size() const { return end_ - begin_; }

  void Swap(VertexRange& rhs) {
    std::swap(begin_, rhs.begin_);
    std::swap(end_, rhs.end_);
  }

  void SetRange(const size_t begin, const size_t end) {
    begin_ = begin;
    end_ = end;
  }

  const size_t begin_value() const { return begin_; }

  const size_t end_value() const { return end_; }

 private:
  GRIN_GRAPH g_;
  GRIN_VERTEX_LIST vl_;
  size_t begin_;
  size_t end_;
};
}  // namespace arrow_flattened_fragment_impl

/**
 * @brief This class represents the fragment flattened from ArrowFragment.
 * Different from ArrowProjectedFragment, an ArrowFlattenedFragment derives from
 * an ArrowFragment, but flattens all the labels to one type, result in a graph
 * with a single type of vertices and a single type of edges. Optionally,
 * a common property across labels of vertices(reps., edges) in the
 * ArrowFragment will be reserved as vdata(resp, edata).
 * ArrowFlattenedFragment usually used as a wrapper for ArrowFragment to run the
 * applications/algorithms defined in NetworkX or Analytical engine,
 * since these algorithms need the topology of the whole (property) graph.
 *
 * @tparam OID_T
 * @tparam VID_T
 * @tparam VDATA_T
 * @tparam EDATA_T
 */
template <typename OID_T, typename VID_T, typename VDATA_T, typename EDATA_T>
class ArrowFlattenedFragment {
 public:
  using oid_t = OID_T;
  using vid_t = VID_T;
  using vdata_t = VDATA_T;
  using edata_t = EDATA_T;
  using vertex_t = arrow_flattened_fragment_impl::Vertex;
  using fid_t = GRIN_PARTITION_ID;
  using vertex_range_t = arrow_flattened_fragment_impl::VertexRange;
  using inner_vertices_t = vertex_range_t;
  using outer_vertices_t = vertex_range_t;
  using vertices_t = vertex_range_t;

  using adj_list_t =
      arrow_flattened_fragment_impl::AdjList;

  // This member is used by grape::check_load_strategy_compatible()
  static constexpr grape::LoadStrategy load_strategy =
      grape::LoadStrategy::kBothOutIn;

  ArrowFlattenedFragment() = default;

#if defined(GRIN_WITH_VERTEX_PROPERTY) && defined(GRIN_WITH_EDGE_PROPERTY)  // property graph storage
  explicit ArrowFlattenedFragment(GRIN_PARTITIONED_GRAPH partitioned_graph, GRIN_PARTITION partition,
      const std::string& v_prop_name, const std::string& e_prop_name)
        : pg_(partitioned_graph), partition_(partition), v_prop_(v_prop_name), e_prop_(e_prop_name) {
    pg_ = partitioned_graph;
    partition_ = partition;
    g_ = grin_get_local_graph_from_partition(pg_, partition_);
    ivnum_ = ovnum_ = 0;
    auto vl = grin_get_vertex_list(g_);
    tvnum_ = grin_get_vertex_list_size(g_, vl);
    auto vtl = grin_get_vertex_type_list(g_);
    auto tsize = grin_get_vertex_type_list_size(g_, vtl);
    for (size_t i = 0; i < tsize; ++i) {
      auto type = grin_get_vertex_type_from_list(g_, vtl, i);
      auto vl1 = grin_filter_type_for_vertex_list(g_, type, vl);
      auto ivl = grin_filter_master_for_vertex_list(g_, vl1);
      auto ovl = grin_filter_mirror_for_vertex_list(g_, vl1);
      ivnum_ += grin_get_vertex_list_size(g_, ivl);
      ovnum_ += grin_get_vertex_list_size(g_, ovl);
      grin_destroy_vertex_list(g_, vl1);
      grin_destroy_vertex_list(g_, ivl);
      grin_destroy_vertex_list(g_, ovl);
    }
    grin_destroy_vertex_list(g_, vl);
    grin_destroy_vertex_type_list(g_, vtl);
  }
#else  // simple graph storage
  explicit ArrowFlattenedFragment(GRIN_PARTITIONED_GRAPH partitioned_graph, GRIN_PARTITION partition)
    : pg_(partitioned_graph), partition_(partition) {
    g_ = grin_get_local_graph_from_partition(pg_, partition_);
    auto vl = grin_get_vertex_list(g_)
    tvnum_ = grin_get_vertex_list_size(g_, vl);
    auto vl1 = grin_filter_master_for_vertex_list(g_, vl);
    ivnum_ = grin_get_vertex_list_size(g_, vl1);
    auto vl2 = grin_filter_mirror_for_vertex_list(g_, )
    ovnum_ = grin_get_vertex_list_size(g_, vl2);
    grin_destroy_vertex_list(g_, vl);
    grin_destroy_vertex_list(g_, vl1);
    grin_destroy_vertex_list(g_, vl2);
  }
#endif

  virtual ~ArrowFlattenedFragment() = default;

  inline fid_t fid() const {
  #ifdef GRIN_TRAIT_NATURAL_ID_FOR_PARTITION
    return grin_get_partition_id(pg_, partition_);
  #else
    // FIXME: raise error here
    return 0;
  #endif
  }

  inline fid_t fnum() const {
  #ifdef GRIN_ENABLE_GRAPH_PARTITION
    return grin_get_total_partitions_number(pg_);
  #else
    return 1;
  #endif
  }

  inline bool directed() const { return grin_is_directed(g_); }

  inline vertex_range_t Vertices() const {
    auto vl = grin_get_vertex_list(g_);
    auto sz = grin_get_vertex_list_size(g_, vl);
    return vertex_range_t(g_, vl, 0, sz);
  }

  inline vertex_range_t InnerVertices() const {
    auto vl = grin_get_vertex_list(g_);
    auto ivl = grin_filter_master_for_vertex_list(g_, vl);
    auto sz = grin_get_vertex_list_size(g_, ivl);
    return vertex_range_t(g_, ivl, 0, sz);
  }

  inline vertex_range_t OuterVertices() const {
    auto vl = grin_get_vertex_list(g_);
    auto ovl = grin_filter_mirror_for_vertex_list(g_, vl);
    auto sz = grin_get_vertex_list_size(g_, ovl);
    return vertex_range_t(g_, ovl, 0, sz);
  }

  inline GRIN_VERTEX_TYPE vertex_label(const vertex_t& v) const {
    return grin_get_vertex_type(g_, v.grin_v);
  }

  bool GetVertex(oid_t& oid, vertex_t& v) {
#if defined(GRIN_WITH_VERTEX_ORIGINAL_ID) && !defined(GRIN_ASSUME_BY_TYPE_VERTEX_ORIGINAL_ID)
    if (GRIN_DATATYPE_ENUM<oid_t>::value != grin_get_vertex_original_id_type(g_)) return false;
    v.g_ = g_;
    v.grin_v = grin_get_vertex_from_original_id(g_, (GRIN_VERTEX_ORIGINAL_ID)(&oid));
    return v != NULL;
#else
    // get oid from iterating type oid, the first match if the oid
    auto vt = grin_get_vertex_type_list(g_);
    auto vts = grin_get_vertex_type_list_size(g_, vt);

    return false;
#endif
  }

  inline bool GetId(const vertex_t& v, oid_t& oid) const {
    if (GRIN_DATATYPE_ENUM<oid_t>::value != grin_get_vertex_original_id_type(g_)) return false;
    if (v.grin_v == GRIN_NULL_VERTEX) return false;
    auto _id = grin_get_vertex_original_id(g_, v.grin_v);
    if (_id == NULL) return false;
    auto _oid = static_cast<oid_t*>(_id);
    oid = *_oid;
    return true;
  }

#ifdef GRIN_NATURAL_PARTITION_ID_TRAIT
  inline GRIN_PARTITION_ID GetFragId(const vertex_t& u) const {
    auto vref = grin_get_vertex_ref_for_vertex(g_, u.grin_v);
    auto partition = grin_get_master_partition_from_vertex_ref(g_, vref);
    return grin_get_partition_id(pg_, partition);
  }
#endif

/*
  inline bool Gid2Vertex(const vid_t& gid, vertex_t& v) const {
    if (fragment_->Gid2Vertex(gid, v)) {
      v.grin
      v.SetValue(union_id_parser_.GenerateContinuousLid(v.GetValue()));
      return true;
    }
    return false;
  }

  inline vid_t Vertex2Gid(const vertex_t& v) const {
    vertex_t v_(union_id_parser_.ParseContinuousLid(v.GetValue()));
    return fragment_->Vertex2Gid(v_);
  }
  */

  bool GetData(const vertex_t& v, vdata_t& value) const {
    if (v.grin_v == GRIN_NULL_VERTEX) return false;
#ifdef GRIN_WITH_VERTEX_DATA
    if (GRIN_DATATYPE_ENUM<vdata_t>::value != grin_get_vertex_data_type(g_, v)) return false;
    auto _value = grin_get_vertex_data_value(g_, v.grin_v)
    if (_value != NULL) {
      value = *(static_cast<vdata_t*>(_value));
      return true;
    }
#else
    if (GRIN_DATATYPE_ENUM<vdata_t>::value != grin_get_vertex_property_data_type(g_, v_prop_)) return false;
    auto vtype = grin_get_vertex_type(g_, v.grin_v);
    auto vpt = grin_get_vertex_property_table_by_type(g_, vtype);
    auto v_prop = grin_get
    auto _value = grin_get_value_from_vertex_property_table(g_, vpt, v, v_prop_);
    if (_value != NULL) {
      value = *(static_cast<vdata_t*>(_value));
      return true;
    }
#endif
    return false;
  }

  inline vid_t GetInnerVerticesNum() const { return ivnum_; }

  inline vid_t GetOuterVerticesNum() const { return ovnum_; }

  inline vid_t GetVerticesNum() const { return tvnum_; }

  inline size_t GetTotalVerticesNum() const {
    return 0;
    // TODO: apply this to store
    // return grin_get_total_vertex_num(pg_);
  }

  inline size_t GetEdgeNum() const {
    return grin_get_edge_num(g_, GRIN_DIRECTION::BOTH);
  }

  inline bool IsInnerVertex(const vertex_t& v) const {
    return grin_is_master_vertex(g_, v.grin_v);
  }

  inline bool IsOuterVertex(const vertex_t& v) const {
    return grin_is_mirror_vertex(g_, v.grin_v);
  }

/*
  inline oid_t Gid2Oid(const vid_t& gid) const {
    return fragment_->Gid2Oid(gid);
  }

  inline bool Oid2Gid(const oid_t& oid, vid_t& gid) const {
    for (label_id_t label = 0; label < fragment_->vertex_label_num(); label++) {
      if (fragment_->Oid2Gid(label, oid, gid)) {
        return true;
      }
    }
    return false;
  }
*/

  inline adj_list_t GetOutgoingAdjList(const vertex_t& v) const {
    auto al = grin_get_adjacent_list(g_, GRIN_DIRECTION::OUT, v.grin_v);
    auto sz = grin_get_adjacent_list_size(g_, al);
    std::vector<GRIN_EDGE_PROPERTY_TABLE> epts;
    std::vector<size_t> agg_nums;
    auto etl = grin_get_edge_type_list(g_);
    auto etl_size = grin_get_edge_type_list_size(g_, etl);
    for (size_t i = 0; i < etl_size; ++i) {
      auto e_label = grin_get_vertex_type_from_list(g_, etl, i);
      auto ept = grin_get_edge_property_table_by_type(g_, e_label);
      auto al1 = grin_filter_edge_type_for_adjacent_list(g_, e_label, al);
      auto sz1 = grin_get_adjacent_list_size(g_, al1);
      epts.push_back(ept);
      agg_nums.push_back(sz1);
      grin_destroy_adjacent_list(g_, al1);
    }
    grin_destroy_edge_type_list(g_, etl);
    return adj_list_t(g_, al, epts, agg_nums, 0, sz);
  }

  inline adj_list_t GetIncomingAdjList(const vertex_t& v) const {
    auto al = grin_get_adjacent_list(g_, GRIN_DIRECTION::IN, v.grin_v);
    auto sz = grin_get_adjacent_list_size(g_, al);
    std::vector<GRIN_EDGE_PROPERTY_TABLE> epts;
    std::vector<size_t> agg_nums;
    auto etl = grin_get_edge_type_list(g_);
    auto etl_size = grin_get_edge_type_list_size(g_, etl);
    for (size_t i = 0; i < etl_size; ++i) {
      auto e_label = grin_get_vertex_type_from_list(g_, etl, i);
      auto ept = grin_get_edge_property_table_by_type(g_, e_label);
      auto al1 = grin_filter_edge_type_for_adjacent_list(g_, e_label, al);
      auto sz1 = grin_get_adjacent_list_size(g_, al1);
      epts.push_back(ept);
      agg_nums.push_back(sz1);
      grin_destroy_adjacent_list(g_, al1);
    }
    grin_destroy_edge_type_list(g_, etl);
    return adj_list_t(g_, al, epts, agg_nums, 0, sz);
  }

  inline int GetLocalOutDegree(const vertex_t& v) const {
    auto al = grin_get_adjacent_list(g_, GRIN_DIRECTION::OUT, v.grin_v);
    auto sz = grin_get_adjacent_list_size(g_, al);
    grin_destroy_adjacent_list(g_, al);
    return static_cast<int>(sz);
  }

  inline int GetLocalInDegree(const vertex_t& v) const {
    auto al = grin_get_adjacent_list(g_, GRIN_DIRECTION::IN, v.grin_v);
    auto sz = grin_get_adjacent_list_size(g_, al);
    grin_destroy_adjacent_list(g_, al);
    return static_cast<int>(sz);
  }

  /*
  inline dest_list_t IEDests(const vertex_t& v) const {
    vertex_t v_(union_id_parser_.ParseContinuousLid(v.GetValue()));
    std::vector<grape::DestList> dest_lists;
    dest_lists.reserve(fragment_->edge_label_num());
    for (label_id_t e_label = 0; e_label < fragment_->edge_label_num();
         e_label++) {
      dest_lists.push_back(fragment_->IEDests(v_, e_label));
    }
    return dest_list_t(dest_lists);
  }

  inline dest_list_t OEDests(const vertex_t& v) const {
    vertex_t v_(union_id_parser_.ParseContinuousLid(v.GetValue()));
    std::vector<grape::DestList> dest_lists;
    dest_lists.reserve(fragment_->edge_label_num());
    for (label_id_t e_label = 0; e_label < fragment_->edge_label_num();
         e_label++) {
      dest_lists.push_back(fragment_->OEDests(v_, e_label));
    }
    return dest_list_t(dest_lists);
  }

  inline dest_list_t IOEDests(const vertex_t& v) const {
    vertex_t v_(union_id_parser_.ParseContinuousLid(v.GetValue()));
    std::vector<grape::DestList> dest_lists;
    dest_lists.reserve(fragment_->edge_label_num());
    for (label_id_t e_label = 0; e_label < fragment_->edge_label_num();
         e_label++) {
      dest_lists.push_back(fragment_->IOEDests(v_, e_label));
    }
    return dest_list_t(dest_lists);
  }
  */

 private:
  GRIN_PARTITIONED_GRAPH pg_;
  GRIN_GRAPH g_;
  GRIN_PARTITION partition_;
  std::string v_prop_, e_prop_;

  vid_t ivnum_;
  vid_t ovnum_;
  vid_t tvnum_;
  std::vector<vid_t> ivnums_, ovnums_, tvnums_;
};

}  // namespace gs
#endif  // ANALYTICAL_ENGINE_CORE_FRAGMENT_ARROW_FLATTENED_FRAGMENT_H_
