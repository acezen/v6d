/** Copyright 2020-2021 Alibaba Group Holding Limited.

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

#ifndef SRC_CLIENT_CLIENT_BASE_H_
#define SRC_CLIENT_CLIENT_BASE_H_

#include <sys/mman.h>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "client/ds/object_meta.h"
#include "common/util/boost.h"
#include "common/util/status.h"
#include "common/util/uuid.h"
#include "common/util/version.h"

namespace vineyard {

struct InstanceStatus;

/**
 * @brief ClientBase is the base class for vineyard IPC and RPC client.
 *
 * ClientBase implements common communication stuffs, and leave the IPC and RPC
 * specific functionalities to Client and RPCClient.
 *
 * Vineyard's Client and RPCClient is non-copyable.
 */
class ClientBase {
 public:
  ClientBase();
  virtual ~ClientBase() {}

  ClientBase(const ClientBase&) = delete;
  ClientBase(ClientBase&&) = delete;
  ClientBase& operator=(const ClientBase&) = delete;
  ClientBase& operator=(ClientBase&&) = delete;

  /**
   * @brief Get object metadata from vineyard using given object ID.
   *
   * @param id The ID of the requested object.
   * @param tree The returned metadata tree of the requested object.
   * @param sync_remote Whether to trigger an immediate remote metadata
   *        synchronization before get specific metadata. Default is false.
   * @param wait The request could be blocked util the object with given id has
   *        been created on vineyard by other clients. Default is false.
   *
   * @return Status that indicates whether the get action succeeds.
   */
  Status GetData(const ObjectID id, json& tree, const bool sync_remote = false,
                 const bool wait = false);

  /**
   * @brief Get multiple object metadatas from vineyard using given object IDs.
   *
   * @param ids The IDs of the requested objects
   * @param trees The returned metadata trees of the requested objects
   * @param sync_remote Whether to trigger an immediate remote metadata
   *        synchronization before get specific metadata. Default is false.
   * @param wait The request could be blocked util the object with given id has
   *        been created on vineyard by other clients. Default is false.
   *
   * @return Status that indicates whether the get action has succeeded.
   */
  Status GetData(const std::vector<ObjectID>& ids, std::vector<json>& trees,
                 const bool sync_remote = false, const bool wait = false);

  /**
   * @brief Create the metadata in the vineyard server.
   *
   * @param tree The metadata that will be created in vineyard.
   * @param id The returned object ID of the created data.
   * @param instance_id The vineyard instance ID where this object is created.
   * at.
   *
   * @return Status that indicates whether the create action has succeeded.
   */
  Status CreateData(const json& tree, ObjectID& id, Signature& signature,
                    InstanceID& instance_id);

  /**
   * @brief Create the metadata in the vineyard server, after created, the
   * resulted object id in the `meta_data` will be filled.
   *
   * @param meta_data The metadata that will be created in vineyard.
   * @param id The returned object ID of the created metadata.
   *
   * @return Status that indicates whether the create action has succeeded.
   */
  Status CreateMetaData(ObjectMeta& meta_data, ObjectID& id);

  /**
   * @brief Get the meta-data of the requested object
   *
   * @param id The ID of the requested object
   * @param meta_data The returned metadata of the requested object
   * @param sync_remote Whether trigger remote sync
   *
   * @return Status that indicates whether the get action has succeeded.
   */
  virtual Status GetMetaData(const ObjectID id, ObjectMeta& meta_data,
                             const bool sync_remote = false) = 0;

  /**
   * Sync remote metadata from etcd to the connected vineyardd.
   *
   * @return Status that indicates whether the sync action has succeeded.
   */
  Status SyncMetaData();

  /**
   * @brief Delete metadata in vineyard. When the object is a used by other
   * object, it will be deleted only when the `force` parameter is specified.
   *
   * @param id The ID to delete.
   * @param force Whether to delete the object forcely. Forcely delete an object
   *        means the object and objects which use this object will be delete.
   *        Default is false.
   * @param deep Whether to delete the member of this object. Default is false.
   *        Note that when deleting object which has *direct* blob members, the
   *        processing on those blobs yields a "deep" behavior.
   *
   * @return Status that indicates whether the delete action has succeeded.
   */
  Status DelData(const ObjectID id, const bool force = false,
                 const bool deep = true);
  /**
   * @brief Delete multiple metadatas in vineyard.
   *
   * @param ids The IDs to delete.
   * @param force Whether to delete the object forcely. Forcely delete an object
   *        means the object and objects which use this object will be delete.
   *        Default is false.
   * @param deep Whether to delete the member of this object. Default is false.
   *        Note that when deleting objects which have *direct* blob members,
   *        the processing on those blobs yields a "deep" behavior.
   *
   * @return Status that indicates whether the delete action has succeeded.
   */
  Status DelData(const std::vector<ObjectID>& ids, const bool force = false,
                 const bool deep = true);

  /**
   * @brief List objectmetas in vineyard, using the given typename patterns.
   *
   * @param pattern The pattern string that will be used to matched against
   * objects' `typename`.
   * @param regex Whether the pattern is a regular expression pattern. Default
   * is false. When `regex` is false, the pattern will be treated as a glob
   * pattern.
   * @param limit The number limit for how many objects will be returned at
   * most.
   * @param meta_trees An map that contains the returned object metadatas.
   *
   * @return Status that indicates whether the list action has succeeded.
   */
  Status ListData(std::string const& pattern, bool const regex,
                  size_t const limit,
                  std::unordered_map<ObjectID, json>& meta_trees);

  /**
   * @brief Persist the given object to etcd to make it visible to clients that
   * been connected to vineyard servers in the cluster.
   *
   * @param id The object id of object that will be persisted.
   *
   * @return Status that indicates whether the persist action has succeeded.
   */
  Status Persist(const ObjectID id);

  /**
   * @brief Check if the given object has been persist to etcd.
   *
   * @param id The object id to check.
   * @param persist The result variable will be stored in `persist` as return
   * value. The value true means the object is visible by other vineyard
   * servers.
   *
   * @return Status that indicates whether the check has succeeded.
   */
  Status IfPersist(const ObjectID id, bool& persist);

  /**
   * @brief Check if the given object exists in vineyard server.
   *
   * @param id The object id to check.
   * @param exists The result variable will be stored in `exists` as return
   * value. The value true means the object exists.
   *
   * @return Status that indicates whether the check has succeeded.
   */
  Status Exists(const ObjectID id, bool& exists);

  /**
   * @brief Make a shallow copy on the given object. A "shallow copy" means the
   * result object has the same type with the source object and they shares all
   * member objects.
   *
   * @param id The object id to shallow copy.
   * @param target_id The result object id will be stored in `target_id` as
   * return value.
   *
   * @return Status that indicates whether the shallow copy has succeeded.
   */
  Status ShallowCopy(const ObjectID id, ObjectID& target_id);

  /**
   * @brief Vineyard support associating a user-specific name with an object.
   * PutName registers a name entry in vineyard server. An object can be
   * assoiciated with more than one names.
   *
   * @param id The ID of the object.
   * @param name The user-specific name that will be associated with the given
   * object.
   *
   * @return Status that indicates whether the request has succeeded.
   */
  Status PutName(const ObjectID id, std::string const& name);

  /**
   * @brief Retrieve the object ID by assoicated name.
   *
   * @param name The name of the requested object.
   * @param id The returned object ID.
   * @param wait If wait is specified, the request will be blocked util the
   * given name has been registered on vineyard by other clients.
   *
   * @return Status that indicates whether the query has succeeded.
   */
  Status GetName(const std::string& name, ObjectID& id,
                 const bool wait = false);

  /**
   * @brief Deregister a name entry. The assoicated object will be kept and
   * won't be deleted.
   *
   * @param name The name that will be deregistered.
   *
   * @return Status that indicates whether the query has succeeded.
   */
  Status DropName(const std::string& name);

  /**
   * @brief Migrate remote object to local.
   *
   * @param object_id The existing object that will be migrated to current
   * vineyardd.
   * @param result_id Record the result object id.
   * @param is_stream Indicates whether the migrated object is a stream
   *
   * @return Status that indicates if the migration success.
   */
  Status MigrateObject(const ObjectID object_id, ObjectID& result_id,
                       bool is_stream = false);

  /**
   * @brief Migrate remote stream to local.
   *
   * @param object_id The existing stream that will be migrated to current
   * vineyardd.
   * @param result_id Record the result stream id.
   *
   * @return Status that indicates if the migration success.
   */
  Status MigrateStream(const ObjectID object_id, ObjectID& result_id);

  /**
   * @brief Check if the client still connects to the vineyard server.
   *
   * @return True when the connection is still alive, otherwise false.
   */
  bool Connected() const;

  /**
   * @brief Disconnect this client.
   */
  void Disconnect();

  /**
   * @brief Get the UNIX domain socket location of the connected vineyardd
   * server.
   *
   * @return Location of the IPC socket.
   */
  std::string const& IPCSocket() { return this->ipc_socket_; }

  /**
   * @brief The RPC endpoint of the connected vineyardd server.
   *
   * @return The RPC endpoint.
   */
  std::string const& RPCEndpoint() { return this->rpc_endpoint_; }

  /**
   * @brief Get the instance id of the connected vineyard server.
   *
   * Note that for RPC client the instance id is not available.
   *
   * @return The vineyard server's instance id.
   */
  const InstanceID instance_id() const { return instance_id_; }

  /**
   * @brief Retrieve the cluster information of the connected vineyard server.
   *
   * The cluster information for every instance mainly includes the host address
   * (i.e., ip address).
   *
   * @return Status that indicates whether the query has succeeded.
   */
  Status ClusterInfo(std::map<InstanceID, json>& meta);

  /**
   * @brief Return the status of connected vineyard instance.
   *
   * If success, the `status` parameter will be reseted as an instance of
   * InstanceStatus.
   *
   * @param status The result instance status.
   *
   * @return Status that indicates whether the query has succeeded.
   */
  Status InstanceStatus(std::shared_ptr<struct InstanceStatus>& status);

  /**
   * @brief List all instances in the connected vineyard cluster.
   *
   * @param A list of instance IDs will be stored in `instances`.
   *
   * @return Status that indicates whether the query has succeeded.
   */
  Status Instances(std::vector<InstanceID>& instances);

  /**
   * @brief Get the version of connected vineyard server.
   *
   * @return Return a version string MAJOR.MINOR.PATCH that follows the semver
   * convention.
   */
  const std::string& Version() const { return server_version_; }

 protected:
  Status doWrite(const std::string& message_out);

  Status doRead(std::string& message_in);

  Status doRead(json& root);

  /**
   * @brief Implementation for migrate remote object to local.
   *
   * @return Status that indicates if the migration success.
   */
  Status migrateObjectImpl(const ObjectID object_id, ObjectID& result_id,
                           bool const local, bool const is_stream,
                           std::string const& peer,
                           std::string const& peer_rpc_endpoint);

  mutable bool connected_;
  std::string ipc_socket_;
  std::string rpc_endpoint_;
  int vineyard_conn_;
  InstanceID instance_id_;
  std::string server_version_;

  // A mutex which protects the client.
  std::recursive_mutex client_mutex_;
};

struct InstanceStatus {
  /// The connected instance id.
  const InstanceID instance_id;
  /// The deployment manner, can be local or distributed.
  const std::string deployment;
  /// The current memory usage in vineyard server, in bytes.
  const size_t memory_usage;
  /// The memory upper bound of this vineyard server, in bytes.
  const size_t memory_limit;
  /// How many requests are deferred in the queue.
  const size_t deferred_requests;
  /// How many Client connects to this vineyard server.
  const size_t ipc_connections;
  /// How many RPCClient connects to this vineyard server.
  const size_t rpc_connections;

  /**
   * @brief Initialize the status value using a json returned from the vineyard
   * server.
   *
   * @param tree JSON that returned from the vineyard server.
   */
  explicit InstanceStatus(const json& tree);
};

}  // namespace vineyard

#endif  // SRC_CLIENT_CLIENT_BASE_H_
