use crate::{ApiClientError, MayastorApiClient};
use regex::Regex;
use rpc::csi::*;
use std::collections::HashMap;
use tonic::{Response, Status};
use tracing::instrument;
use uuid::Uuid;

use common_lib::types::v0::openapi::models::{
    Node, Pool, PoolStatus, SpecStatus, Volume, VolumeShareProtocol,
};

use rpc::csi::Topology as CsiTopology;

const K8S_HOSTNAME: &str = "kubernetes.io/hostname";
const VOLUME_NAME_PATTERN: &str =
    r"pvc-([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})";
const PROTO_NVMF: &str = "nvmf";
const MAYASTOR_NODE_PREFIX: &str = "mayastor://";

#[derive(Debug, Default)]
pub struct CsiControllerSvc {}

mod volume_opts {
    pub const IO_TIMEOUT: &str = "ioTimeout";
    pub const LOCAL_VOLUME: &str = "local";

    const YAML_TRUE_VALUE: [&str; 11] = [
        "y", "Y", "yes", "Yes", "YES", "true", "True", "TRUE", "on", "On", "ON",
    ];

    // Decode 'local' volume attribute into a boolean flag.
    pub fn decode_local_volume_flag(encoded: Option<&String>) -> bool {
        match encoded {
            Some(v) => YAML_TRUE_VALUE.iter().any(|p| p == v),
            None => false,
        }
    }
}

/// Check whether target volume capabilites are valid. As of now, only
/// SingleNodeWriter capability is supported.
fn check_volume_capabilities(capabilities: &[VolumeCapability]) -> Result<(), tonic::Status> {
    for c in capabilities {
        if let Some(access_mode) = c.access_mode.as_ref() {
            if access_mode.mode != volume_capability::access_mode::Mode::SingleNodeWriter as i32 {
                return Err(Status::invalid_argument(format!(
                    "Invalid volume access mode: {:?}",
                    access_mode.mode
                )));
            }
        }
    }
    Ok(())
}

/// Parse string protocol into REST API protocol enum.
fn parse_protocol(proto: &str) -> Result<VolumeShareProtocol, Status> {
    match proto {
        "iscsi" => Ok(VolumeShareProtocol::Iscsi),
        "nvmf" => Ok(VolumeShareProtocol::Nvmf),
        _ => Err(Status::invalid_argument(format!(
            "Invalid protocol: {}",
            proto
        ))),
    }
}

/// Transform Kubernetes Mayastor node ID into its real hostname.
fn normalize_hostname(name: String) -> String {
    if let Some(hostname) = name.strip_prefix(MAYASTOR_NODE_PREFIX) {
        hostname.to_string()
    } else {
        name
    }
}

/// Get share URI for existing volume object.
fn get_volume_share_uri(volume: &Volume) -> Result<String, Status> {
    let volume_id = volume.spec.uuid;

    match volume.state.as_ref() {
        Some(state) => {
            if let Some(nexus) = state.child.as_ref() {
                if nexus.device_uri.is_empty() {
                    let m = format!("No nexus device URI available for volume {}", volume_id);
                    error!("{}", m);
                    return Err(Status::internal(m));
                }
                Ok(nexus.device_uri.to_string())
            } else {
                let m = format!("No nexus info available for volume {}", volume_id);
                error!("{}", m);
                Err(Status::internal(m))
            }
        }
        None => {
            let m = format!("Volume {} reports no current state", volume_id);
            Err(Status::internal(m))
        }
    }
}

impl From<ApiClientError> for Status {
    fn from(error: ApiClientError) -> Self {
        match error {
            ApiClientError::ResourceNotExists(reason) => Status::not_found(reason),
            error => Status::internal(format!("Operation failed: {:?}", error)),
        }
    }
}

/// Check whether existing volume is compatible with requested configuration.
/// Target volume is assumed to exist.
/// TODO: Add full topology check once Control Plane supports full volume spec.
#[instrument]
fn check_existing_volume(
    volume: &Volume,
    replica_count: u8,
    size: u64,
    pinned_volume: bool,
) -> Result<(), Status> {
    // Check if the existing volume is compatible, which means
    //  - number of replicas is equal or greater
    //  - size is equal or greater
    //  - volume is fully created
    let spec = &volume.spec;

    if spec.status != SpecStatus::Created {
        return Err(Status::already_exists(format!(
            "Existing volume {} is in insufficient state: {:?}",
            spec.uuid, spec.status
        )));
    }

    if spec.num_replicas < replica_count {
        return Err(Status::already_exists(format!(
            "Existing volume {} has insufficient number of replicas: {} ({} requested)",
            spec.uuid, spec.num_replicas, replica_count
        )));
    }

    if spec.size < size {
        return Err(Status::already_exists(format!(
            "Existing volume {} has insufficient size: {} bytes ({} requested)",
            spec.uuid, spec.size, size
        )));
    }

    Ok(())
}

struct VolumeTopologyMapper {
    nodes: Vec<Node>,
}

impl VolumeTopologyMapper {
    async fn init() -> Result<VolumeTopologyMapper, Status> {
        let nodes = MayastorApiClient::get_client()
            .list_nodes()
            .await
            .map_err(|e| {
                Status::failed_precondition(format!(
                    "Failed to list Mayastor nodes, error = {:?}",
                    e
                ))
            })?;

        Ok(Self { nodes })
    }

    // Determine the list of nodes where the workload can be placed.
    // If volume is created as pinned (i.e. local=true), then the nexus and the workload
    // must be placed on the same node, which in fact means running workloads only on Mayastor
    // daemonset nodes.
    // For non-pinned volumes, workload can be put on any node in the Kubernetes cluster.
    pub fn volume_accessible_topology(&self, pinned_volume: bool) -> Vec<CsiTopology> {
        if pinned_volume {
            self.nodes
                .iter()
                .map(|n| {
                    let mut segments = HashMap::new();
                    segments.insert(K8S_HOSTNAME.to_string(), n.id.to_string());
                    rpc::csi::Topology { segments }
                })
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Determines whether target volume is pinned.
    /// TODO: as of now all volumes are assumed pinned.
    pub fn is_volume_pinned(_volume: &Volume) -> bool {
        true
    }
}

#[tonic::async_trait]
impl rpc::csi::controller_server::Controller for CsiControllerSvc {
    #[instrument]
    async fn create_volume(
        &self,
        request: tonic::Request<CreateVolumeRequest>,
    ) -> Result<tonic::Response<CreateVolumeResponse>, tonic::Status> {
        let args = request.into_inner();

        debug!("Request to create volume: {:?}", args);
        if args.volume_content_source.is_some() {
            return Err(Status::invalid_argument(
                "Source for create volume is not supported",
            ));
        }

        // k8s uses names pvc-{uuid} and we use uuid further as ID in SPDK so we
        // must require it.
        let re = Regex::new(VOLUME_NAME_PATTERN).unwrap();
        let volume_uuid = match re.captures(&args.name) {
            Some(captures) => captures.get(1).unwrap().as_str().to_string(),
            None => {
                return Err(Status::invalid_argument(format!(
                    "Expected the volume name in pvc-<UUID> format: {}",
                    args.name
                )))
            }
        };

        check_volume_capabilities(&args.volume_capabilities)?;

        // Check volume size.
        let size = match args.capacity_range {
            Some(range) => {
                if range.required_bytes <= 0 {
                    return Err(Status::invalid_argument(
                        "Volume size must be a non-negative number",
                    ));
                }
                range.required_bytes as u64
            }
            None => {
                return Err(Status::invalid_argument(
                    "Volume capacity range is not provided",
                ))
            }
        };

        // Check storage protocol.
        let protocol = match args.parameters.get("protocol") {
            Some(p) => p.to_string(),
            None => return Err(Status::invalid_argument("Missing storage protocol")),
        };

        // Check I/O timeout.
        if let Some(io_timeout) = args.parameters.get(volume_opts::IO_TIMEOUT) {
            if protocol != PROTO_NVMF {
                return Err(Status::invalid_argument(
                    "I/O timeout is valid only for nvmf protocol",
                ));
            }
            if io_timeout.parse::<u64>().is_err() {
                return Err(Status::invalid_argument("Invalid I/O timeout"));
            }
        }

        let replica_count: u8 = match args.parameters.get("repl") {
            Some(c) => match c.parse::<u8>() {
                Ok(c) => {
                    if c == 0 {
                        return Err(Status::invalid_argument(
                            "Replica count must be greater than zero",
                        ));
                    }
                    c
                }
                Err(_) => return Err(Status::invalid_argument("Invalid replica count")),
            },
            None => 1,
        };

        let pinned_volume =
            volume_opts::decode_local_volume_flag(args.parameters.get(volume_opts::LOCAL_VOLUME));

        // For exaplanation of accessibilityRequirements refer to a table at
        // https://github.com/kubernetes-csi/external-provisioner.
        // Our case is WaitForFirstConsumer = true, strict-topology = false.
        //
        // The first node in preferred array the node that was chosen for running
        // the app by the k8s scheduler. The rest of the entries are in random
        // order and perhaps don't even run mayastor csi node plugin.
        //
        // The requisite array contains all nodes in the cluster irrespective
        // of what node was chosen for running the app.
        let mut allowed_nodes: Vec<String> = Vec::new();
        let mut preferred_nodes: Vec<String> = Vec::new();

        if let Some(reqs) = args.accessibility_requirements {
            for r in reqs.requisite.iter() {
                for (k, v) in r.segments.iter() {
                    // We are not able to evaluate any other topology requirements than
                    // the hostname req. Reject all others.
                    if k != K8S_HOSTNAME {
                        return Err(Status::invalid_argument(
                            "Volume topology other than hostname not supported",
                        ));
                    }
                    allowed_nodes.push(v.to_string());
                }
            }

            for p in reqs.preferred.iter() {
                for (k, v) in p.segments.iter() {
                    // Ignore others than hostname (it's only preferred)
                    if k == K8S_HOSTNAME {
                        preferred_nodes.push(v.to_string());
                    }
                }
            }
        }

        let u = Uuid::parse_str(&volume_uuid).map_err(|_e| {
            Status::invalid_argument(format!("Malformed volume UUID: {}", volume_uuid))
        })?;

        // First check if the volume already exists.
        if let Some(existing_volume) = MayastorApiClient::get_client()
            .list_volumes()
            .await?
            .into_iter()
            .find(|v| v.spec.uuid == u)
        {
            check_existing_volume(&existing_volume, replica_count, size, pinned_volume)?;
            debug!(
                "Volume {} already exists and is compatible with requested config",
                volume_uuid
            );
        } else {
            MayastorApiClient::get_client()
                .create_volume(
                    &volume_uuid,
                    replica_count,
                    size,
                    &allowed_nodes,
                    &preferred_nodes,
                )
                .await?;

            debug!(
                "Volume {} successfully created, pinned volume = {}",
                volume_uuid, pinned_volume
            );
        }

        let vt_mapper = VolumeTopologyMapper::init().await?;

        let volume = rpc::csi::Volume {
            capacity_bytes: size as i64,
            volume_id: volume_uuid,
            volume_context: args.parameters.clone(),
            content_source: None,
            accessible_topology: vt_mapper.volume_accessible_topology(pinned_volume),
        };

        debug!("Created volume: {:?}", volume);

        Ok(Response::new(CreateVolumeResponse {
            volume: Some(volume),
        }))
    }

    async fn delete_volume(
        &self,
        request: tonic::Request<DeleteVolumeRequest>,
    ) -> Result<tonic::Response<DeleteVolumeResponse>, tonic::Status> {
        let args = request.into_inner();

        debug!("Request to delete volume: {:?}", args);
        MayastorApiClient::get_client()
            .delete_volume(&args.volume_id)
            .await
            .map_err(|e| {
                Status::internal(format!(
                    "Failed to delete volume {}, error = {:?}",
                    args.volume_id, e
                ))
            })?;

        Ok(Response::new(DeleteVolumeResponse {}))
    }

    async fn controller_publish_volume(
        &self,
        request: tonic::Request<ControllerPublishVolumeRequest>,
    ) -> Result<tonic::Response<ControllerPublishVolumeResponse>, tonic::Status> {
        let args = request.into_inner();

        debug!("Request to publish volume: {:?}", args);
        if args.readonly {
            return Err(Status::invalid_argument(
                "Read-only volumes are not supported",
            ));
        }

        let protocol = match args.volume_context.get("protocol") {
            Some(p) => parse_protocol(p)?,
            None => {
                return Err(Status::invalid_argument(
                    "No protocol specified for publish volume request",
                ))
            }
        };

        if args.node_id.is_empty() {
            return Err(Status::invalid_argument("Node ID must not be empty"));
        }

        if args.volume_id.is_empty() {
            return Err(Status::invalid_argument("Volume ID must not be empty"));
        }

        match args.volume_capability {
            Some(c) => {
                check_volume_capabilities(&[c])?;
            }
            None => {
                return Err(Status::invalid_argument("Missing volume capability"));
            }
        };

        let node_id = normalize_hostname(args.node_id);
        let volume_id = args.volume_id.to_string();

        // Check if the volume is already published.
        let volume = MayastorApiClient::get_client()
            .get_volume(&volume_id)
            .await?;
        let uri = if let Some(state) = volume.state.as_ref() {
            let curr_proto = state.protocol.to_string();

            // Volume is aready published, make sure the protocol matches and get URI.
            if curr_proto != "none" {
                if curr_proto != *args.volume_context.get("protocol").unwrap().to_string() {
                    let m = format!(
                        "Volume {} already shared via different protocol: {:?}",
                        volume_id, state.protocol,
                    );
                    error!("{}", m);
                    return Err(Status::failed_precondition(m));
                }
                let uri = get_volume_share_uri(&volume)?;
                debug!("Volume {} already published at {}", volume_id, uri);
                uri
            } else {
                // Volume is not published.
                let v = MayastorApiClient::get_client()
                    .publish_volume(&volume_id, &node_id, protocol)
                    .await?;
                let uri = get_volume_share_uri(&v)?;
                debug!("Volume {} successfully published at {}", volume_id, uri);
                uri
            }
        } else {
            let m = format!("Volume {} is missing current state", volume_id);
            error!("{}", m);
            return Err(Status::internal(m));
        };

        // Prepare the context for the Mayastor Node CSI plugin.
        let mut publish_context = HashMap::new();
        publish_context.insert("uri".to_string(), uri);

        if let Some(io_timeout) = args.volume_context.get(volume_opts::IO_TIMEOUT) {
            publish_context.insert(volume_opts::IO_TIMEOUT.to_string(), io_timeout.to_string());
        }

        debug!(
            "Publish context for volume {}: {:?}",
            volume_id, publish_context
        );
        Ok(Response::new(ControllerPublishVolumeResponse {
            publish_context,
        }))
    }

    #[instrument]
    async fn controller_unpublish_volume(
        &self,
        request: tonic::Request<ControllerUnpublishVolumeRequest>,
    ) -> Result<tonic::Response<ControllerUnpublishVolumeResponse>, tonic::Status> {
        let args = request.into_inner();

        debug!("Request to unpublish volume: {:?}", args);
        // Check if node exists.

        MayastorApiClient::get_client()
            .unpublish_volume(&args.volume_id)
            .await
            .map_err(|e| {
                Status::internal(format!(
                    "Failed to unpublish volume {}, error = {:?}",
                    &args.volume_id, e
                ))
            })?;

        Ok(Response::new(ControllerUnpublishVolumeResponse {}))
    }

    #[instrument]
    async fn validate_volume_capabilities(
        &self,
        request: tonic::Request<ValidateVolumeCapabilitiesRequest>,
    ) -> Result<tonic::Response<ValidateVolumeCapabilitiesResponse>, tonic::Status> {
        let args = request.into_inner();

        debug!("Request to validate volume capabilities: {:?}", args);
        let _volume = MayastorApiClient::get_client()
            .get_volume(&args.volume_id)
            .await
            .map_err(|_e| Status::unimplemented("Not implemented"))?;

        let caps: Vec<VolumeCapability> = args
            .volume_capabilities
            .into_iter()
            .filter(|cap| {
                if let Some(access_mode) = cap.access_mode.as_ref() {
                    if access_mode.mode
                        == volume_capability::access_mode::Mode::SingleNodeWriter as i32
                    {
                        return true;
                    }
                }
                false
            })
            .collect();

        let response = if !caps.is_empty() {
            ValidateVolumeCapabilitiesResponse {
                confirmed: Some(validate_volume_capabilities_response::Confirmed {
                    volume_context: HashMap::new(),
                    parameters: HashMap::new(),
                    volume_capabilities: caps,
                }),
                message: "".to_string(),
            }
        } else {
            ValidateVolumeCapabilitiesResponse {
                confirmed: None,
                message: "The only supported capability is SINGLE_NODE_WRITER".to_string(),
            }
        };

        Ok(Response::new(response))
    }

    #[instrument]
    async fn list_volumes(
        &self,
        request: tonic::Request<ListVolumesRequest>,
    ) -> Result<tonic::Response<ListVolumesResponse>, tonic::Status> {
        let args = request.into_inner();

        debug!("Request to list volumes: {:?}", args);

        let max_entries = args.max_entries;
        if max_entries < 0 {
            return Err(Status::invalid_argument("max_entries can't be negative"));
        }

        let vt_mapper = VolumeTopologyMapper::init().await?;

        let entries = MayastorApiClient::get_client()
            .list_volumes()
            .await
            .map_err(|e| Status::internal(format!("Failed to list volumes, error = {:?}", e)))?
            .into_iter()
            .take(if max_entries > 0 {
                max_entries as usize
            } else {
                usize::MAX
            })
            .map(|v| {
                let volume = rpc::csi::Volume {
                    volume_id: v.spec.uuid.to_string(),
                    capacity_bytes: v.spec.size as i64,
                    volume_context: HashMap::new(),
                    content_source: None,
                    accessible_topology: vt_mapper
                        .volume_accessible_topology(VolumeTopologyMapper::is_volume_pinned(&v)),
                };

                list_volumes_response::Entry {
                    volume: Some(volume),
                    status: None,
                }
            })
            .collect();

        debug!("Available Mayastor k8s volumes: {:?}", entries);

        Ok(Response::new(ListVolumesResponse {
            entries,
            next_token: "".to_string(),
        }))
    }

    #[instrument]
    async fn get_capacity(
        &self,
        request: tonic::Request<GetCapacityRequest>,
    ) -> Result<tonic::Response<GetCapacityResponse>, tonic::Status> {
        let args = request.into_inner();

        debug!("Request to get storage capacity: {:?}", args);

        // Check capabilities.
        check_volume_capabilities(&args.volume_capabilities)?;

        // Determine target node, if requested.
        let node: Option<&String> = if let Some(topology) = args.accessible_topology.as_ref() {
            topology.segments.get(K8S_HOSTNAME)
        } else {
            None
        };

        let pools: Vec<Pool> = if let Some(node) = node {
            debug!("Calculating pool capacity for node {}", node);
            MayastorApiClient::get_client()
                .get_node_pools(node)
                .await
                .map_err(|e| {
                    Status::internal(format!(
                        "Failed to list pools for node {}, error = {:?}",
                        node, e,
                    ))
                })?
        } else {
            debug!("Calculating overall pool capacity");
            MayastorApiClient::get_client()
                .list_pools()
                .await
                .map_err(|e| {
                    Status::internal(format!("Failed to list all pools, error = {:?}", e,))
                })?
        };

        let available_capacity: i64 = pools.into_iter().fold(0, |acc, p| match p.state {
            Some(state) => match state.status {
                PoolStatus::Online | PoolStatus::Degraded => acc + state.capacity as i64,
                _ => {
                    warn!(
                        "Pool {} on node {} is in '{:?}' state, not accounting it for capacity",
                        p.id, state.node, state.status,
                    );
                    acc
                }
            },
            None => 0,
        });

        Ok(Response::new(GetCapacityResponse {
            available_capacity,
            maximum_volume_size: None,
            minimum_volume_size: None,
        }))
    }

    #[instrument]
    async fn controller_get_capabilities(
        &self,
        _request: tonic::Request<ControllerGetCapabilitiesRequest>,
    ) -> Result<tonic::Response<ControllerGetCapabilitiesResponse>, tonic::Status> {
        debug!("Request to get controller capabilities");

        let capabilities = vec![
            controller_service_capability::rpc::Type::CreateDeleteVolume,
            controller_service_capability::rpc::Type::PublishUnpublishVolume,
            controller_service_capability::rpc::Type::ListVolumes,
            controller_service_capability::rpc::Type::GetCapacity,
        ];

        Ok(Response::new(ControllerGetCapabilitiesResponse {
            capabilities: capabilities
                .into_iter()
                .map(|c| ControllerServiceCapability {
                    r#type: Some(controller_service_capability::Type::Rpc(
                        controller_service_capability::Rpc { r#type: c as i32 },
                    )),
                })
                .collect(),
        }))
    }

    async fn create_snapshot(
        &self,
        _request: tonic::Request<CreateSnapshotRequest>,
    ) -> Result<tonic::Response<CreateSnapshotResponse>, tonic::Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn delete_snapshot(
        &self,
        _request: tonic::Request<DeleteSnapshotRequest>,
    ) -> Result<tonic::Response<DeleteSnapshotResponse>, tonic::Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn list_snapshots(
        &self,
        _request: tonic::Request<ListSnapshotsRequest>,
    ) -> Result<tonic::Response<ListSnapshotsResponse>, tonic::Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn controller_expand_volume(
        &self,
        _request: tonic::Request<ControllerExpandVolumeRequest>,
    ) -> Result<tonic::Response<ControllerExpandVolumeResponse>, tonic::Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn controller_get_volume(
        &self,
        _request: tonic::Request<ControllerGetVolumeRequest>,
    ) -> Result<tonic::Response<ControllerGetVolumeResponse>, tonic::Status> {
        Err(Status::unimplemented("Not implemented"))
    }
}