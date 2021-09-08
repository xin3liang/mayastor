use crate::MayastorApiClient;
use regex::Regex;
use rpc::csi::*;
use tonic::{Response, Status};
use tracing::instrument;

use common_lib::types::v0::openapi::models::Pool;

const K8S_HOSTNAME: &str = "kubernetes.io/hostname";
const VOLUME_NAME_PATTERN: &str =
    r"pvc-([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})";
const PROTO_NVMF: &str = "nvmf";

#[derive(Debug, Default)]
pub struct CsiControllerSvc {}

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

#[tonic::async_trait]
impl rpc::csi::controller_server::Controller for CsiControllerSvc {
    #[instrument]
    async fn create_volume(
        &self,
        request: tonic::Request<CreateVolumeRequest>,
    ) -> Result<tonic::Response<CreateVolumeResponse>, tonic::Status> {
        let args = request.into_inner();

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
            Some(range) => range.required_bytes as u64,
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
        let _io_timeout = match args.parameters.get("ioTimeout") {
            Some(t) => {
                if protocol != PROTO_NVMF {
                    return Err(Status::invalid_argument(
                        "I/O timeout is valid only for nvmf protocol",
                    ));
                }
                match t.parse::<u64>() {
                    Ok(c) => Some(c),
                    Err(_) => return Err(Status::invalid_argument("Invalid I/O timeout")),
                }
            }
            None => None,
        };

        let replica_count: u8 = match args.parameters.get("repl") {
            Some(c) => match c.parse::<u8>() {
                Ok(c) => c,
                Err(_) => return Err(Status::invalid_argument("Invalid I/O timeout")),
            },
            None => 1,
        };

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
        let mut must_nodes: Vec<String> = Vec::new();
        let mut should_nodes: Vec<String> = Vec::new();

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
                    must_nodes.push(v.to_string());
                }
            }

            for p in reqs.preferred.iter() {
                for (k, v) in p.segments.iter() {
                    // Ignore others than hostname (it's only preferred)
                    if k == K8S_HOSTNAME {
                        should_nodes.push(v.to_string());
                    }
                }
            }
        }

        let volume = MayastorApiClient::get_client()
            .create_volume(&volume_uuid, replica_count, size, None, None)
            .await
            .map_err(|e| {
                Status::internal(format!(
                    "Failed to create volume {}, error = {}",
                    volume_uuid, e
                ))
            })?;
        info!("Volume created ! {:?}", volume);

        Ok(Response::new(CreateVolumeResponse {
            volume: Some(Volume {
                capacity_bytes: volume.spec.size as i64,
                volume_id: volume_uuid,
                volume_context: args.parameters,
                content_source: None,
                // Volume can be accessed from any node.
                accessible_topology: Vec::new(),
            }),
        }))
    }

    async fn delete_volume(
        &self,
        _request: tonic::Request<DeleteVolumeRequest>,
    ) -> Result<tonic::Response<DeleteVolumeResponse>, tonic::Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn controller_publish_volume(
        &self,
        _request: tonic::Request<ControllerPublishVolumeRequest>,
    ) -> Result<tonic::Response<ControllerPublishVolumeResponse>, tonic::Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn controller_unpublish_volume(
        &self,
        _request: tonic::Request<ControllerUnpublishVolumeRequest>,
    ) -> Result<tonic::Response<ControllerUnpublishVolumeResponse>, tonic::Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn validate_volume_capabilities(
        &self,
        _request: tonic::Request<ValidateVolumeCapabilitiesRequest>,
    ) -> Result<tonic::Response<ValidateVolumeCapabilitiesResponse>, tonic::Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn list_volumes(
        &self,
        _request: tonic::Request<ListVolumesRequest>,
    ) -> Result<tonic::Response<ListVolumesResponse>, tonic::Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    #[instrument]
    async fn get_capacity(
        &self,
        request: tonic::Request<GetCapacityRequest>,
    ) -> Result<tonic::Response<GetCapacityResponse>, tonic::Status> {
        let args = request.into_inner();

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
                .map_err(|e| Status::internal(e.to_string()))?
        } else {
            debug!("Calculating overall pool capacity");
            MayastorApiClient::get_client()
                .list_pools()
                .await
                .map_err(|e| Status::internal(e.to_string()))?
        };

        let available_capacity: i64 = pools.into_iter().fold(0, |acc, p| match p.state {
            Some(state) => acc + state.capacity as i64,
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
