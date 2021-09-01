use crate::MayastorApiClient;
use rpc::csi::*;
use std::collections::HashMap;
use tonic::{Request, Response, Status};
use tracing::instrument;

/// TODO
#[derive(Debug, Default)]
pub struct CsiIdentitySvc {}

const CSI_PLUGIN_NAME: &str = "io.openebs.CSI-mayastor";
const CSI_PLUGIN_VERSION: &str = "0.5";

#[tonic::async_trait]
impl rpc::csi::identity_server::Identity for CsiIdentitySvc {
    #[instrument]
    async fn get_plugin_info(
        &self,
        _request: Request<GetPluginInfoRequest>,
    ) -> Result<Response<GetPluginInfoResponse>, Status> {
        Ok(Response::new(GetPluginInfoResponse {
            name: CSI_PLUGIN_NAME.to_string(),
            vendor_version: CSI_PLUGIN_VERSION.to_string(),
            // Optional manifest is empty.
            manifest: HashMap::new(),
        }))
    }

    #[instrument]
    async fn get_plugin_capabilities(
        &self,
        _request: tonic::Request<GetPluginCapabilitiesRequest>,
    ) -> Result<Response<GetPluginCapabilitiesResponse>, Status> {
        let capabilities = vec![
            plugin_capability::service::Type::ControllerService,
            plugin_capability::service::Type::VolumeAccessibilityConstraints,
        ];

        Ok(Response::new(GetPluginCapabilitiesResponse {
            capabilities: capabilities
                .into_iter()
                .map(|c| PluginCapability {
                    r#type: Some(plugin_capability::Type::Service(
                        plugin_capability::Service { r#type: c as i32 },
                    )),
                })
                .collect(),
        }))
    }

    #[instrument]
    async fn probe(
        &self,
        _request: tonic::Request<ProbeRequest>,
    ) -> Result<Response<ProbeResponse>, Status> {
        // Make sure REST API enpoint is accessible.
        let available = match MayastorApiClient::get_client().list_nodes().await {
            Ok(_) => true,
            Err(e) => {
                error!(
                    "Failed to locate REST server: {:?}, CSI plugin remains not ready",
                    e
                );
                false
            }
        };

        Ok(Response::new(ProbeResponse {
            ready: Some(available),
        }))
    }
}
