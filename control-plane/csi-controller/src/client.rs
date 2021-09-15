use common_lib::types::v0::openapi::models::{
    CreateVolumeBody, ExplicitTopology, Node, Pool, Topology, Volume, VolumeHealPolicy,
    VolumeShareProtocol,
};

use anyhow::{anyhow, Result};
use once_cell::sync::OnceCell;
use reqwest::{Client, Error, Response, StatusCode, Url};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use tracing::instrument;

#[derive(Debug, PartialEq, Eq)]
pub enum ApiClientError {
    // Error while communicating with the server.
    ServerCommunicationError(String),
    // Requested resource already exists. This error has a dedicated variant
    // in order to handle resource idempotency properly.
    ResourceAlreadyExists(String),
    // No resource instance exists.
    ResourceNotExists(String),
    // Generic operation errors.
    GenericOperationError(String),
    // Problems with parsing response body.
    InvalidResponseError(String),
}

static REST_CLIENT: OnceCell<MayastorApiClient> = OnceCell::new();

// REST API URI names for API objects.
mod uri {
    pub const VOLUMES: &str = "volumes";
    pub const POOLS: &str = "pools";
    pub const NODES: &str = "nodes";
}

/// Enum for representing URI.
#[derive(Debug)]
struct UrnType<'a>(&'a [&'a str]);

impl UrnType<'_> {
    /// Classifies URI as a tuple (resource type, resource id) based on URI.
    pub fn classify(&self) -> (String, String) {
        match self.0.len() {
            0 | 1 => panic!("Resource URI must contain collection name and resource id"),
            _ => {
                let rtype = match self.0[0] {
                    uri::VOLUMES => "volume",
                    uri::POOLS => "pool",
                    uri::NODES => "node",
                    unknown => panic!("Unknown resource type: {}", unknown),
                };

                (rtype.to_string(), self.0[1].to_string())
            }
        }
    }
}

impl Display for UrnType<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.join("/"))
    }
}

/// Single instance API client for accessing REST API gateway.
/// Incapsulates communication with REST API by exposing a set of
/// high-level API functions, which perform (de)serialization
/// of API request/response objects.
#[derive(Debug)]
pub struct MayastorApiClient {
    base_url: String,
    rest_client: Client,
}

impl MayastorApiClient {
    /// Initialize API client instance. Must be called prior to
    /// obtaining the client instance.
    pub fn initialize(endpoint: String) -> Result<()> {
        if REST_CLIENT.get().is_some() {
            return Err(anyhow!("API client already initialized"));
        }

        let rest_client = reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .expect("Failed to build REST client");

        REST_CLIENT.get_or_init(|| Self {
            base_url: format!("{}/v0", endpoint),
            rest_client,
        });

        debug!("API client is initialized with endpoint {}", endpoint);
        Ok(())
    }

    /// Obtain client instance. Panics if called before the client
    /// has been initialized.
    pub fn get_client() -> &'static MayastorApiClient {
        REST_CLIENT.get().expect("Rest client is not initialized")
    }
}

/// Generate a getter for a given collection URI.
macro_rules! collection_getter {
    ($name:ident, $t:ty, $urn:expr) => {
        pub async fn $name(&self) -> Result<Vec<$t>, ApiClientError> {
            self.get_collection::<$t>($urn).await
        }
    };
}

impl MayastorApiClient {
    async fn get_collection_item<R>(&self, urn: UrnType<'_>) -> Result<R, ApiClientError>
    where
        for<'a> R: Deserialize<'a>,
    {
        let response = self.do_get(&urn).await.map_err(|e| {
            ApiClientError::ServerCommunicationError(format!(
                "Failed to get {:?}, error = {}",
                urn, e
            ))
        })?;

        // Check HTTP status code.
        match response.status() {
            StatusCode::OK => {}
            StatusCode::NOT_FOUND => {
                let (rtype, rname) = urn.classify();
                return Err(ApiClientError::ResourceNotExists(format!(
                    "{} {} not found",
                    rtype, rname
                )));
            }
            http_status => {
                return Err(ApiClientError::GenericOperationError(format!(
                    "Failed to GET {:?}, HTTP error = {}",
                    urn, http_status,
                )))
            }
        };

        // Get response body if request succeeded.
        let body = response.bytes().await.map_err(|e| {
            ApiClientError::InvalidResponseError(format!(
                "Failed to obtain body from HTTP response while getting {}, error = {}",
                urn, e,
            ))
        })?;

        serde_json::from_slice::<R>(&body).map_err(|e| {
            ApiClientError::InvalidResponseError(format!(
                "Failed to deserialize object {}, error = {}",
                std::any::type_name::<R>(),
                e
            ))
        })
    }

    // Get one resource instance.
    async fn do_get(&self, urn: &UrnType<'_>) -> Result<Response, Error> {
        let u = format!("{}/{}", self.base_url, urn);
        let uri = Url::parse(&u).unwrap();

        self.rest_client.get(uri).send().await
    }

    // Perform resource deletion, optionally idempotent.
    async fn do_delete(&self, urn: &UrnType<'_>, idempotent: bool) -> Result<(), ApiClientError> {
        let u = format!("{}/{}", self.base_url, urn);
        let uri = Url::parse(&u).unwrap();

        let response = self.rest_client.delete(uri).send().await.map_err(|e| {
            ApiClientError::ServerCommunicationError(format!(
                "DELETE {} request failed, error={}",
                u, e
            ))
        })?;

        // Check HTTP status code, handle DELETE idempotency transparently.
        let res = match response.status() {
            StatusCode::OK => Ok(()),
            // Handle idempotency as requested by the caller.
            StatusCode::NOT_FOUND | StatusCode::NO_CONTENT | StatusCode::PRECONDITION_FAILED => {
                if idempotent {
                    Ok(())
                } else {
                    let (rtype, rname) = urn.classify();
                    return Err(ApiClientError::ResourceNotExists(format!(
                        "{} {} not found",
                        rtype, rname
                    )));
                }
            }
            code => Err(ApiClientError::GenericOperationError(format!(
                "DELETE {} failed, HTTP status code = {}",
                u, code
            ))),
        };
        debug!("Resource {} successfully deleted", u);
        res
    }

    async fn do_put<I, O>(&self, urn: &UrnType<'_>, object: I) -> Result<O, ApiClientError>
    where
        I: Serialize + Sized,
        for<'a> O: Deserialize<'a>,
    {
        let u = format!("{}/{}", self.base_url, urn);
        let uri = Url::parse(&u).unwrap();

        let response = self
            .rest_client
            .put(uri)
            .json(&object)
            .send()
            .await
            .map_err(|e| {
                ApiClientError::ServerCommunicationError(format!(
                    "PUT {} request failed, error={}",
                    u, e
                ))
            })?;

        // Check HTTP status of the operation.
        match response.status() {
            StatusCode::OK => {}
            StatusCode::UNPROCESSABLE_ENTITY => {
                return Err(ApiClientError::ResourceAlreadyExists(format!(
                    "Resource {} already exists",
                    u
                )));
            }
            _ => {
                return Err(ApiClientError::GenericOperationError(format!(
                    "PUT {} failed, HTTP status = {}",
                    u,
                    response.status()
                )));
            }
        };

        let body = response.bytes().await.map_err(|e| {
            ApiClientError::InvalidResponseError(format!(
                "Failed to obtain body from HTTP PUT {} response, error = {}",
                u, e,
            ))
        })?;

        serde_json::from_slice::<O>(&body).map_err(|e| {
            ApiClientError::InvalidResponseError(format!(
                "Failed to deserialize object {}, error = {}",
                std::any::type_name::<O>(),
                e
            ))
        })
    }

    async fn get_collection<R>(&self, urn: UrnType<'_>) -> Result<Vec<R>, ApiClientError>
    where
        for<'a> R: Deserialize<'a>,
    {
        let body = self
            .do_get(&urn)
            .await
            .map_err(|e| {
                ApiClientError::ServerCommunicationError(format!(
                    "Failed to GET {:?}, error = {}",
                    urn, e
                ))
            })?
            .bytes()
            .await
            .map_err(|e| {
                ApiClientError::InvalidResponseError(format!(
                    "Failed to obtain body from HTTP response while listing {:?}, error = {}",
                    urn, e,
                ))
            })?;

        serde_json::from_slice::<Vec<R>>(&body).map_err(|e| {
            ApiClientError::InvalidResponseError(format!(
                "Failed to deserialize objects {}, error = {}",
                std::any::type_name::<R>(),
                e
            ))
        })
    }

    // List all nodes available in Mayastor cluster.
    collection_getter!(list_nodes, Node, UrnType(&[uri::NODES]));

    // List all pools available in Mayastor cluster.
    collection_getter!(list_pools, Pool, UrnType(&[uri::POOLS]));

    // List all volumes available in Mayastor cluster.
    collection_getter!(list_volumes, Volume, UrnType(&[uri::VOLUMES]));

    // List pools available on target Mayastor node.
    pub async fn get_node_pools(&self, node: &str) -> Result<Vec<Pool>, ApiClientError> {
        self.get_collection(UrnType(&[uri::NODES, node, uri::POOLS]))
            .await
    }

    #[instrument]
    /// Create a volume of target size and provision storage resources for it.
    /// This operation is not idempotent, so the caller is responsible for taking
    /// all actions with regards to idempotency.
    pub async fn create_volume(
        &self,
        volume_id: &str,
        replicas: u8,
        size: u64,
        allowed_nodes: &[String],
        preferred_nodes: &[String],
    ) -> Result<Volume, ApiClientError> {
        let mut allowed = Vec::new();
        let mut preferred = Vec::new();

        allowed.extend_from_slice(allowed_nodes);
        preferred.extend_from_slice(preferred_nodes);

        let topology = Topology::new_all(Some(ExplicitTopology::new(allowed, preferred)), None);

        let req = CreateVolumeBody {
            replicas,
            size,
            topology,
            policy: VolumeHealPolicy::default(),
        };

        self.do_put(&UrnType(&[uri::VOLUMES, volume_id]), &req)
            .await
    }

    #[instrument]
    /// Delete volume and reclaim all storage resources associated with it.
    /// This operation is idempotent, so the caller does not see errors indicating
    /// abscence of the resource.
    pub async fn delete_volume(&self, volume_id: &str) -> Result<(), ApiClientError> {
        self.do_delete(&UrnType(&[uri::VOLUMES, volume_id]), true)
            .await
    }

    #[instrument]
    /// Describe specific volume.
    pub async fn get_volume(&self, volume_id: &str) -> Result<Volume, ApiClientError> {
        self.get_collection_item(UrnType(&[uri::VOLUMES, volume_id]))
            .await
    }

    #[instrument]
    /// Unublish volume (i.e. destroy a target which exposes the volume).
    pub async fn unpublish_volume(&self, volume_id: &str) -> Result<(), ApiClientError> {
        self.do_delete(&UrnType(&[uri::VOLUMES, volume_id, "target"]), false)
            .await
    }

    #[instrument]
    /// Publish volume (i.e. make it accessible via specified protocol by creating a target).
    pub async fn publish_volume(
        &self,
        volume_id: &str,
        node: &str,
        protocol: VolumeShareProtocol,
    ) -> Result<Volume, ApiClientError> {
        let u = format!("target?protocol={}&node={}", protocol.to_string(), node,);

        self.do_put(&UrnType(&[uri::VOLUMES, volume_id, &u]), protocol)
            .await
    }
}
