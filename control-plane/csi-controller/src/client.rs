use common_lib::types::v0::openapi::models::{Node, Pool};

use anyhow::{anyhow, Result};
use once_cell::sync::OnceCell;
use reqwest::{Client, Error, Response, Url};
use serde::Deserialize;

static REST_CLIENT: OnceCell<MayastorApiClient> = OnceCell::new();

/// TODO: Implement formatting {}
/// Enum for representing URI in both single and multipart forms.
#[derive(Debug)]
enum UrnType<'a> {
    /// Single-part URI
    Single(&'a str),
    /// Multi-part URI
    Multiple(&'a [&'a str]),
}

/// TODO:
pub struct MayastorApiClient {
    base_url: String,
    rest_client: Client,
}

impl MayastorApiClient {
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

    pub fn get_client() -> &'static MayastorApiClient {
        REST_CLIENT.get().expect("Rest client is not initialized")
    }
}

macro_rules! collection_getter {
    ($name:ident, $t:ty, $urn:expr) => {
        pub async fn $name(&self) -> Result<Vec<$t>> {
            self.get_collection::<$t>($urn).await
        }
    };
}

impl MayastorApiClient {
    async fn _get_collection_item<R>(&self, urn: UrnType<'_>) -> Result<R>
    where
        for<'a> R: Deserialize<'a>,
    {
        let body = self
            .do_get(&urn)
            .await
            .map_err(|e| anyhow!("Failed to get {:?}, error = {}", urn, e))?
            .bytes()
            .await
            .map_err(|e| {
                anyhow!(
                    "Failed to obtain body from HTTP response while getting {:?}, error = {}",
                    urn,
                    e,
                )
            })?;

        serde_json::from_slice::<R>(&body).map_err(|e| {
            anyhow!(
                "Failed to deserialize object {}, error = {}",
                std::any::type_name::<R>(),
                e
            )
        })
    }

    async fn do_get(&self, urn: &UrnType<'_>) -> Result<Response, Error> {
        let p = match urn {
            UrnType::Single(s) => s.to_string(),
            UrnType::Multiple(ss) => ss.join("/"),
        };

        let u = format!("{}/{}", self.base_url, p);
        debug!("Issuing GET for URL {}", u);
        let uri = Url::parse(&u).unwrap();
        self.rest_client.get(uri).send().await
    }

    async fn get_collection<R>(&self, urn: UrnType<'_>) -> Result<Vec<R>>
    where
        for<'a> R: Deserialize<'a>,
    {
        let body = self
            .do_get(&urn)
            .await
            .map_err(|e| anyhow!("Failed to list {:?}, error = {}", urn, e))?
            .bytes()
            .await
            .map_err(|e| {
                anyhow!(
                    "Failed to obtain body from HTTP response while listing {:?}, error = {}",
                    urn,
                    e,
                )
            })?;

        serde_json::from_slice::<Vec<R>>(&body).map_err(|e| {
            anyhow!(
                "Failed to deserialize objects {}, error = {}",
                std::any::type_name::<R>(),
                e
            )
        })
    }

    collection_getter!(list_nodes, Node, UrnType::Single("nodes"));
    collection_getter!(list_pools, Pool, UrnType::Single("pools"));

    pub async fn get_node_pools(&self, node: &str) -> Result<Vec<Pool>> {
        self.get_collection(UrnType::Multiple(&["nodes", node, "pools"]))
            .await
    }
}
