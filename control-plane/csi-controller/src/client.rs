use common_lib::types::v0::openapi::models::Node;

use anyhow::{anyhow, Result};
use once_cell::sync::OnceCell;
use reqwest::{Client, Error, Response, Url};
use serde::Deserialize;

static REST_CLIENT: OnceCell<MayastorApiClient> = OnceCell::new();

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
    ($name:ident, $t:ty, $urn:literal) => {
        pub async fn $name(&self) -> Result<Vec<$t>> {
            self.get_collection::<$t>($urn).await
        }
    };
}

impl MayastorApiClient {
    async fn get_collection<R>(&self, urn: &str) -> Result<Vec<R>>
    where
        for<'a> R: Deserialize<'a>,
    {
        let body = self
            .do_get(urn)
            .await
            .map_err(|e| anyhow!("Failed to list {}, error = {}", urn, e))?
            .bytes()
            .await
            .map_err(|e| {
                anyhow!(
                    "Failed to obtain body from HTTP response while listing {}, error = {}",
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

    async fn do_get(&self, urn: &str) -> Result<Response, Error> {
        let u = format!("{}{}", self.base_url, urn);
        let uri = Url::parse(&u).unwrap();
        self.rest_client.get(uri).send().await
    }

    collection_getter!(list_nodes, Node, "/nodes");
}
