use teaql_core::{EntityDescriptor, Record, Value};
use teaql_data_service::{
    DataServiceCapabilities, DataServiceExecutor, DataServiceOperation, ExecutionMetadata,
    MutationExecutor, MutationRequest, MutationResult, QueryExecutor, QueryRequest, QueryResult,
};

#[derive(Debug)]
pub struct MeilisearchError(pub String);

impl std::fmt::Display for MeilisearchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Meilisearch Error: {}", self.0)
    }
}

impl std::error::Error for MeilisearchError {}

#[derive(Clone)]
pub struct MeilisearchProvider {
    client: reqwest::Client,
    host: String,
    api_key: Option<String>,
}

impl MeilisearchProvider {
    pub fn new(host: impl Into<String>, api_key: Option<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            host: host.into(),
            api_key,
        }
    }

    fn request_builder(&self, method: reqwest::Method, url: &str) -> reqwest::RequestBuilder {
        let mut builder = self.client.request(method, url);
        if let Some(key) = &self.api_key {
            builder = builder.header("Authorization", format!("Bearer {}", key));
        }
        builder
    }

    pub async fn sync_schema(
        &self,
        _descriptor: &EntityDescriptor,
    ) -> Result<(), MeilisearchError> {
        Ok(())
    }
}

impl DataServiceExecutor for MeilisearchProvider {
    type Error = MeilisearchError;

    fn capabilities(&self) -> DataServiceCapabilities {
        DataServiceCapabilities::default()
    }
}

fn json_to_value(json: serde_json::Value) -> Value {
    match json {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(b),
        serde_json::Value::Number(n) => n
            .as_i64()
            .map(Value::I64)
            .or_else(|| n.as_f64().map(Value::F64))
            .unwrap_or(Value::Null),
        serde_json::Value::String(s) => Value::Text(s),
        serde_json::Value::Array(arr) => Value::List(arr.into_iter().map(json_to_value).collect()),
        serde_json::Value::Object(map) => {
            let mut record = Record::new();
            for (k, v) in map {
                record.insert(k, json_to_value(v));
            }
            Value::Object(record)
        }
    }
}

impl QueryExecutor for MeilisearchProvider {
    async fn query(&self, request: QueryRequest) -> Result<QueryResult, Self::Error> {
        let started_at = std::time::SystemTime::now();
        let entity = request.query.entity.clone();
        let search = request.query.search_with_text.clone().unwrap_or_default();

        let url = format!("{}/indexes/{}/search", self.host, entity);
        let payload = serde_json::json!({
            "q": search,
            "limit": 100
        });

        println!(
            "[MeilisearchProvider] Sending HTTP SEARCH to {} with payload: {}",
            url,
            serde_json::to_string_pretty(&payload).unwrap_or_default()
        );

        let res = self
            .request_builder(reqwest::Method::POST, &url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| MeilisearchError(e.to_string()))?;

        if !res.status().is_success() {
            let err = res.text().await.unwrap_or_default();
            return Err(MeilisearchError(format!("Search failed: {}", err)));
        }

        let json: serde_json::Value = res
            .json()
            .await
            .map_err(|e| MeilisearchError(e.to_string()))?;

        let mut rows = Vec::new();
        if let Some(hits) = json.get("hits").and_then(|h| h.as_array()) {
            for hit in hits {
                if let serde_json::Value::Object(map) = hit {
                    let mut record = Record::new();
                    for (k, v) in map {
                        record.insert(k.clone(), json_to_value(v.clone()));
                    }
                    rows.push(record);
                }
            }
        }

        Ok(QueryResult {
            rows,
            metadata: ExecutionMetadata {
                debug_query: Some(format!("POST {} with {:?}", url, payload)),
                backend: "meilisearch".to_owned(),
                operation: DataServiceOperation::Query,
                started_at,
                ended_at: std::time::SystemTime::now(),
                affected_rows: None,
                result_count: Some(
                    json.get("hits")
                        .and_then(|h| h.as_array())
                        .map(|a| a.len())
                        .unwrap_or(0),
                ),
                trace_chain: Vec::new(),
                comment: None,
                backend_request_id: None,
            },
        })
    }
}

impl MutationExecutor for MeilisearchProvider {
    async fn mutate(&self, request: MutationRequest) -> Result<MutationResult, Self::Error> {
        let started_at = std::time::SystemTime::now();

        match request {
            MutationRequest::Insert(cmd) => {
                let entity = cmd.entity.clone();
                let url = format!("{}/indexes/{}/documents?primaryKey=id", self.host, entity);

                let mut map = serde_json::Map::new();
                for (k, v) in &cmd.values {
                    map.insert(k.clone(), v.to_json_value());
                }
                let payload = serde_json::Value::Array(vec![serde_json::Value::Object(map)]);

                println!(
                    "[MeilisearchProvider] Sending HTTP POST to {} with payload: {}",
                    url,
                    serde_json::to_string_pretty(&payload).unwrap_or_default()
                );

                let res = self
                    .request_builder(reqwest::Method::POST, &url)
                    .json(&payload)
                    .send()
                    .await
                    .map_err(|e| MeilisearchError(e.to_string()))?;

                if !res.status().is_success() {
                    let err = res.text().await.unwrap_or_default();
                    return Err(MeilisearchError(format!("Insert failed: {}", err)));
                }

                Ok(MutationResult {
                    affected_rows: 1,
                    generated_values: Record::new(),
                    metadata: ExecutionMetadata {
                        debug_query: Some(format!("POST {} to Meilisearch", entity)),
                        backend: "meilisearch".to_owned(),
                        operation: DataServiceOperation::Update,
                        started_at,
                        ended_at: std::time::SystemTime::now(),
                        affected_rows: Some(1),
                        result_count: None,
                        trace_chain: Vec::new(),
                        comment: None,
                        backend_request_id: None,
                    },
                })
            }
            _ => {
                // Ignore other mutations or return an error depending on strictness
                Ok(MutationResult {
                    affected_rows: 0,
                    generated_values: Record::new(),
                    metadata: ExecutionMetadata {
                        debug_query: Some("Skipped non-insert mutation".to_owned()),
                        backend: "meilisearch".to_owned(),
                        operation: DataServiceOperation::Update,
                        started_at,
                        ended_at: std::time::SystemTime::now(),
                        affected_rows: Some(0),
                        result_count: None,
                        trace_chain: Vec::new(),
                        comment: None,
                        backend_request_id: None,
                    },
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_recursive_json_to_value_conversion_for_meilisearch() {
        let json = serde_json::json!({
            "null_val": null,
            "bool_val": true,
            "int_val": 42,
            "float_val": 1.23,
            "str_val": "hello",
            "arr_val": [1, "two", null],
            "obj_val": {
                "nested": "value"
            }
        });

        let val = json_to_value(json);
        match val {
            Value::Object(record) => {
                assert_eq!(record.get("null_val"), Some(&Value::Null));
                assert_eq!(record.get("bool_val"), Some(&Value::Bool(true)));
                assert_eq!(record.get("int_val"), Some(&Value::I64(42)));
                assert_eq!(record.get("float_val"), Some(&Value::F64(1.23)));
                assert_eq!(
                    record.get("str_val"),
                    Some(&Value::Text("hello".to_string()))
                );

                let arr = record.get("arr_val").unwrap();
                if let Value::List(list) = arr {
                    assert_eq!(list.len(), 3);
                    assert_eq!(list[0], Value::I64(1));
                    assert_eq!(list[1], Value::Text("two".to_string()));
                    assert_eq!(list[2], Value::Null);
                } else {
                    panic!("Expected List");
                }

                let obj = record.get("obj_val").unwrap();
                if let Value::Object(nested) = obj {
                    assert_eq!(
                        nested.get("nested"),
                        Some(&Value::Text("value".to_string()))
                    );
                } else {
                    panic!("Expected Object");
                }
            }
            _ => panic!("Expected Object"),
        }
    }
}
