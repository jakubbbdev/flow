use super::flow_identifier;
use crate::flow_store::connection::FlowStore;
use async_trait::async_trait;
use log::error;
use redis::{AsyncCommands, JsonAsyncCommands, RedisError, RedisResult};
use tucana::shared::{Flows, ValidationFlow};

#[derive(Debug)]
pub struct FlowStoreError {
    pub kind: FlowStoreErrorKind,
    pub flow_id: i64,
    pub reason: String,
}

#[derive(Debug)]
pub enum FlowStoreErrorKind {
    Serialization,
    RedisOperation,
    NoIdentifier,
}

/// Trait representing a service for managing flows in a Redis.
#[async_trait]
pub trait FlowStoreServiceBase {
    async fn new(redis_client_arc: FlowStore) -> Self;
    async fn insert_flow(&mut self, flow: ValidationFlow) -> Result<i64, FlowStoreError>;
    async fn insert_flows(&mut self, flows: Flows) -> Result<i64, FlowStoreError>;
    async fn delete_flow(&mut self, flow_id: i64) -> Result<i64, RedisError>;
    async fn delete_flows(&mut self, flow_ids: Vec<i64>) -> Result<i64, RedisError>;
    async fn get_all_flow_ids(&mut self) -> Result<Vec<i64>, RedisError>;
    async fn query_flows(&mut self, pattern: String) -> Result<Flows, FlowStoreError>;
}

/// Struct representing a service for managing flows in a Redis.
#[derive(Clone)]
pub struct FlowStoreService {
    pub(crate) redis_client_arc: FlowStore,
}

/// Implementation of a service for managing flows in a Redis.
#[async_trait]
impl FlowStoreServiceBase for FlowStoreService {
    async fn new(redis_client_arc: FlowStore) -> FlowStoreService {
        FlowStoreService { redis_client_arc }
    }

    /// Insert a list of flows into Redis
    async fn insert_flow(&mut self, flow: ValidationFlow) -> Result<i64, FlowStoreError> {
        let mut connection = self.redis_client_arc.lock().await;

        let identifier = match flow_identifier::get_flow_identifier(&flow) {
            Some(id) => id,
            None => {
                return Err(FlowStoreError {
                    kind: FlowStoreErrorKind::NoIdentifier,
                    flow_id: flow.flow_id,
                    reason: String::from("Identifier can't be determent!"),
                });
            }
        };

        let insert_result: RedisResult<()> = connection.json_set(identifier, "$", &flow).await;

        match insert_result {
            Err(redis_error) => {
                error!("An Error occurred {}", redis_error);
                Err(FlowStoreError {
                    flow_id: flow.flow_id,
                    kind: FlowStoreErrorKind::RedisOperation,
                    reason: redis_error.to_string(),
                })
            }
            _ => Ok(1),
        }
    }

    /// Insert a flows into Redis
    async fn insert_flows(&mut self, flows: Flows) -> Result<i64, FlowStoreError> {
        let mut total_modified = 0;

        for flow in flows.flows {
            let result = self.insert_flow(flow).await?;
            total_modified += result;
        }

        Ok(total_modified)
    }

    /// Deletes a flow
    async fn delete_flow(&mut self, flow_id: i64) -> Result<i64, RedisError> {
        let mut connection = self.redis_client_arc.lock().await;

        let identifier = format!("{}::*", flow_id);
        let keys: Vec<String> = connection.keys(&identifier).await?;
        let deleted_flow: RedisResult<i64> = connection.json_del(keys, ".").await;

        match deleted_flow {
            Ok(int) => Ok(int),
            Err(redis_error) => {
                error!("An Error occurred {}", redis_error);
                Err(redis_error)
            }
        }
    }

    /// Deletes a list of flows
    async fn delete_flows(&mut self, flow_ids: Vec<i64>) -> Result<i64, RedisError> {
        let mut total_modified = 0;

        for id in flow_ids {
            let result = self.delete_flow(id).await?;
            total_modified += result;
        }

        Ok(total_modified)
    }

    /// Queries for all ids in the redis
    /// Returns `Result<Vec<i64>, RedisError>`: Result of the flow ids currently in Redis
    async fn get_all_flow_ids(&mut self) -> Result<Vec<i64>, RedisError> {
        let mut connection = self.redis_client_arc.lock().await;

        let string_keys: Vec<String> = {
            match connection.keys("*").await {
                Ok(res) => res,
                Err(error) => {
                    error!("Can't retrieve keys from redis. Reason: {error}");
                    return Err(error);
                }
            }
        };

        let mut real_keys: Vec<String> = vec![];

        for key in string_keys {
            if key.contains("::") {
                let number = key.splitn(2, "::").next();
                if let Some(real_number) = number {
                    real_keys.push(String::from(real_number));
                }
            }
        }

        let int_keys: Vec<i64> = real_keys
            .into_iter()
            .filter_map(|key| key.parse::<i64>().ok())
            .collect();

        Ok(int_keys)
    }

    async fn query_flows(&mut self, pattern: String) -> Result<Flows, FlowStoreError> {
        let mut connection = self.redis_client_arc.lock().await;

        let keys: Vec<String> = {
            match connection.keys(pattern).await {
                Ok(res) => res,
                Err(error) => {
                    error!("Can't retrieve keys from redis. Reason: {error}");
                    return Err(FlowStoreError {
                        kind: FlowStoreErrorKind::RedisOperation,
                        flow_id: 0,
                        reason: error.detail().unwrap().to_string(),
                    });
                }
            }
        };

        if keys.is_empty() {
            return Ok(Flows { flows: vec![] });
        }

        match connection
            .json_get::<Vec<String>, &str, Vec<String>>(keys, "$")
            .await
        {
            Ok(json_values) => {
                let mut all_flows: Vec<ValidationFlow> = Vec::new();

                for json_str in json_values {
                    match serde_json::from_str::<Vec<ValidationFlow>>(&json_str) {
                        Ok(mut flows) => all_flows.append(&mut flows),
                        Err(error) => {
                            return Err(FlowStoreError {
                                kind: FlowStoreErrorKind::Serialization,
                                flow_id: 0,
                                reason: error.to_string(),
                            });
                        }
                    }
                }

                return Ok(Flows { flows: all_flows });
            }
            Err(error) => {
                return Err(FlowStoreError {
                    kind: FlowStoreErrorKind::RedisOperation,
                    flow_id: 0,
                    reason: error.detail().unwrap_or("Unknown Redis error").to_string(),
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::flow_store::connection::FlowStore;
    use crate::flow_store::connection::create_flow_store_connection;
    use crate::flow_store::service::FlowStoreService;
    use crate::flow_store::service::FlowStoreServiceBase;
    use redis::{AsyncCommands, JsonAsyncCommands};
    use serial_test::serial;
    use testcontainers::GenericImage;
    use testcontainers::core::IntoContainerPort;
    use testcontainers::core::WaitFor;
    use testcontainers::runners::AsyncRunner;
    use tucana::shared::FlowSetting;
    use tucana::shared::Struct;
    use tucana::shared::{Flows, ValidationFlow};

    fn get_string_value(value: &str) -> tucana::shared::Value {
        tucana::shared::Value {
            kind: Some(tucana::shared::value::Kind::StringValue(String::from(
                value,
            ))),
        }
    }

    fn get_settings() -> Vec<FlowSetting> {
        vec![
            FlowSetting {
                database_id: 1234567,
                flow_setting_id: String::from("HTTP_HOST"),
                object: Some(Struct {
                    fields: {
                        let mut map = HashMap::new();
                        map.insert(String::from("host"), get_string_value("abc.code0.tech"));
                        map
                    },
                }),
            },
            FlowSetting {
                database_id: 14245252352,
                flow_setting_id: String::from("HTTP_METHOD"),
                object: Some(Struct {
                    fields: {
                        let mut map = HashMap::new();
                        map.insert(String::from("method"), get_string_value("GET"));
                        map
                    },
                }),
            },
        ]
    }

    macro_rules! redis_integration_test {
        ($test_name:ident, $consumer:expr) => {
            #[tokio::test]
            #[serial]
            async fn $test_name() {
                let port: u16 = 6379;
                let image_name = "redis/redis-stack";
                let wait_message = "Ready to accept connections";

                let container = GenericImage::new(image_name, "latest")
                    .with_exposed_port(port.tcp())
                    .with_wait_for(WaitFor::message_on_stdout(wait_message))
                    .start()
                    .await
                    .unwrap();

                let host = container.get_host().await.unwrap();
                let host_port = container.get_host_port_ipv4(port).await.unwrap();
                let url = format!("redis://{host}:{host_port}");
                println!("Redis server started correctly on: {}", url.clone());

                let connection = create_flow_store_connection(url).await;

                {
                    let mut con = connection.lock().await;

                    let _: () = redis::cmd("FLUSHALL")
                        .query_async(&mut **con)
                        .await
                        .expect("FLUSHALL command failed");
                }

                let base = FlowStoreService::new(connection.clone()).await;

                $consumer(connection, base).await;
                let _ = container.stop().await;
            }
        };
    }

    redis_integration_test!(
        insert_one_flow,
        (|connection: FlowStore, mut service: FlowStoreService| async move {
            let flow = ValidationFlow {
                flow_id: 1,
                r#type: "REST".to_string(),
                settings: get_settings(),
                starting_node: None,
                data_types: vec![],
                input_type_identifier: None,
                return_type_identifier: None,
                project_id: 1,
            };

            match service.insert_flow(flow.clone()).await {
                Ok(i) => println!("{}", i),
                Err(err) => println!("{}", err.reason),
            };

            let redis_result: Option<String> = {
                let mut redis_cmd = connection.lock().await;
                redis_cmd
                    .json_get("1::1::REST::abc.code0.tech::GET", "$")
                    .await
                    .unwrap()
            };

            println!("{}", redis_result.clone().unwrap());

            assert!(redis_result.is_some());
            let redis_flow: Vec<ValidationFlow> =
                serde_json::from_str(&*redis_result.unwrap()).unwrap();
            assert_eq!(redis_flow[0], flow);
        })
    );

    redis_integration_test!(
        insert_one_flow_fails_no_identifier,
        (|_connection: FlowStore, mut service: FlowStoreService| async move {
            let flow = ValidationFlow {
                flow_id: 1,
                r#type: "".to_string(),
                settings: get_settings(),
                starting_node: None,
                data_types: vec![],
                input_type_identifier: None,
                return_type_identifier: None,
                project_id: 1,
            };

            assert!(!service.insert_flow(flow.clone()).await.is_ok());
        })
    );

    redis_integration_test!(
        insert_will_overwrite_existing_flow,
        (|connection: FlowStore, mut service: FlowStoreService| async move {
            let flow = ValidationFlow {
                flow_id: 1,
                r#type: "REST".to_string(),
                settings: get_settings(),
                data_types: vec![],
                input_type_identifier: None,
                return_type_identifier: None,
                project_id: 1,
                starting_node: None,
            };

            match service.insert_flow(flow.clone()).await {
                Ok(i) => println!("{}", i),
                Err(err) => println!("{}", err.reason),
            };

            let flow_overwrite = ValidationFlow {
                flow_id: 1,
                r#type: "REST".to_string(),
                settings: get_settings(),
                data_types: vec![],
                input_type_identifier: Some(String::from("ABC")),
                return_type_identifier: None,
                project_id: 1,
                starting_node: None,
            };

            let _ = service.insert_flow(flow_overwrite).await;
            let amount = service.get_all_flow_ids().await;
            assert_eq!(amount.unwrap().len(), 1);

            let redis_result: Vec<String> = {
                let mut redis_cmd = connection.lock().await;
                redis_cmd
                    .json_get("1::1::REST::abc.code0.tech::GET", "$")
                    .await
                    .unwrap()
            };

            assert_eq!(redis_result.len(), 1);
            let string: &str = &*redis_result[0];
            let redis_flow: Vec<ValidationFlow> = serde_json::from_str(string).unwrap();
            assert!(redis_flow[0].r#input_type_identifier.is_some());
        })
    );

    redis_integration_test!(
        insert_many_flows,
        (|_connection: FlowStore, mut service: FlowStoreService| async move {
            let flow_one = ValidationFlow {
                flow_id: 1,
                r#type: "REST".to_string(),
                settings: get_settings(),
                data_types: vec![],
                input_type_identifier: None,
                return_type_identifier: None,
                project_id: 1,
                starting_node: None,
            };

            let flow_two = ValidationFlow {
                flow_id: 2,
                r#type: "REST".to_string(),
                settings: get_settings(),
                data_types: vec![],
                input_type_identifier: None,
                return_type_identifier: None,
                project_id: 1,
                starting_node: None,
            };

            let flow_three = ValidationFlow {
                flow_id: 3,
                r#type: "REST".to_string(),
                settings: get_settings(),
                starting_node: None,
                data_types: vec![],
                input_type_identifier: None,
                return_type_identifier: None,
                project_id: 1,
            };

            let flow_vec = vec![flow_one.clone(), flow_two.clone(), flow_three.clone()];
            let flows = Flows { flows: flow_vec };

            let amount = service.insert_flows(flows).await.unwrap();
            assert_eq!(amount, 3);
        })
    );

    redis_integration_test!(
        delete_one_existing_flow,
        (|connection: FlowStore, mut service: FlowStoreService| async move {
            let flow = ValidationFlow {
                flow_id: 1,
                r#type: "REST".to_string(),
                settings: get_settings(),
                starting_node: None,
                data_types: vec![],
                input_type_identifier: None,
                return_type_identifier: None,
                project_id: 1,
            };

            match service.insert_flow(flow.clone()).await {
                Ok(i) => println!("{}", i),
                Err(err) => println!("{}", err.reason),
            };

            let result = service.delete_flow(1).await;

            assert_eq!(result.unwrap(), 1);

            let redis_result: Option<String> = {
                let mut redis_cmd = connection.lock().await;
                redis_cmd.get("1").await.unwrap()
            };

            assert!(redis_result.is_none());
        })
    );

    redis_integration_test!(
        delete_one_non_existing_flow,
        (|_connection: FlowStore, mut service: FlowStoreService| async move {
            let result = service.delete_flow(1).await;
            assert_eq!(result.unwrap(), 0);
        })
    );

    redis_integration_test!(
        delete_many_existing_flows,
        (|_connection: FlowStore, mut service: FlowStoreService| async move {
            let flow_one = ValidationFlow {
                flow_id: 1,
                r#type: "REST".to_string(),
                settings: get_settings(),
                starting_node: None,
                data_types: vec![],
                input_type_identifier: None,
                return_type_identifier: None,
                project_id: 1,
            };

            let flow_two = ValidationFlow {
                flow_id: 2,
                r#type: "REST".to_string(),
                settings: get_settings(),
                starting_node: None,
                data_types: vec![],
                input_type_identifier: None,
                return_type_identifier: None,
                project_id: 1,
            };

            let flow_three = ValidationFlow {
                flow_id: 3,
                r#type: "REST".to_string(),
                settings: get_settings(),
                starting_node: None,
                data_types: vec![],
                input_type_identifier: None,
                return_type_identifier: None,
                project_id: 1,
            };

            let flow_vec = vec![flow_one.clone(), flow_two.clone(), flow_three.clone()];
            let flows = Flows { flows: flow_vec };

            let amount = service.insert_flows(flows).await.unwrap();
            assert_eq!(amount, 3);

            let deleted_amount = service.delete_flows(vec![1, 2, 3]).await;
            assert_eq!(deleted_amount.unwrap(), 3);
        })
    );

    redis_integration_test!(
        delete_many_non_existing_flows,
        (|_connection: FlowStore, mut service: FlowStoreService| async move {
            let deleted_amount = service.delete_flows(vec![1, 2, 3]).await;
            assert_eq!(deleted_amount.unwrap(), 0);
        })
    );

    redis_integration_test!(
        get_existing_flow_ids,
        (|_connection: FlowStore, mut service: FlowStoreService| async move {
            let flow_one = ValidationFlow {
                flow_id: 1,
                r#type: "REST".to_string(),
                settings: get_settings(),
                starting_node: None,
                data_types: vec![],
                input_type_identifier: None,
                return_type_identifier: None,
                project_id: 1,
            };

            let flow_two = ValidationFlow {
                flow_id: 2,
                r#type: "REST".to_string(),
                settings: get_settings(),
                starting_node: None,
                data_types: vec![],
                input_type_identifier: None,
                return_type_identifier: None,
                project_id: 1,
            };

            let flow_three = ValidationFlow {
                flow_id: 3,
                r#type: "REST".to_string(),
                settings: get_settings(),
                starting_node: None,
                data_types: vec![],
                input_type_identifier: None,
                return_type_identifier: None,
                project_id: 1,
            };

            let flow_vec = vec![flow_one.clone(), flow_two.clone(), flow_three.clone()];
            let flows = Flows { flows: flow_vec };

            let amount = service.insert_flows(flows).await.unwrap();
            assert_eq!(amount, 3);

            let mut flow_ids = service.get_all_flow_ids().await.unwrap();
            flow_ids.sort();

            assert_eq!(flow_ids, vec![1, 2, 3]);
        })
    );

    redis_integration_test!(
        get_empty_flow_ids,
        (|_connection: FlowStore, mut service: FlowStoreService| async move {
            let flow_ids = service.get_all_flow_ids().await;
            assert_eq!(flow_ids.unwrap(), Vec::<i64>::new());
        })
    );

    redis_integration_test!(
        query_empty_flow_store,
        (|_connection: FlowStore, mut service: FlowStoreService| async move {
            let flows = service.query_flows(String::from("*")).await;
            assert!(flows.is_ok());
            assert!(flows.unwrap().flows.is_empty());
        })
    );

    redis_integration_test!(
        query_all_flows,
        (|_connection: FlowStore, mut service: FlowStoreService| async move {
            let flow_one = ValidationFlow {
                flow_id: 1,
                r#type: "REST".to_string(),
                settings: get_settings(),
                starting_node: None,
                data_types: vec![],
                input_type_identifier: None,
                return_type_identifier: None,
                project_id: 1,
            };

            let flow_two = ValidationFlow {
                flow_id: 2,
                r#type: "REST".to_string(),
                settings: get_settings(),
                starting_node: None,
                data_types: vec![],
                input_type_identifier: None,
                return_type_identifier: None,
                project_id: 1,
            };

            let flow_three = ValidationFlow {
                flow_id: 3,
                r#type: "REST".to_string(),
                settings: get_settings(),
                starting_node: None,
                data_types: vec![],
                input_type_identifier: None,
                return_type_identifier: None,
                project_id: 1,
            };

            let flows = service.query_flows(String::from("*")).await;
            assert!(flows.is_ok());
            assert!(flows.unwrap().flows.is_empty());

            let flow_vec = vec![flow_one.clone(), flow_two.clone(), flow_three.clone()];
            let flows = Flows { flows: flow_vec };

            let amount = service.insert_flows(flows.clone()).await.unwrap();
            assert_eq!(amount, 3);

            let query_flows = service.query_flows(String::from("*")).await;

            println!("{:?}", &query_flows);

            assert!(query_flows.is_ok());

            assert_eq!(flows.flows.len(), query_flows.unwrap().flows.len())
        })
    );

    redis_integration_test!(
        query_one_existing_flow,
        (|_connection: FlowStore, mut service: FlowStoreService| async move {
            let flow_one = ValidationFlow {
                flow_id: 1,
                r#type: "REST".to_string(),
                settings: get_settings(),
                starting_node: None,
                data_types: vec![],
                input_type_identifier: None,
                return_type_identifier: None,
                project_id: 1,
            };

            let flow_two = ValidationFlow {
                flow_id: 2,
                r#type: "REST".to_string(),
                settings: get_settings(),
                starting_node: None,
                data_types: vec![],
                input_type_identifier: None,
                return_type_identifier: None,
                project_id: 1,
            };

            let flow_three = ValidationFlow {
                flow_id: 3,
                r#type: "REST".to_string(),
                settings: get_settings(),
                starting_node: None,
                data_types: vec![],
                input_type_identifier: None,
                return_type_identifier: None,
                project_id: 1,
            };

            let flows = service.query_flows(String::from("*")).await;
            assert!(flows.is_ok());
            assert!(flows.unwrap().flows.is_empty());

            let flow_vec = vec![flow_one.clone(), flow_two.clone(), flow_three.clone()];
            let flows = Flows { flows: flow_vec };

            let amount = service.insert_flows(flows.clone()).await.unwrap();
            assert_eq!(amount, 3);

            let query_flows = service.query_flows(String::from("1::*")).await;

            assert!(query_flows.is_ok());
            assert_eq!(query_flows.unwrap().flows, vec![flow_one])
        })
    );
}