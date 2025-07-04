use redis::Client;
use redis::aio::MultiplexedConnection;
use std::sync::Arc;
use tokio::sync::Mutex;

pub type FlowStore = Arc<Mutex<Box<MultiplexedConnection>>>;

pub fn build_connection(redis_url: String) -> Client {
    match Client::open(redis_url) {
        Ok(client) => {
            log::info!("Successfully created connection to the FlowStore (Redis)");
            client
        }
        Err(con_error) => {
            log::error!(
                "Cannot create FlowStoreClient (Redis) connection! Reason: {:?}",
                con_error
            );
            panic!("Failed to create Redis client")
        }
    }
}

pub async fn create_flow_store_connection(url: String) -> FlowStore {
    let client = match build_connection(url)
        .get_multiplexed_async_connection()
        .await
    {
        Ok(connection) => {
            log::info!("Successfully created connection to the FlowStore (Redis)");
            connection
        }
        Err(con_error) => {
            log::error!(
                "Cannot create FlowStoreClient (Redis) connection! Reason: {:?}",
                con_error
            );
            panic!("Failed to create Redis client")
        }
    };

    Arc::new(Mutex::new(Box::new(client)))
}

#[cfg(test)]
mod tests {
    use crate::flow_store::connection::create_flow_store_connection;
    use redis::{AsyncCommands, RedisResult};
    use serial_test::serial;
    use testcontainers::GenericImage;
    use testcontainers::core::IntoContainerPort;
    use testcontainers::core::WaitFor;
    use testcontainers::runners::AsyncRunner;

    macro_rules! redis_container_test {
        ($test_name:ident, $consumer:expr) => {
            #[tokio::test]
            #[serial]
            async fn $test_name() {
                let port: u16 = 6379;
                let image_name = "redis";
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

                $consumer(url).await;

                let _ = container.stop().await;
            }
        };
    }

    redis_container_test!(
        test_redis_startup,
        (|url: String| async move {
            println!("Redis server started correctly on: {}", url);
        })
    );

    redis_container_test!(
        test_redis_ping,
        (|url: String| async move {
            println!("Redis server started correctly on: {}", url.clone());

            let flow_store = create_flow_store_connection(url.clone()).await;
            let mut con = flow_store.lock().await;

            let ping_res: RedisResult<String> = con.ping().await;
            assert!(ping_res.is_ok());
            assert_eq!(ping_res.unwrap(), "PONG");
        })
    );
}