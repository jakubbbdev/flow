use lapin::Connection;

pub async fn build_connection(rabbitmq_url: &str) -> Connection {
    match Connection::connect(rabbitmq_url, lapin::ConnectionProperties::default()).await {
        Ok(env) => env,
        Err(error) => {
            log::error!(
                "Cannot connect to FlowQueue (RabbitMQ) instance! Reason: {:?}",
                error
            );
            panic!("Cannot connect to FlowQueue (RabbitMQ) instance!");
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::flow_queue::connection::build_connection;
    use testcontainers::GenericImage;
    use testcontainers::core::{IntoContainerPort, WaitFor};
    use testcontainers::runners::AsyncRunner;

    macro_rules! rabbitmq_container_test {
        ($test_name:ident, $consumer:expr) => {
            #[tokio::test]
            async fn $test_name() {
                let port: u16 = 5672;
                let image_name = "rabbitmq";
                let wait_message = "Server startup complete";

                let container = GenericImage::new(image_name, "latest")
                    .with_exposed_port(port.tcp())
                    .with_wait_for(WaitFor::message_on_stdout(wait_message))
                    .start()
                    .await
                    .unwrap();

                let host_port = container.get_host_port_ipv4(port).await.unwrap();
                let url = format!("amqp://guest:guest@localhost:{}", host_port);

                $consumer(url).await;
            }
        };
    }

    rabbitmq_container_test!(
        test_rabbitmq_startup,
        (|url: String| async move {
            println!("RabbitMQ started with the url: {}", url);
        })
    );

    rabbitmq_container_test!(
        test_rabbitmq_connection,
        (|url: String| async move {
            build_connection(&*url).await;
        })
    );
}