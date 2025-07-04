use std::{sync::Arc, time::Duration};

use futures_lite::StreamExt;
use lapin::{
    Channel,
    options::{BasicConsumeOptions, QueueDeclareOptions},
    types::FieldTable,
};
use log::debug;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use super::connection::build_connection;

#[derive(Serialize, Deserialize)]
pub enum MessageType {
    ExecuteFlow,
    TestExecuteFlow,
}

#[derive(Serialize, Deserialize)]
pub struct Sender {
    pub name: String,
    pub protocol: String,
    pub version: String,
}

#[derive(Serialize, Deserialize)]
pub struct Message {
    pub message_type: MessageType,
    pub sender: Sender,
    pub timestamp: i64,
    pub message_id: String,
    pub body: String,
}

pub struct RabbitmqClient {
    pub channel: Arc<Mutex<Channel>>,
}

#[derive(Debug)]
pub enum RabbitMqError {
    LapinError(lapin::Error),
    ConnectionError(String),
    TimeoutError,
    DeserializationError,
    SerializationError,
}

impl From<lapin::Error> for RabbitMqError {
    fn from(error: lapin::Error) -> Self {
        RabbitMqError::LapinError(error)
    }
}

impl From<std::io::Error> for RabbitMqError {
    fn from(error: std::io::Error) -> Self {
        RabbitMqError::ConnectionError(error.to_string())
    }
}

impl std::fmt::Display for RabbitMqError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RabbitMqError::LapinError(err) => write!(f, "RabbitMQ error: {}", err),
            RabbitMqError::ConnectionError(msg) => write!(f, "Connection error: {}", msg),
            RabbitMqError::TimeoutError => write!(f, "Operation timed out"),
            RabbitMqError::DeserializationError => write!(f, "Failed to deserialize message"),
            RabbitMqError::SerializationError => write!(f, "Failed to serialize message"),
        }
    }
}

impl RabbitmqClient {
    // Create a new RabbitMQ client with channel
    pub async fn new(rabbitmq_url: &str) -> Self {
        let connection = build_connection(rabbitmq_url).await;
        let channel = connection.create_channel().await.unwrap();

        match channel
            .queue_declare(
                "send_queue",
                QueueDeclareOptions::default(),
                FieldTable::default(),
            )
            .await
        {
            Ok(_) => {
                log::info!("Successfully declared send_queue");
            }
            Err(err) => log::error!("Failed to declare send_queue: {:?}", err),
        }

        match channel
            .queue_declare(
                "recieve_queue",
                QueueDeclareOptions::default(),
                FieldTable::default(),
            )
            .await
        {
            Ok(_) => {
                log::info!("Successfully declared recieve_queue");
            }
            Err(err) => log::error!("Failed to declare recieve_queue: {:?}", err),
        }

        RabbitmqClient {
            channel: Arc::new(Mutex::new(channel)),
        }
    }

    // Send message to the queue
    pub async fn send_message(
        &self,
        message_json: String,
        queue_name: &str,
    ) -> Result<(), RabbitMqError> {
        let channel = self.channel.lock().await;

        match channel
            .basic_publish(
                "",         // exchange
                queue_name, // routing key (queue name)
                lapin::options::BasicPublishOptions::default(),
                message_json.as_bytes(),
                lapin::BasicProperties::default(),
            )
            .await
        {
            Err(err) => {
                log::error!("Failed to publish message: {:?}", err);
                Err(RabbitMqError::LapinError(err))
            }
            Ok(_) => Ok(()),
        }
    }

    // Receive messages from a queue with no timeout
    pub async fn await_message_no_timeout(
        &self,
        queue_name: &str,
        message_id: String,
        ack_on_success: bool,
    ) -> Result<Message, RabbitMqError> {
        let mut consumer = {
            let channel = self.channel.lock().await;

            let consumer_res = channel
                .basic_consume(
                    queue_name,
                    "consumer",
                    lapin::options::BasicConsumeOptions::default(),
                    FieldTable::default(),
                )
                .await;

            match consumer_res {
                Ok(consumer) => {
                    log::info!("Established queue connection to {}", queue_name);
                    consumer
                }
                Err(err) => {
                    log::error!("Cannot create consumer for queue {}: {:?}", queue_name, err);
                    return Err(RabbitMqError::LapinError(err));
                }
            }
        };

        debug!("Starting to consume from {}", queue_name);

        while let Some(delivery_result) = consumer.next().await {
            let delivery = match delivery_result {
                Ok(del) => del,
                Err(_) => return Err(RabbitMqError::DeserializationError),
            };
            let data = &delivery.data;
            let message_str = match std::str::from_utf8(&data) {
                Ok(str) => str,
                Err(_) => {
                    return Err(RabbitMqError::DeserializationError);
                }
            };

            debug!("Received message: {}", message_str);

            // Parse the message
            let message = match serde_json::from_str::<Message>(message_str) {
                Ok(m) => m,
                Err(e) => {
                    log::error!("Failed to parse message: {:?}", e);
                    return Err(RabbitMqError::DeserializationError);
                }
            };

            if message.message_id == message_id {
                if ack_on_success {
                    if let Err(delivery_error) = delivery
                        .ack(lapin::options::BasicAckOptions::default())
                        .await
                    {
                        log::error!("Failed to acknowledge message: {:?}", delivery_error);
                    }
                }

                return Ok(message);
            }
        }
        Err(RabbitMqError::DeserializationError)
    }

    // Function intended to get used by the runtime
    pub async fn receive_messages(
        &self,
        queue_name: &str,
        handle_message: fn(Message) -> Result<Message, lapin::Error>,
    ) -> Result<(), RabbitMqError> {
        let mut consumer = {
            let channel = self.channel.lock().await;

            let consumer_res = channel
                .basic_consume(
                    queue_name,
                    "consumer",
                    BasicConsumeOptions::default(),
                    FieldTable::default(),
                )
                .await;

            match consumer_res {
                Ok(consumer) => {
                    log::info!("Established queue connection to {}", queue_name);
                    consumer
                }
                Err(err) => {
                    log::error!("Cannot create consumer for queue {}: {:?}", queue_name, err);
                    return Err(RabbitMqError::LapinError(err));
                }
            }
        };

        debug!("Starting to consume from {}", queue_name);

        while let Some(delivery) = consumer.next().await {
            let delivery = match delivery {
                Ok(del) => del,
                Err(err) => {
                    log::error!("Error receiving message: {:?}", err);
                    return Err(RabbitMqError::LapinError(err));
                }
            };

            let data = &delivery.data;
            let message_str = match std::str::from_utf8(&data) {
                Ok(str) => {
                    log::info!("Received message: {}", str);
                    str
                }
                Err(err) => {
                    log::error!("Error decoding message: {:?}", err);
                    return Err(RabbitMqError::DeserializationError);
                }
            };
            // Parse the message
            let inc_message = match serde_json::from_str::<Message>(message_str) {
                Ok(mess) => mess,
                Err(err) => {
                    log::error!("Error parsing message: {:?}", err);
                    return Err(RabbitMqError::DeserializationError);
                }
            };

            let message = match handle_message(inc_message) {
                Ok(mess) => mess,
                Err(err) => {
                    log::error!("Error handling message: {:?}", err);
                    return Err(RabbitMqError::DeserializationError);
                }
            };

            let message_json = match serde_json::to_string(&message) {
                Ok(json) => json,
                Err(err) => {
                    log::error!("Error serializing message: {:?}", err);
                    return Err(RabbitMqError::SerializationError);
                }
            };

            {
                let _ = self.send_message(message_json, "recieve_queue").await;
            }

            // Acknowledge the message
            if let Err(delivery_error) = delivery
                .ack(lapin::options::BasicAckOptions::default())
                .await
            {
                log::error!("Failed to acknowledge message: {:?}", delivery_error);
            }
        }

        Ok(())
    }

    // Receive messages from a queue with timeout
    pub async fn await_message(
        &self,
        queue_name: &str,
        message_id: String,
        timeout: Duration,
        ack_on_success: bool,
    ) -> Result<Message, RabbitMqError> {
        // Set a timeout
        match tokio::time::timeout(
            timeout,
            self.await_message_no_timeout(queue_name, message_id, ack_on_success),
        )
            .await
        {
            Ok(result) => result,
            Err(_) => {
                debug!(
                    "Timeout waiting for message after {} seconds",
                    timeout.as_secs()
                );
                Err(RabbitMqError::TimeoutError)
            }
        }
    }
}