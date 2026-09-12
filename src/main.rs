use dotenvy::dotenv;
use lapin::types::ShortString;
use lapin::{
    BasicProperties,
    Connection,
    ConnectionProperties,
    options::{
        BasicPublishOptions,
        QueueDeclareOptions,
    },
    types::FieldTable,
};
use std::time::Duration;
use std::{env, process};

struct Config {
    rabbitmq_url: String,
    queue_name: String,
    message_count: usize,
    message_prefix: String,
}

#[tokio::main]
async fn main() {
    dotenv().ok();
    let config = load_config();

    let (conn, channel) = setup_rabbitmq(&config).await;

    let queue = declare_queue(&channel, &config.queue_name).await;

    send_messages(&channel, &queue.name().to_string(), &config).await;

    println!("Sent {} messages", config.message_count);

    drop(channel);
    tokio::time::sleep(Duration::from_secs(20)).await;
    if let Err(err) = conn.close(0, ShortString::from("Done")).await {
        eprintln!("Failed to close RabbitMQ connection: {err}");
    }
}

fn load_config() -> Config {
    Config {
        rabbitmq_url: get_env_or_fail("RABBITMQ_URL"),
        queue_name: get_env_or_fail("QUEUE_NAME"),
        message_count: get_env_as_int_or_fail("MESSAGE_COUNT"),
        message_prefix: get_env_or_default("MESSAGE_PREFIX", "Message"),
    }
}

fn get_env_or_fail(key: &str) -> String {
    match env::var(key) {
        Ok(value) if !value.is_empty() => value,
        _ => {
            eprintln!("{key} environment variable is not set");
            process::exit(1);
        }
    }
}

fn get_env_or_default(key: &str, default_value: &str) -> String {
    match env::var(key) {
        Ok(value) if !value.is_empty() => value,
        _ => default_value.to_string(),
    }
}

fn get_env_as_int_or_fail(key: &str) -> usize {
    let value = get_env_or_fail(key);

    match value.parse::<usize>() {
        Ok(value) => value,
        Err(err) => {
            eprintln!("Invalid {key} value: {err}");
            process::exit(1);
        }
    }
}

async fn setup_rabbitmq(config: &Config) -> (Connection, lapin::Channel) {
    let conn = match Connection::connect(
        &config.rabbitmq_url,
        ConnectionProperties::default(),
    )
        .await
    {
        Ok(conn) => conn,
        Err(err) => {
            eprintln!("Failed to connect to RabbitMQ: {err}");
            process::exit(1);
        }
    };

    let channel = match conn.create_channel().await {
        Ok(channel) => channel,
        Err(err) => {
            eprintln!("Failed to open a channel: {err}");
            process::exit(1);
        }
    };

    (conn, channel)
}

async fn declare_queue(
    channel: &lapin::Channel,
    queue_name: &str,
) -> lapin::Queue {

    match channel
        .queue_declare(
            ShortString::from(queue_name),
            QueueDeclareOptions {
                durable: true,
                exclusive: false,
                auto_delete: false,
                nowait: false,
                passive: false,
            },
            FieldTable::default(),
        )
        .await
    {
        Ok(queue) => queue,
        Err(err) => {
            eprintln!("Failed to declare queue: {err}");
            process::exit(1);
        }
    }
}

async fn send_messages(
    channel: &lapin::Channel,
    queue_name: &str,
    config: &Config,
) {
    for i in 1..=config.message_count {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let body = format!("#{}  count#{}", config.message_prefix, i);// produces a simple message #prefix count#
        let result = channel
            .basic_publish(
                ShortString::from(""),
                ShortString::from(queue_name),
                BasicPublishOptions::default(),
                body.as_bytes(),
                BasicProperties::default()
                    .with_content_type("text/plain".into()),
            )
            .await;

        match result {
            Ok(confirm) => {
                if let Err(err) = confirm.await {
                    eprintln!("Failed to confirm message: {err}");
                    process::exit(1);
                }

                println!("Sent {body}");
            }
            Err(err) => {
                eprintln!("Failed to publish message: {err}");
                process::exit(1);
            }
        }
    }
}
