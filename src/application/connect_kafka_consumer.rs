use rdkafka::{ClientContext, Message};
use futures::StreamExt;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, StreamConsumer};

#[derive(Default)]
pub struct KafkaConsumerStore{
    pub app_name: String,
}

struct CustomConsumerContext;

impl ClientContext for CustomConsumerContext {}

impl ConsumerContext for CustomConsumerContext {}

impl KafkaConsumerStore {
    pub async fn connect_kafka(&mut self, group_id: String, app_name: String, topic_name: String, consumer_type: String, ) {
        self.app_name = app_name;

        let broker = "localhost:9092";

        let consumer: StreamConsumer<CustomConsumerContext> =
            rdkafka::config::ClientConfig::new()
                .set("group.id", &group_id)
                .set("bootstrap.servers", broker)
                .set("auto.offset.reset", "earliest")
                .set("enable.auto.commit", "false")
                .set("auto.commit.interval.ms", "1000")
                .create_with_context(CustomConsumerContext)
                .expect("Consumer creation failed");

        consumer.subscribe(&[topic_name.as_str()]).expect("Failed to subscribe to topic");
        println!("Kafka consumer started. Topic: {}, Group: {}", topic_name, group_id);

        let mut message_stream = consumer.stream();

        while let Some(result) = message_stream.next().await {
            match result {
                Ok(message) => {
                    if let Some(payload) = message.payload() {
                        let msg = String::from_utf8_lossy(payload);
                        println!("Kafka message received: {}", msg);
                        let data = msg.trim().to_string();
                        println!("Kafka data received: {}", data.to_string());
                        match consumer_type.as_str() {
                            "micro_consumer_1" => {
                                let processing_success = if data.is_empty() {false}else {true};
                                if processing_success {
                                    consumer.commit_message(&message, CommitMode::Sync).expect("Failed to commit message");
                                }
                            }
                            _ => {
                                consumer.commit_message(&message, CommitMode::Sync).expect("Failed to commit message");
                            }
                        }
                    }
                }

                Err(error) => {
                    println!("Kafka consumer error: {:?}", error);
                }
            }
        }
    }

}