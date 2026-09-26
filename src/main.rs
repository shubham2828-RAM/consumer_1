pub mod application;
use clap::Parser;
use crate::application::connect_kafka_consumer::KafkaConsumerStore;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// environment
    #[clap(short, long, default_value = "local")]
    env: String,

    #[clap(short, long, default_value = "cpaas_whatsapp")]
    app_type: String,
}

#[tokio::main]
async fn main() {
    println!("Hello, world!");

    // getting the command line argument
    let args = Args::parse();

    println!("Consumer initialized...");

    if &args.app_type == "consumer_1" {

        let mut kafka_cpaas_whatsapp_store = KafkaConsumerStore {
            app_name: "consumer_1".to_string()
        };
        kafka_cpaas_whatsapp_store.connect_kafka("micro_consumer_1".to_string(),"consumer_1".to_string(),"micro_consumer_1".to_string(),"micro_consumer_1".to_string()).await;
    }

}
