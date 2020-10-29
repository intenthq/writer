use clap::{Arg, App};
use elasticsearch::{BulkParts, Elasticsearch, Error, http::{request::JsonBody, Url, transport::{SingleNodeConnectionPool, TransportBuilder}}};
use serde::{ Serialize, Deserialize};
use std::slice::Chunks;
use rand::{distributions::Alphanumeric, Rng, thread_rng};

struct ElasticSearchConfig {
    host: String,
    port: String
  }
  
impl ElasticSearchConfig {
  fn new(app: App) -> Self { 
    let es_host_arg = Arg::with_name("elasticsearch-host").long("elasticsearch-host").help("Elasticsearch server host").takes_value(true).required(true);
    let es_port_arg = Arg::with_name("elasticsearch-port").long("elasticsearch-port").help("Elasticsearch server port").takes_value(true).required(true);
    
    let app = app.arg(es_host_arg);
    let app = app.arg(es_port_arg);
  
    let matches = app.get_matches();

    let host = matches.value_of("elasticsearch-host").expect("This can't be None!");
    let port = matches.value_of("elasticsearch-port").expect("This can't be None!");

    ElasticSearchConfig {host: host.to_string(), port: port.to_string()}
  }
}
#[derive(Debug, Deserialize, Serialize)]
struct Message {
  id: i32,
  user: String,
  message: String
}

fn generateProfile(id: i32) -> Message {
    Message {id: id, user: getRandomUsername(), message: getRandomMessage()}
}

fn getRandomUsername() -> String {
  thread_rng()
  .sample_iter(&Alphanumeric)
  .take(10)
  .collect()
}

fn getRandomMessage() -> String {
  thread_rng()
  .sample_iter(&Alphanumeric)
  .take(30)
  .collect()
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Initialize app and fetch command line arguments.
    let app = App::new("writer").version("0.1").about("Bombards Elastic Search with batches of consolidated profiles or user IDs").author("Andrei Maximilian Diamandopol");
    let elastic_search_config = ElasticSearchConfig::new(app);
    
    // Initialize elastic search client.
    let url_string = format!("http://{}:{}", elastic_search_config.host, elastic_search_config.port);
    let url = Url::parse(&url_string)?;
    let conn_pool = SingleNodeConnectionPool::new(url);
    let transport = TransportBuilder::new(conn_pool).disable_proxy().build()?;
    let client = Elasticsearch::new(transport);

    let profiles_range: Vec<i32> = (1..100000).collect();

    let batches: Chunks<Message> = profiles_range.into_iter()
    .map(|id| {generateProfile(id)})
    .collect::<Vec<Message>>()
    .chunks(1000);

    for batch  in batches {
      let mut p: Vec<JsonBody<Message>> = Vec::with_capacity(100);
      for item in batch {
        p.push(JsonBody::new(item));
      }
      let response = client.bulk(BulkParts::Index("test")).body(p).send().await?;
      let successful = response.status_code().to_string();
      println!("{}", successful);
    } 

    println!("Host: {} Port: {}", elastic_search_config.host, elastic_search_config.port);  
    Ok(())
}
