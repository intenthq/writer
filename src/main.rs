use clap::{Arg, App};
use elasticsearch::{BulkParts, Elasticsearch, Error, http::{request::JsonBody, Url, transport::{SingleNodeConnectionPool, TransportBuilder}}};
use rand::{distributions::Alphanumeric, Rng, thread_rng};
use serde_json::{Value, json};

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
// #[derive(Debug, Deserialize, Serialize)]
// struct Message {
//   id: i32,
//   user: String,
//   message:  String
// }

fn generate_profile(id: i32) -> Value {
    json!({"id": id, "user": get_random_username(), "message": get_random_message()})
}

fn get_random_username() -> String {
  thread_rng()
  .sample_iter(&Alphanumeric)
  .take(10)
  .collect::<String>()
}

fn get_random_message() -> String {
  thread_rng()
  .sample_iter(&Alphanumeric)
  .take(30)
  .collect::<String>()
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

    let profiles_range: Vec<i32> = (1..1000001).collect();
    let batches = profiles_range.chunks(10000);

    for batch  in batches {
      let mut index_commands: Vec<JsonBody<Value>> = Vec::with_capacity(20000);
      for id in batch {
        index_commands.push(JsonBody::new(json!({"index": {"_index": "test-batch-size-10k-million","_id": *id, "_type": "_doc"}}).into()));
        index_commands.push(JsonBody::new(generate_profile(*id)))
      }
      //BulkParts::IndexType()
      let req = client.bulk(BulkParts::IndexType("test-batch-size-10k-million", "_doc")).pretty(true).human(true).body(index_commands);
      let before_response = req.send();
      let response = before_response.await?;
      match response.error_for_status_code(){
       Ok(_) => println!("no complaints"),
       Err(e) => println!("{}", e.to_string()),
      }
    } 

    println!("Host: {} Port: {}", elastic_search_config.host, elastic_search_config.port);  
    Ok(())
}
