use clap::{Arg, App};
use elasticsearch::{Elasticsearch, Error, IndexParts, http::{Url, transport::{SingleNodeConnectionPool, TransportBuilder}}};
use serde_json::json;

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

    let response = client.index(IndexParts::IndexId("test", "1")).body(json!({
            "id": 1,
            "user": "test",
            "message": "Trying out Elasticsearch, so far so good?"
        }))
        .refresh(elasticsearch::params::Refresh::WaitFor)
        .send().await?;


    let successful = response.status_code().to_string();
    println!("{}", successful);
    println!("Host: {} Port: {}", elastic_search_config.host, elastic_search_config.port); 
    Ok(())
}
