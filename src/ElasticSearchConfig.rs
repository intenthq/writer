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

      ElasticSearchConfig {host: host.to_string(), port: port.to_string()};
  }
}