use clap::{Arg, App};

fn main() {
    let app = App::new("writer").version("0.1").about("Bombards Elastic Search with batches of consolidated profiles or user IDs").author("Andrei Maximilian Diamandopol");

    let es_host_arg = Arg::with_name("elasticsearch-host").long("elasticsearch-host").help("Elasticsearch server host").takes_value(true).required(true);
    let es_port_arg = Arg::with_name("elasticsearch-port").long("elasticsearch-port").help("Elasticsearch server port").takes_value(true).required(true);
    
    let app = app.arg(es_host_arg);
    let app = app.arg(es_port_arg);

    let matches = app.get_matches();

    let host = matches.value_of("elasticsearch-host").expect("This can't be None!");
    let port = matches.value_of("elasticsearch-port").expect("This can't be None!");
    println!("Host: {} Port: {}", host, port);
}
