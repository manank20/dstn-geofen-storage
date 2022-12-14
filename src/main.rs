#[allow(unused_imports)]
use std::net::{SocketAddr, TcpListener, TcpStream};
use avro_rs::{Reader, from_avro_datum, Schema};

fn main() {
    
    let raw_schema = r#"
        {
            "type": "record",
            "namespace": "com.geofen",
            "name": "geofen",
            "fields": [
                {"name": "timestamp", "type": "long"},
                {"name": "device_id", "type": "string"},
                {"name": "latitude", "type": "double"},
                {"name": "longitude", "type": "double"},
                {"name": "altitude", "type": "double"}
            ]
        }
    "#;
    let schema = Schema::parse_str(raw_schema).unwrap();

    println!("Hello, DSTN!");
    let mut socket = TcpStream::connect("localhost:8000").unwrap();
    println!("Connected to server");
    // let socket = socket;

    // let reader = Reader::new(&mut socket).unwrap();
    
    loop {
        println!("Waiting for data");
        let record = Reader::with_schema(&schema, &mut socket).unwrap();/* from_avro_datum(&schema,result, None); */
        for result in record {
            println!("{:?}", result);
        }
    }
    /* for value in record {
        println!("{:?}", value);
    } */
    // println!("{:?}", record);
    
}
