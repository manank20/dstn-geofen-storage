#[allow(unused_imports)]
use std::net::{ TcpListener, TcpStream};
use avro_rs::{Reader, Schema};
use std::fs::File;
use std::thread;
use crossbeam_queue::SegQueue;
use chrono::prelude::*;
use std::io::prelude::*;


const RAW_SCHEMA: &str = r#"
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

const IP_ADDRESS: &str = "localhost:8000";


#[tokio::main]
async fn main() -> ! {

    let schema = Schema::parse_str(RAW_SCHEMA).unwrap();

    let mut socket = TcpStream::connect(IP_ADDRESS).unwrap();
    println!("Connected to server");

    let avro_queue: SegQueue<Vec<(String, avro_rs::types::Value)>> = SegQueue::new();

    let mut filename = init_file();

    thread::scope(|s| {
        s.spawn(|| {
            loop{
                if let Some(x) = avro_queue.pop(){
                    println!("Writing to file {:?} : {:?}",filename, x);
                    let mut ss = "".to_string();
                    for val in x{
                        match val.1{
                            avro_rs::types::Value::Double(x) => ss.push_str(&format!(",{}", x)),
                            avro_rs::types::Value::Long(x) => ss.push_str(&format!("{}", x)),
                            avro_rs::types::Value::String(x) => ss.push_str(&format!(",{}", x)),
                            _ => (),
                        }
                    }
                    ss.push_str("\n");
                    filename.write_all(ss.as_bytes()).unwrap();
                }
            }
        });

        s.spawn(|| {
            let mut reader = Reader::with_schema(&schema, &mut socket).unwrap();
            loop{
                for result in &mut reader {
                    let record = result.unwrap();
                    match record {
                        avro_rs::types::Value::Record(x) => {
                            avro_queue.push(x);
                        },

                        _ => {
                            println!("Error reading record");
                        }
                    }
                }
            }
        });
    });

    loop{}

}

fn init_file() -> File{
    let bind = Local::now().to_string();
    let path = std::path::Path::new(bind.as_str());
    let mut file = File::create(path).unwrap();
    file.write_all(b"timestamp,device_id,latitude,longitude,altitude\n").unwrap();
    file
}
