#[allow(unused_imports)]
use std::net::{ TcpListener, TcpStream};
use avro_rs::{Reader, Schema};
use chrono::format::StrftimeItems;
use std::fs::{File, self};
use std::thread;
use crossbeam_queue::SegQueue;
use chrono::{Duration, Days, prelude::*};
use tokio::time::sleep;
use std::io::prelude::*;
// use std::io::Write; use std::sync::Arc;


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

    let avro_queue = SegQueue::new();

    let mut filename = init_file();

    thread::scope(|s| {
        s.spawn(|| {
            loop{
                if let Some(x) = avro_queue.pop(){
                    let x = format!("{:?}\n", x);
                    println!("Writing to file {:?} : {}",filename, x);
                    filename.write_all(x.as_bytes()).unwrap();
                    println!("{:?}",x);
                }
            }
        });

        s.spawn(|| {
            let mut reader = Reader::with_schema(&schema, &mut socket).unwrap();
            loop{
                for result in &mut reader {
                    let record = result.unwrap();
                    avro_queue.push(record);
                }
            }
        });
    });

    // loop{
    //     thread::sleep(Duration::seconds(10).to_std().unwrap());
    //     //compress files and create new for next day
    //     filed = init_file();
    // }
    loop{}

    
    // loop {
    //     println!("Waiting for data");
    //     let record = Reader::with_schema(&schema, &mut socket).unwrap();/* from_avro_datum(&schema,result, None); */
    //     for result in record {
    //         avro_queue.push_back(result.unwrap());
    //     }
    // }
    
}

fn init_file() -> File{
    let bind = Local::now().to_string();
    let path = std::path::Path::new(bind.as_str());
    let mut file = File::create(path).unwrap();
    file.write_all(b"timestamp,device_id,latitude,longitude,altitude\n").unwrap();
    file
}
