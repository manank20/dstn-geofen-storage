#[allow(unused_imports)]
use std::net::{ TcpListener, TcpStream};
use avro_rs::{Reader, Schema};
use tokio::fs::create_dir;
use std::fs::File;
use std::thread;
use crossbeam_queue::SegQueue;
use chrono::prelude::*;
use chrono::Duration;
use std::io::prelude::*;

use std::sync::{Arc, atomic::{AtomicBool,Ordering}};

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

const PT: &str  = "data/";

fn main(){
    loop{
        main1();
    }
}
fn main1() {

    println!("Starting server at {}\n\n\n\n", IP_ADDRESS);

    let schema = Schema::parse_str(RAW_SCHEMA).unwrap();

    let checkpoint = Arc::new(AtomicBool::new(true));
    let mut socket = TcpStream::connect(IP_ADDRESS).unwrap();
    println!("Connected to server");

    let avro_queue: SegQueue<Vec<(String, avro_rs::types::Value)>> = SegQueue::new();

    let mut filename = init_file();

    thread::scope(|s| {
        s.spawn(|| {
            while checkpoint.load(Ordering::Relaxed) {
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
            while checkpoint.load(Ordering::Relaxed) {
                let mut reader = Reader::with_schema(&schema, &mut socket).unwrap();
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
                    if checkpoint.load(Ordering::Relaxed) == false{
                        break;
                    }
                }
            }
        });

        s.spawn(|| {
            std::thread::sleep(Duration::days(1).to_std().unwrap());
            checkpoint.store(false, Ordering::Relaxed);
        });
    });
 
    println!("exiting main1");

    return;

}

fn init_file() -> File{
    let bind = Local::now().to_string();
    let mut iter = bind.split(" ");
    let bind = iter.next().unwrap();
    let bind = PT.to_owned() + bind + ".txt";
    let path1 = std::path::Path::new(PT);
    let path = std::path::Path::new(&bind);
    if !path1.exists(){
        std::fs::create_dir(path1).unwrap();
    }
    // print!("path: {:?} path1: {:?}", path, path1);
    let mut file;
    if !path.exists(){
        let fil = File::create(path);
        file = fil.unwrap(); 
        file.write_all(b"timestamp,device_id,latitude,longitude,altitude\n").unwrap();
        return file;
    }
    file = std::fs::OpenOptions::new().append(true).open(path).unwrap();
    file.write_all(b"timestamp,device_id,latitude,longitude,altitude\n").unwrap();
    file
}
