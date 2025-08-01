use heck::ToSnakeCase;
use serde::Deserialize;
use std::env;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::Path;

#[derive(Deserialize, Debug)]
struct MessageSpec {
    name: String,
    fields: Vec<FieldSpec>,
}

#[derive(Deserialize, Debug)]
struct FieldSpec {
    name: String,
    #[serde(rename = "type")]
    field_type: String,
    versions: String,
}

fn map_type(json_type: &str) -> &str {
    match json_type {
        "string" => "String",
        "bool" => "bool",
        "int8" => "i8",
        "int16" => "i16",
        "int32" => "i32",
        "int64" => "i64",
        "uint16" => "u16",
        "uint32" => "u32",
        "uuid" => "u128", // Or use a uuid crate type
        _ => "bytes::Bytes", // Default for complex types for now
    }
}

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=kafka-protocol-defs/");

    let out_dir = env::var("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("kafka_protocol.rs");
    let mut f = File::create(&dest_path).unwrap();

    let mut all_code = String::new();

    let defs_dir = "kafka-protocol-defs";
    for entry in fs::read_dir(defs_dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_file() && path.extension().unwrap() == "json" {
            let mut file_content = String::new();
            File::open(&path)
                .unwrap()
                .read_to_string(&mut file_content)
                .unwrap();
            
            // Manually strip comments
            let content_without_comments: String = file_content
                .lines()
                .filter(|line| !line.trim().starts_with("//"))
                .collect::<Vec<&str>>()
                .join("\n");

            let spec: MessageSpec = serde_json::from_str(&content_without_comments).unwrap_or_else(|e| {
                panic!("Failed to parse JSON from {}: {}", path.display(), e);
            });

            let mut fields_code = String::new();
            for field in spec.fields {
                fields_code.push_str(&format!(
                    "    #[kafka(versions = \"{versions}\", original_name = \"{original_name}\")]\n    pub {name}: {rust_type},\n",
                    versions = field.versions,
                    original_name = field.name,
                    name = field.name.to_snake_case(),
                    rust_type = map_type(&field.field_type)
                ));
            }

            let struct_code = format!(
                "
#[derive(crate::bifrost_protocol_macro::KafkaProtocol, Debug, Default)]
pub struct {name} {{
{fields}
}}
",
                name = spec.name,
                fields = fields_code
            );
            all_code.push_str(&struct_code);
        }
    }

    f.write_all(all_code.as_bytes()).unwrap();
} 