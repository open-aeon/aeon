use heck::ToSnakeCase;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::env;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::Path;

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
struct MessageSpec {
    name: String,
    fields: Vec<FieldSpec>,
    #[serde(default)]
    flexible_versions: String,
}

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
struct FieldSpec {
    name: String,
    #[serde(rename = "type")]
    field_type: String,
    versions: String,
    #[serde(default)]
    fields: Vec<FieldSpec>,
    #[serde(default)]
    tagged_versions: String,
    #[serde(default)]
    tag: Option<u32>,
    #[serde(default)]
    nullable_versions: String,
}

fn map_type(field: &FieldSpec, defined_structs: &HashSet<String>) -> String {
    let json_type = &field.field_type;
    let is_nullable = !field.nullable_versions.is_empty();

    let mut base_type = if json_type.starts_with("[]") {
        let inner_type_str = &json_type[2..];

        let mut inner_field = field.clone();
        inner_field.field_type = inner_type_str.to_string();

        inner_field.nullable_versions = String::new();

        let rust_inner_type = map_type(&inner_field, defined_structs);

        format!("Vec<{}>", rust_inner_type)
    } else {
        match json_type.as_str() {
            "string" => "String".to_string(),
            "bytes" => "bytes::Bytes".to_string(),
            "records" => "bytes::Bytes".to_string(),
            "bool" => "bool".to_string(),
            "int8" => "i8".to_string(),
            "int16" => "i16".to_string(),
            "int32" => "i32".to_string(),
            "int64" => "i64".to_string(),
            "uint16" => "u16".to_string(),
            "uint32" => "u32".to_string(),
            "uuid" => "u128".to_string(),
            _ => {
                if defined_structs.contains(json_type.as_str()) {
                    json_type.to_string()
                } else {
                    "()".to_string()
                }
            }
        }
    };

    if is_nullable {
        base_type = format!("Option<{}>", base_type);
    }
    
    base_type
}


fn collect_specs_recursively(
    specs: &mut HashMap<String, MessageSpec>,
    fields: &[FieldSpec],
    parent_flexible_versions: &str,
) {
    for field in fields {
        if !field.fields.is_empty() {
            let struct_name = field.field_type.trim_start_matches("[]").to_string();
            let new_spec = MessageSpec {
                name: struct_name.clone(),
                fields: field.fields.clone(),
                // Inherit flexible versions from parent to ensure nested structs
                // participate in compact encoding when the parent is flexible.
                flexible_versions: parent_flexible_versions.to_string(),
            };
            collect_specs_recursively(specs, &new_spec.fields, parent_flexible_versions);
            specs.insert(struct_name, new_spec);
        }
    }
}


fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=src/kafka/schemas/");
    println!("cargo:rerun-if-changed=aeon-protocol-macro/src/lib.rs");

    let out_dir = env::var("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("kafka_protocol.rs");
    let mut f = File::create(&dest_path).unwrap();

    let defs_dir = "src/kafka/schemas";
    
    // First pass: collect all defined struct names and their specs, including inline ones.
    let mut specs: HashMap<String, MessageSpec> = HashMap::new();

    for entry in fs::read_dir(defs_dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_file() && path.extension().unwrap() == "json" {
             if path.file_stem().unwrap().to_str().unwrap().contains("IGNORE") {
                continue;
            }
            let mut file_content = String::new();
            File::open(&path)
                .unwrap()
                .read_to_string(&mut file_content)
                .unwrap();
            
            let content_without_comments: String = file_content
                .lines()
                .filter(|line| !line.trim().starts_with("//"))
                .collect::<Vec<&str>>()
                .join("\n");

            let spec: MessageSpec = serde_json::from_str(&content_without_comments).unwrap_or_else(|e| {
                panic!("Failed to parse JSON from {}: {}", path.display(), e);
            });
            
            collect_specs_recursively(&mut specs, &spec.fields, &spec.flexible_versions);
            specs.insert(spec.name.clone(), spec);
        }
    }

    let defined_structs: HashSet<String> = specs.keys().cloned().collect();
    let mut all_code = String::new();

    // Second pass: generate code using the collected information
    for (name, spec) in &specs {
        let mut fields_code = String::new();
        for field in &spec.fields {
            let mut attrs = vec![
                format!("versions = \"{}\"", field.versions),
                format!("original_name = \"{}\"", field.name),
            ];
            if !field.tagged_versions.is_empty() {
                attrs.push(format!("tagged_versions = \"{}\"", field.tagged_versions));
            }
            if let Some(tag) = field.tag {
                attrs.push(format!("tag = {}", tag));
            }

            fields_code.push_str(&format!(
                "    #[kafka({})]\n    pub {}: {},\n",
                attrs.join(", "),
                field.name.to_snake_case(),
                map_type(&field, &defined_structs)
            ));
        }

        let mut struct_attrs = String::new();
        if !spec.flexible_versions.is_empty() {
            struct_attrs.push_str(&format!("#[kafka_flexible_versions(\"{}\")]\n", spec.flexible_versions));
        }

        let struct_code = format!(
            "
#[derive(crate::aeon_protocol_macro::KafkaProtocol, Debug, Default, Clone, PartialEq)]
{struct_attrs}pub struct {name} {{
{fields}
}}
",
            struct_attrs = struct_attrs,
            name = name,
            fields = fields_code
        );
        all_code.push_str(&struct_code);
    }

    f.write_all(all_code.as_bytes()).unwrap();

    // Raft gRPC generation
    tonic_build::configure()
        .compile(&["src/raft/proto/raft.proto"], &["src/raft/proto/"])
        .unwrap();

}
