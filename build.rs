#[cfg(feature = "capi")]
extern crate cbindgen;

#[cfg(feature = "capi")]
fn generate_with_lang(crate_dir: &str, lang: cbindgen::Language, out: &str) {
    let cfg = cbindgen::Config::from_root_or_default(std::path::Path::new(crate_dir));

    match cbindgen::Builder::new()
        .with_config(cfg)
        .with_header(format!(
            "/* libPVM Header Version {} */",
            env!("CARGO_PKG_VERSION")
        ))
        .with_language(lang)
        .with_crate(&crate_dir)
        .generate()
    {
        Ok(b) => {
            b.write_to_file(out);
        }
        Err(e) => {
            eprintln!("Failed to generate bindings: {}", e);
            panic!();
        }
    }
}

#[cfg(feature = "capi")]
fn main() {
    use std::env;

    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();

    generate_with_lang(&crate_dir, cbindgen::Language::C, "src/include/pvm.h");

    generate_with_lang(&crate_dir, cbindgen::Language::Cxx, "src/include/pvm.hpp");
}

#[cfg(not(feature = "capi"))]
fn main() {}
