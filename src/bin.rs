use std::{
    env::var,
    error::Error,
    fs::File,
    io::{stdin, Read},
};

use pvm::{
    cfg::{CfgMode, Config},
    engine::Engine,
    view::{View, ViewParams, ViewParamsExt},
};

use clap::{app_from_crate, crate_authors, crate_description, crate_name, crate_version, Arg};

struct ViewArgDetails {
    id: usize,
    name: String,
    help: &'static str,
    params: Vec<ViewParamArgDetails>,
}

impl ViewArgDetails {
    fn from_view(view: &dyn View) -> Self {
        let mut vname = view.name().to_lowercase();
        if vname.ends_with("view") {
            vname.truncate(vname.len() - 4);
        }
        ViewArgDetails {
            id: view.id(),
            help: view.desc(),
            params: view
                .params()
                .into_iter()
                .map(|p| ViewParamArgDetails::from_param(&vname, p))
                .collect(),
            name: vname,
        }
    }
}

struct ViewParamArgDetails {
    act_name: &'static str,
    name: String,
    help: &'static str,
}

impl ViewParamArgDetails {
    fn from_param(vname: &str, param: (&'static str, &'static str)) -> Self {
        let (name, desc) = param;
        let p_name = format!("{}-{}", &vname, name.to_lowercase());
        ViewParamArgDetails {
            act_name: name,
            name: p_name,
            help: desc,
        }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut e = Engine::new(Config {
        cfg_detail: None,
        cfg_mode: CfgMode::Auto,
        plugin_dir: var("PVM_PLUGIN_DIR").ok(),
    })?;
    e.init_pipeline()?;

    let mut app = Some(
        app_from_crate!().arg(
            Arg::with_name("path")
                .required(true)
                .help("Path to begin ingesting data from."),
        ),
    );

    let args = e
        .list_view_types()?
        .into_iter()
        .map(ViewArgDetails::from_view)
        .collect::<Vec<_>>();

    for arg in &args {
        let tmp = app
            .take()
            .unwrap()
            .arg(Arg::with_name(&arg.name).long(&arg.name).help(&arg.help));
        app = Some(tmp);

        for param in &arg.params {
            let tmp = app.take().unwrap().arg(
                Arg::with_name(&param.name)
                    .long(&param.name)
                    .help(&param.help)
                    .requires(&arg.name)
                    .takes_value(true),
            );
            app = Some(tmp);
        }
    }

    let m = app.unwrap().get_matches();

    for arg in &args {
        if m.is_present(&arg.name) {
            let mut params = ViewParams::new();
            for param in &arg.params {
                if let Some(val) = m.value_of(&param.name) {
                    params.insert_param(&param.act_name, val.to_string());
                }
            }
            e.create_view_by_id(arg.id, params)?;
        }
    }

    let src: Box<dyn Read> = {
        let path = m.value_of("path").unwrap();
        if path == "-" {
            Box::new(stdin())
        } else {
            Box::new(File::open(path)?)
        }
    };

    pvm::timeit!(e.ingest_reader(src)?);

    e.shutdown_pipeline()?;

    Ok(())
}
