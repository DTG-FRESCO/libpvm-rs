use std::{
    env::var,
    error::Error,
    fs::File,
    io::{stdin, Read},
};

use pvm::{
    cfg::Config,
    engine::Engine,
    view::{View, ViewParams, ViewParamsExt},
};

use clap::{
    app_from_crate, crate_authors, crate_description, crate_name, crate_version, Arg, ArgMatches,
};

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

    fn as_clap_args(&self) -> Vec<Arg> {
        let mut ret = Vec::new();
        ret.push(Arg::with_name(&self.name).long(&self.name).help(&self.help));
        ret.extend(self.params.iter().map(|p| p.as_clap_arg(&self.name)));
        ret
    }

    fn is_present(&self, m: &ArgMatches) -> bool {
        m.is_present(&self.name)
    }

    fn get_id_and_params(&self, m: &ArgMatches) -> (usize, ViewParams) {
        let mut params = ViewParams::new();
        for param in &self.params {
            if let Some(val) = m.value_of(&param.name) {
                params.insert_param(&param.act_name, val.to_string());
            }
        }
        (self.id, params)
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

    fn as_clap_arg<'a>(&'a self, aname: &'a str) -> Arg<'a, 'a> {
        Arg::with_name(&self.name)
            .long(&self.name)
            .help(&self.help)
            .requires(aname)
            .takes_value(true)
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let plugin_dir = var("PVM_PLUGIN_DIR").ok();

    let cfg = if let Some(plugin_dir) = plugin_dir {
        Config::build().plugin_dir(plugin_dir).finish()
    } else {
        Config::default()
    };

    let mut e = Engine::new(cfg)?;
    e.init_pipeline()?;

    let args = e
        .list_view_types()?
        .into_iter()
        .map(ViewArgDetails::from_view)
        .collect::<Vec<_>>();

    let m = app_from_crate!()
        .arg(
            Arg::with_name("path")
                .required(true)
                .help("Path to begin ingesting data from."),
        )
        .args(
            &args
                .iter()
                .flat_map(ViewArgDetails::as_clap_args)
                .collect::<Vec<_>>(),
        )
        .get_matches();

    for arg in &args {
        if arg.is_present(&m) {
            let (id, params) = arg.get_id_and_params(&m);
            e.create_view_by_id(id, params)?;
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
