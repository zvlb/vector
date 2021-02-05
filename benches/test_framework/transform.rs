use serde::{Deserialize, Serialize};
use vector::{config::TransformConfig, transforms::Transform};

pub mod bench;
pub mod test;

#[derive(Deserialize, Serialize, Debug)]
struct Parser {
    #[serde(flatten)]
    pub inner: Box<dyn TransformConfig>,
}

pub fn parse_config(toml_config: &str) -> Box<dyn TransformConfig> {
    let parser: Parser =
        toml::from_str(toml_config).expect("you must pass a valid config in benches");
    parser.inner
}

pub fn build(transform_config: &dyn TransformConfig) -> Transform {
    futures::executor::block_on(transform_config.build()).expect("transform must build")
}
