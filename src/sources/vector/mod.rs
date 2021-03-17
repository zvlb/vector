pub mod v1;
pub mod v2;

use crate::{
    config::{DataType, GenerateConfig, GlobalOptions, Resource, SourceConfig, SourceDescription},
    shutdown::ShutdownSignal,
    Pipeline,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
enum V1 {
    #[serde(rename = "1")]
    V1,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct VectorSourceConfigV1 {
    version: Option<V1>,
    #[serde(flatten)]
    config: v1::VectorSourceConfig,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum VectorSourceConfig {
    V1(VectorSourceConfigV1),
    //    V2(VectorSourceConfigV2),
}
//#[derive(Serialize, Deserialize, Debug, Clone)]
//enum V2 {
//    #[serde(rename = "2")]
//    V2,
//}

//#[derive(Serialize, Deserialize, Debug, Clone)]
//#[serde(deny_unknown_fields)]
//pub struct VectorSourceConfigV2 {
//    version: V2,
//    #[serde(flatten)]
//    config: v2::VectorSourceConfig,
//}

inventory::submit! {
    SourceDescription::new::<VectorSourceConfig>("vector")
}

impl GenerateConfig for VectorSourceConfig {
    fn generate_config() -> toml::Value {
        toml::from_str(
            r#"version = "2"
            hooks.process = """#,
        )
        .unwrap()
    }
}

#[async_trait::async_trait]
#[typetag::serde(name = "vector")]
impl SourceConfig for VectorSourceConfig {
    async fn build(
        &self,
        name: &str,
        globals: &GlobalOptions,
        shutdown: ShutdownSignal,
        out: Pipeline,
    ) -> crate::Result<super::Source> {
        match self {
            VectorSourceConfig::V1(v1) => v1.config.build(name, globals, shutdown, out).await,
            //            VectorSourceConfig::V2(v2) => v2.config.build(name, globals, shutdown, out).await,
        }
    }

    fn output_type(&self) -> DataType {
        match self {
            VectorSourceConfig::V1(v1) => v1.config.output_type(),
            //            VectorSourceConfig::V2(v2) => v2.config.output_type(),
        }
    }

    fn source_type(&self) -> &'static str {
        match self {
            VectorSourceConfig::V1(v1) => v1.config.source_type(),
            //            VectorSourceConfig::V2(v2) => v2.config.source_type(),
        }
    }
    fn resources(&self) -> Vec<Resource> {
        match self {
            VectorSourceConfig::V1(v1) => v1.config.resources(),
            //            VectorSourceConfig::V2(v2) => v2.config.resources(),
        }
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<super::VectorSourceConfig>();
    }
}
