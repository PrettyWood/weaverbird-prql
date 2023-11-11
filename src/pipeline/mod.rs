pub(crate) mod steps;
use crate::translate::{Dialect, ToPrql};
use anyhow::Result;

use enum_dispatch::enum_dispatch;
use serde::{Deserialize, Serialize};

pub(crate) use steps::AggregateStep;
pub(crate) use steps::DomainStep;
pub(crate) use steps::FilterStep;

#[derive(Serialize, Deserialize, Debug)]
pub struct Pipeline(pub Vec<PipelineStep>);

impl ToPrql for Pipeline {
    fn to_prql(&self, dialect: &Dialect) -> Result<String> {
        Ok(self
            .0
            .iter()
            .map(|step| step.to_prql(dialect))
            .collect::<Result<Vec<String>>>()?
            .join(" | "))
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "name", rename_all = "lowercase")]
#[enum_dispatch(ToPrql)]
pub enum PipelineStep {
    Domain(DomainStep),
    Aggregate(AggregateStep),
    Filter(FilterStep),
}
