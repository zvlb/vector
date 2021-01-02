use crate::{state, value, Expression, Object, Result, TypeDef, Value};

#[derive(Debug, Clone, PartialEq)]
pub struct Noop;

impl Expression for Noop {
    #[tracing::instrument(fields(noop = %self), skip(self))]
    fn execute(&self, _: &mut state::Program, _: &mut dyn Object) -> Result<Value> {
        Ok(Value::Null)
    }

    fn type_def(&self, _: &state::Compiler) -> TypeDef {
        TypeDef {
            kind: value::Kind::Null,
            ..Default::default()
        }
    }
}

impl std::fmt::Display for Noop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Value::Null.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_type_def;

    test_type_def![noop {
        expr: |_| Noop,
        def: TypeDef {
            kind: value::Kind::Null,
            ..Default::default()
        },
    }];
}
