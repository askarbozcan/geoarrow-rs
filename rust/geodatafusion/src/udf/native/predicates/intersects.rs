use std::any::Any;
use std::sync::Arc;

use arrow_schema::DataType;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion::error::Result as DataFusionResult;
use geoarrow_geo::intersects;

use crate::data_types::{any_geometry_type_input, any_single_geometry_type_input, parse_to_native_array, GEOMETRY_TYPE};

#[derive(Debug)]
struct Intersects {
    signature: Signature
}

impl Intersects {
    pub fn new() -> Self {
        Self {
            // TODO: Implement a more strict type check(?)
            signature: any_geometry_type_input(2)
        }
    }
}

impl ScalarUDFImpl for Intersects {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "st_intersects"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;

        let left = parse_to_native_array(arrays[0].clone()).unwrap();
        let right = parse_to_native_array(arrays[1].clone()).unwrap();
        let result = intersects(
            left.as_ref(),
            right.as_ref()
        ).unwrap();

        Ok(ColumnarValue::Array(Arc::new(result)))

    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::{logical_expr::ScalarUDF, prelude::SessionContext};
    use geoarrow_array::{array::GeometryArray, builder::GeometryBuilder};
    use geoarrow_schema::{CoordType, GeometryType};

    #[tokio::test]
    async fn test_intersects_simple() {
        let ctx = SessionContext::new();
        ctx.register_udf(Intersects::new().into());
 
        ctx.register_udf(crate::udf::native::io::GeomFromText::new(CoordType::Separated).into());


        ctx.sql("
            select st_intersects(
                st_geomfromtext('LINESTRING(0 0, 1 1)'),
                st_geomfromtext('LINESTRING(0 1, 1 0)')
            ) as intersects;
        ").await
        .unwrap()
        .show()
        .await
        .unwrap();

    }
}