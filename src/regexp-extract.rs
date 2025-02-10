use datafusion::arrow::array::{ArrayRef, Int32Array, StringBuilder, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{ScalarUDF, ScalarUDFImpl};
use datafusion::physical_plan::functions::make_scalar_function;
use regex::Regex;
use std::sync::Arc;

/// Creates a new regexp_extract UDF that matches Spark's functionality.
///
/// This function creates a User Defined Function (UDF) that implements
/// regexp_extract, matching Spark's behavior exactly.
///
/// # Returns
/// * `ScalarUDF` - A DataFusion UDF that can be registered with a context
///
/// # Example
/// ```rust
/// use datafusion::prelude::*;
/// use datafusion_impl::create_regexp_extract;
///
/// let ctx = SessionContext::new();
/// ctx.register_udf(create_regexp_extract());
/// ```
pub fn create_regexp_extract() -> ScalarUDF {
    let func = make_scalar_function(regexp_extract_impl);
    ScalarUDF::new(
        "regexp_extract",
        &[DataType::Utf8, DataType::Utf8, DataType::Int32],
        &DataType::Utf8,
        ScalarUDFImpl::Volatility(func),
    )
}

/// Implementation of regexp_extract that matches Spark's behavior
///
/// # Arguments
/// * `args` - Array of input arguments:
///   - args[0]: Input string array
///   - args[1]: Regex pattern array
///   - args[2]: Group index array
///
/// # Returns
/// * `Result<ArrayRef>` - String array containing extracted matches
///
/// # Error
/// Returns error if:
/// - Wrong number of arguments
/// - Wrong argument types
/// - Memory allocation fails
fn regexp_extract_impl(args: &[ArrayRef]) -> Result<ArrayRef> {
    // Validate number of arguments
    if args.len() != 3 {
        return Err(DataFusionError::Internal(
            "regexp_extract requires 3 arguments: string, pattern, and group index".to_string(),
        ));
    }

    // Cast input arrays to their expected types
    let str_array = args[0]
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Internal("First argument must be a string array".to_string())
        })?;

    let pattern_array = args[1]
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Internal("Second argument must be a string array".to_string())
        })?;

    let idx_array = args[2]
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| {
            DataFusionError::Internal("Third argument must be an integer array".to_string())
        })?;

    // Create builder for result array with appropriate capacity
    let mut result_builder = StringBuilder::with_capacity(str_array.len());

    // Process each row
    for i in 0..str_array.len() {
        // Handle null values - if any input is null, output is null
        if str_array.is_null(i) || pattern_array.is_null(i) || idx_array.is_null(i) {
            result_builder.append_null()?;
            continue;
        }

        let input_str = str_array.value(i);
        let pattern = pattern_array.value(i);
        let group_idx = idx_array.value(i) as usize;

        // Compile and apply regex
        match Regex::new(pattern) {
            Ok(re) => {
                let captured = re
                    .captures(input_str)
                    .and_then(|caps| caps.get(group_idx))
                    .map(|m| m.as_str())
                    .unwrap_or("");
                result_builder.append_value(captured)?;
            }
            Err(_) => {
                // Invalid regex pattern - following Spark's behavior, return empty string
                result_builder.append_value("")?;
            }
        }
    }

    // Build and return final array
    Ok(Arc::new(result_builder.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_regexp_extract_basic() {
        let str_array = StringArray::from(vec!["100-200", "300-400", "500-600"]);
        let pattern_array = StringArray::from(vec!["(\\d+)-(\\d+)", "(\\d+)-(\\d+)", "(\\d+)-(\\d+)"]);
        let idx_array = Int32Array::from(vec![1, 1, 1]);

        let result = regexp_extract_impl(&[
            Arc::new(str_array),
            Arc::new(pattern_array),
            Arc::new(idx_array),
        ])
        .unwrap();

        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), "100");
        assert_eq!(result_array.value(1), "300");
        assert_eq!(result_array.value(2), "500");
    }

    #[test]
    fn test_regexp_extract_second_group() {
        let str_array = StringArray::from(vec!["100-200", "300-400", "500-600"]);
        let pattern_array = StringArray::from(vec!["(\\d+)-(\\d+)", "(\\d+)-(\\d+)", "(\\d+)-(\\d+)"]);
        let idx_array = Int32Array::from(vec![2, 2, 2]);

        let result = regexp_extract_impl(&[
            Arc::new(str_array),
            Arc::new(pattern_array),
            Arc::new(idx_array),
        ])
        .unwrap();

        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), "200");
        assert_eq!(result_array.value(1), "400");
        assert_eq!(result_array.value(2), "600");
    }

    #[test]
    fn test_regexp_extract_no_match() {
        let str_array = StringArray::from(vec!["abc", "def"]);
        let pattern_array = StringArray::from(vec!["(\\d+)", "(\\d+)"]);
        let idx_array = Int32Array::from(vec![1, 1]);

        let result = regexp_extract_impl(&[
            Arc::new(str_array),
            Arc::new(pattern_array),
            Arc::new(idx_array),
        ])
        .unwrap();

        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), "");
        assert_eq!(result_array.value(1), "");
    }

    #[test]
    fn test_regexp_extract_invalid_group() {
        let str_array = StringArray::from(vec!["100-200"]);
        let pattern_array = StringArray::from(vec!["(\\d+)-(\\d+)"]);
        let idx_array = Int32Array::from(vec![3]); // Invalid group index

        let result = regexp_extract_impl(&[
            Arc::new(str_array),
            Arc::new(pattern_array),
            Arc::new(idx_array),
        ])
        .unwrap();

        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), "");
    }

    #[test]
    fn test_regexp_extract_null_handling() {
        let str_array = StringArray::from(vec![Some("100-200"), None, Some("300-400")]);
        let pattern_array = StringArray::from(vec![Some("(\\d+)-(\\d+)"), Some("(\\d+)-(\\d+)"), None]);
        let idx_array = Int32Array::from(vec![Some(1), Some(1), Some(1)]);

        let result = regexp_extract_impl(&[
            Arc::new(str_array),
            Arc::new(pattern_array),
            Arc::new(idx_array),
        ])
        .unwrap();

        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), "100");
        assert!(result_array.is_null(1));
        assert!(result_array.is_null(2));
    }

    #[test]
    fn test_regexp_extract_invalid_regex() {
        let str_array = StringArray::from(vec!["100-200"]);
        let pattern_array = StringArray::from(vec!["[invalid"]);  // Invalid regex pattern
        let idx_array = Int32Array::from(vec![1]);

        let result = regexp_extract_impl(&[
            Arc::new(str_array),
            Arc::new(pattern_array),
            Arc::new(idx_array),
        ])
        .unwrap();

        let result_array = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), "");
    }
}
