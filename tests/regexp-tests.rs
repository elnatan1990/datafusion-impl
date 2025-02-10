use datafusion::prelude::*;
use datafusion_impl::create_regexp_extract;
use std::sync::Arc;

#[tokio::test]
async fn test_regexp_extract_integration() -> Result<()> {
    // Create a new session context
    let ctx = SessionContext::new();
    
    // Register the UDF
    ctx.register_udf(create_regexp_extract());
    
    // Create test data
    let df = ctx.read_csv("test_data.csv").await?;
    
    // Apply regexp_extract
    let result = df.select(vec![
        col("text"),
        call_fn(
            "regexp_extract",
            vec![col("text"), lit("(\\d+)-(\\d+)"), lit(1)]
        )?
    ])?;
    
    result.show().await?;
    Ok(())
}

#[tokio::test]
async fn test_regexp_extract_edge_cases() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_udf(create_regexp_extract());

    // Test various edge cases
    let test_cases = vec![
        // Empty string
        ("", "(.*)", 1, ""),
        // No match
        ("abc", "\\d+", 1, ""),
        // Invalid group index
        ("123", "(\\d+)", 2, ""),
        // Multiple groups
        ("123-456", "(\\d+)-(\\d+)", 2, "456"),
        // Special characters
        ("foo$bar", "(\\w+)\\$(\\w+)", 2, "bar"),
    ];

    for (input, pattern, group, expected) in test_cases {
        let sql = format!(
            "SELECT regexp_extract('{}', '{}', {}) as extracted",
            input, pattern, group
        );
        
        let df = ctx.sql(&sql).await?;
        let batches = df.collect().await?;
        
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1);
        
        let col = batch.column(0);
        let result = col.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result.value(0), expected);
    }

    Ok(())
}

#[tokio::test]
async fn test_regexp_extract_null_handling() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_udf(create_regexp_extract());

    // Create a DataFrame with NULL values
    let df = ctx.sql(
        "SELECT 
            regexp_extract(
                CASE WHEN id % 2 = 0 THEN text ELSE NULL END,
                '(\\d+)',
                1
            ) as extracted
         FROM (
            SELECT id, CAST(id as VARCHAR) as text 
            FROM (VALUES (1), (2), (3), (4)) as t(id)
         )"
    ).await?;

    let batches = df.collect().await?;
    assert_eq!(batches.len(), 1);
    
    let batch = &batches[0];
    let col = batch.column(0);
    let result = col.as_any().downcast_ref::<StringArray>().unwrap();

    // Check alternating NULL and non-NULL values
    assert!(result.is_null(0));
    assert_eq!(result.value(1), "2");
    assert!(result.is_null(2));
    assert_eq!(result.value(3), "4");

    Ok(())
}

#[tokio::test]
async fn test_regexp_extract_performance() -> Result<()> {
    use std::time::Instant;
    
    let ctx = SessionContext::new();
    ctx.register_udf(create_regexp_extract());

    // Generate large test dataset
    let mut values = Vec::new();
    for i in 0..10000 {
        values.push(format!("test-{}-data", i));
    }

    // Create DataFrame
    let df = ctx.sql(&format!(
        "SELECT regexp_extract(text, '-(\\d+)-', 1) as extracted
         FROM (VALUES {}) as t(text)",
        values.iter()
            .map(|v| format!("('{}')", v))
            .collect::<Vec<_>>()
            .join(",")
    )).await?;

    // Measure execution time
    let start = Instant::now();
    let batches = df.collect().await?;
    let duration = start.elapsed();

    // Verify results
    let batch = &batches[0];
    let col = batch.column(0);
    let result = col.as_any().downcast_ref::<StringArray>().unwrap();

    // Check first few results
    assert_eq!(result.value(0), "0");
    assert_eq!(result.value(1), "1");
    assert_eq!(result.value(2), "2");

    // Print performance metrics
    println!("Processed {} rows in {:?}", values.len(), duration);
    println!("Average time per row: {:?}", duration / values.len() as u32);

    Ok(())
}

#[tokio::test]
async fn test_regexp_extract_concurrent() -> Result<()> {
    use futures::future::join_all;
    use std::time::Duration;
    use tokio::time::sleep;

    let ctx = SessionContext::new();
    ctx.register_udf(create_regexp_extract());

    // Create multiple concurrent tasks
    let mut handles = Vec::new();
    for i in 0..5 {
        let ctx = ctx.clone();
        let handle = tokio::spawn(async move {
            // Add some delay to ensure concurrent execution
            sleep(Duration::from_millis(i * 100)).await;

            let df = ctx.sql(
                "SELECT regexp_extract('test-123-data', '-(\\d+)-', 1) as extracted"
            ).await?;
            
            let batches = df.collect().await?;
            Result::Ok(batches)
        });
        handles.push(handle);
    }

    // Wait for all tasks to complete
    let results = join_all(handles).await;
    
    // Verify results
    for result in results {
        let batches = result??;
        assert_eq!(batches.len(), 1);
        
        let batch = &batches[0];
        let col = batch.column(0);
        let result = col.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result.value(0), "123");
    }

    Ok(())
}

#[tokio::test]
async fn test_regexp_extract_error_handling() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_udf(create_regexp_extract());

    // Test invalid regex pattern
    let df = ctx.sql(
        "SELECT regexp_extract('test', '[invalid', 1) as extracted"
    ).await?;
    
    let batches = df.collect().await?;
    let batch = &batches[0];
    let col = batch.column(0);
    let result = col.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(result.value(0), "");

    // Test invalid group index (out of bounds)
    let df = ctx.sql(
        "SELECT regexp_extract('test', '(t)(e)(s)(t)', 5) as extracted"
    ).await?;
    
    let batches = df.collect().await?;
    let batch = &batches[0];
    let col = batch.column(0);
    let result = col.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(result.value(0), "");

    Ok(())
}
