# DataFusion RegExp Extract Implementation

This project implements Spark's `regexp_extract` function in DataFusion, providing a high-performance regex extraction capability that matches Spark's functionality.

## Features

- Matches Spark's `regexp_extract` behavior exactly
- Vectorized implementation for high performance
- Comprehensive error handling
- Full test coverage including edge cases
- No dependency on DataFusion's SQL API

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/datafusion-impl
cd datafusion-impl
```

2. Build the project:
```bash
cargo build --release
```

3. Run tests:
```bash
cargo test
```

## Usage

```rust
use datafusion::prelude::*;
use datafusion_impl::create_regexp_extract;

#[tokio::main]
async fn main() -> Result<()> {
    // Create a new session context
    let ctx = SessionContext::new();
    
    // Register the UDF
    ctx.register_udf(create_regexp_extract());
    
    // Use in DataFrame operations
    let df = ctx.read_csv("your_data.csv").await?;
    let result = df.select(vec![
        call_fn("regexp_extract", vec![
            col("text_column"),
            lit("(\\d+)"),
            lit(1)
        ])?
    ])?;
    
    result.show().await?;
    Ok(())
}
```

## Function Behavior

The `regexp_extract` function matches Spark's behavior:

- Takes 3 arguments:
  1. Input string
  2. Regex pattern
  3. Group index to extract
- Returns:
  - The matched group as a string
  - Empty string if no match or invalid group index
  - NULL if any input is NULL
- Handles edge cases:
  - Invalid regex patterns
  - Out-of-bounds group indices
  - NULL values
  - Empty strings

## Testing

The implementation includes comprehensive tests:

```bash
# Run all tests
cargo test

# Run specific test
cargo test test_regexp_extract_basic
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.