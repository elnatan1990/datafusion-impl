// Copyright 2024 The DataFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

//! DataFusion implementation of Spark's regexp_extract function
//! 
//! This crate provides an implementation of the regexp_extract function
//! that matches Spark's functionality while taking advantage of DataFusion's
//! architecture and Rust's safety features.
//!
//! # Example
//! ```rust
//! use datafusion::prelude::*;
//! use datafusion_impl::create_regexp_extract;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let ctx = SessionContext::new();
//!     ctx.register_udf(create_regexp_extract());
//!     Ok(())
//! }
//! ```

pub mod regexp_extract;

pub use regexp_extract::create_regexp_extract;
