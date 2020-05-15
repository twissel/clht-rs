//! A concurrent hash table based on <a href="https://github.com/LPD-EPFL/CLHT">CLHT</a>.
//!
//! A hash table that supports full concurrency of retrievals and high expected concurrency for
//! updates. This type is functionally very similar to `std::collections::HashMap`, and for the
//! most part has a similar API. Even though all operations on the map are thread-safe and operate
//! on shared references, retrieval operations do *not* entail locking, and there is *not* any
//! support for locking the entire table in a way that prevents all access.

mod bucket;
mod map;

pub type DefaultHashBuilder = ahash::RandomState;

pub use crate::map::HashMap;
