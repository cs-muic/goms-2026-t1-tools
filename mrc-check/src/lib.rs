//! mrc-check: a black-box correctness checker for Micro-Redis Cluster (Mini 3).

pub mod check;
pub mod history;
pub mod resp;

#[cfg(test)]
mod check_tests;
