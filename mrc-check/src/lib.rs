//! mrc-check: a black-box correctness checker for Micro-Redis Cluster (Mini 3).

pub mod check;
pub mod history;
pub mod resp;
pub mod schedule;

#[cfg(test)]
mod check_tests;
