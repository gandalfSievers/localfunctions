pub(crate) mod common;
mod eventstream;
pub(crate) mod extensions;
pub(crate) mod function_url;
pub(crate) mod functions;
pub(crate) mod health;
pub(crate) mod invoke;
pub(crate) mod runtime;
pub(crate) mod sns;
pub(crate) mod streaming;
pub(crate) mod virtual_host;
mod sample_events;

use common::*;

// Re-export key items from submodules for external consumers.
pub use invoke::invoke_routes;
pub(crate) use invoke::acquire_container;

// ---------------------------------------------------------------------------
// Runtime API routes are in the `runtime` submodule.
pub(crate) use runtime::runtime_routes;
