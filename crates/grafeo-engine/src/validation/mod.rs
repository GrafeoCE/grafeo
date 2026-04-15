//! SHACL validation engine.
//!
//! This module provides the engine-level SHACL validation integration,
//! including SPARQL constraint dispatch via `SessionSparqlExecutor`.

use std::collections::HashMap;
use std::sync::Arc;

use grafeo_common::types::Value;
use grafeo_core::graph::rdf::shacl::{ShaclError, SparqlExecutor, ValidationReport};
use grafeo_core::graph::rdf::{RdfStore, Term};

use crate::session::Session;

/// SPARQL executor backed by a `Session`.
///
/// Implements the `SparqlExecutor` trait from grafeo-core, allowing
/// SHACL-SPARQL constraints to be evaluated via the engine's SPARQL pipeline.
pub struct SessionSparqlExecutor<'a> {
    session: &'a Session,
    /// When set, wraps queries in `GRAPH <name> { ... }` to scope execution
    /// to a named data graph (used by `validate_shacl_graph`).
    graph_name: Option<String>,
}

impl<'a> SessionSparqlExecutor<'a> {
    /// Creates a new executor wrapping the given session (default graph scope).
    pub fn new(session: &'a Session) -> Self {
        Self {
            session,
            graph_name: None,
        }
    }

    /// Creates an executor scoped to a named graph.
    pub fn with_graph(session: &'a Session, graph_name: String) -> Self {
        Self {
            session,
            graph_name: Some(graph_name),
        }
    }
}

impl SparqlExecutor for SessionSparqlExecutor<'_> {
    fn execute(
        &self,
        query: &str,
        this_binding: &Term,
    ) -> Result<Vec<HashMap<String, Term>>, ShaclError> {
        // Substitute $this with the N-Triples representation of the focus node.
        // IRIs are validated to prevent SPARQL injection via crafted terms.
        let this_str = match this_binding {
            Term::Iri(iri) => {
                let iri_str = iri.as_str();
                if !is_safe_iri(iri_str) {
                    return Err(ShaclError::SparqlError(format!(
                        "IRI contains characters unsafe for SPARQL embedding: {iri_str}"
                    )));
                }
                format!("<{iri_str}>")
            }
            Term::BlankNode(bnode) => format!("_:{}", bnode.id()),
            Term::Literal(lit) => {
                let escaped = escape_ntriples(lit.value());
                if let Some(lang) = lit.language() {
                    format!("\"{escaped}\"@{}", lang)
                } else if lit.datatype() != "http://www.w3.org/2001/XMLSchema#string" {
                    let dt = lit.datatype();
                    if !is_safe_iri(dt) {
                        return Err(ShaclError::SparqlError(format!(
                            "Datatype IRI contains unsafe characters: {dt}"
                        )));
                    }
                    format!("\"{escaped}\"^^<{dt}>")
                } else {
                    format!("\"{escaped}\"")
                }
            }
            _ => return Ok(Vec::new()),
        };

        let mut substituted = query.replace("$this", &this_str);

        // Scope to named data graph via FROM clause when configured
        if let Some(ref graph) = self.graph_name {
            if !is_safe_iri(graph) {
                return Err(ShaclError::SparqlError(format!(
                    "Graph name contains characters unsafe for SPARQL embedding: {graph}"
                )));
            }
            // Case-insensitive WHERE detection
            let upper = substituted.to_uppercase();
            if let Some(pos) = upper.find("WHERE") {
                substituted.insert_str(pos, &format!("FROM <{graph}> "));
            }
        }

        let result = self
            .session
            .execute_sparql(&substituted)
            .map_err(|e| ShaclError::SparqlError(e.to_string()))?;

        // Convert QueryResult rows to Vec<HashMap<String, Term>>
        let columns = &result.columns;
        let mut rows = Vec::new();
        for row in result.rows() {
            let mut map = HashMap::new();
            for (i, col) in columns.iter().enumerate() {
                if let Some(value) = row.get(i)
                    && let Some(term) = value_to_term(value)
                {
                    map.insert(col.clone(), term);
                }
            }
            rows.push(map);
        }

        Ok(rows)
    }
}

/// Escapes a string for N-Triples literal representation.
/// Checks whether an IRI string is safe for embedding in a SPARQL query.
///
/// Rejects characters that could break out of `<...>` IRI delimiters or
/// inject additional SPARQL syntax.
fn is_safe_iri(iri: &str) -> bool {
    !iri.chars().any(|c| {
        matches!(c, '<' | '>' | '"' | '{' | '}' | '|' | '\\' | '^' | '`') || c.is_whitespace()
    })
}

fn escape_ntriples(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ => out.push(ch),
        }
    }
    out
}

/// Converts a `grafeo_common::types::Value` to an RDF `Term`.
fn value_to_term(value: &Value) -> Option<Term> {
    match value {
        Value::Null => None,
        Value::String(s) => {
            if s.starts_with("http://") || s.starts_with("https://") || s.starts_with("urn:") {
                Some(Term::iri(s.as_str()))
            } else {
                Some(Term::literal(s.as_str()))
            }
        }
        Value::Int64(n) => Some(Term::typed_literal(
            n.to_string(),
            "http://www.w3.org/2001/XMLSchema#integer",
        )),
        Value::Float64(f) => Some(Term::typed_literal(
            f.to_string(),
            "http://www.w3.org/2001/XMLSchema#double",
        )),
        Value::Bool(b) => Some(Term::typed_literal(
            if *b { "true" } else { "false" },
            "http://www.w3.org/2001/XMLSchema#boolean",
        )),
        _ => Some(Term::literal(value.to_string())),
    }
}

/// Validates the default graph against shapes in a named graph.
///
/// This is the engine-level entry point that wires up the SPARQL executor.
///
/// # Errors
///
/// Returns an error if shape parsing fails, the shapes graph doesn't exist,
/// or a SPARQL constraint fails.
pub fn validate_shacl(
    session: &Session,
    rdf_store: &Arc<RdfStore>,
    shapes_graph_name: &str,
) -> grafeo_common::utils::error::Result<ValidationReport> {
    let shapes_store = rdf_store.graph(shapes_graph_name).ok_or_else(|| {
        grafeo_common::utils::error::Error::Internal(format!(
            "Named graph '{shapes_graph_name}' not found"
        ))
    })?;

    let executor = SessionSparqlExecutor::new(session);
    grafeo_core::graph::rdf::shacl::validate(rdf_store, &shapes_store, Some(&executor))
        .map_err(|e| grafeo_common::utils::error::Error::Internal(e.to_string()))
}
