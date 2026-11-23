//! Invocation-owned scope capture through the documented RequestPolicy SPI.
use school_management_service_core::Q;
use std::sync::{Arc, Mutex};
use teaql_core::{QueryRequestScope, SelectQuery};
use teaql_runtime::log_formatter::{HumanReaderFormatter, LogFormatter};
use teaql_runtime::{RequestPolicy, RuntimeError, UserContext};

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<QueryRequestScope>>>);
impl RequestPolicy for Capture {
    fn enforce_select(
        &self,
        _context: &UserContext,
        query: &mut SelectQuery,
    ) -> Result<(), RuntimeError> {
        self.0
            .lock()
            .unwrap()
            .push(query.request_scope().unwrap().clone());
        Ok(())
    }
}

pub async fn verify(context: &mut UserContext) -> Result<(), Box<dyn std::error::Error>> {
    let capture = Capture::default();
    context.set_request_policy(capture.clone());
    let mut observations = Vec::new();
    for variant in 0..5 {
        capture.0.lock().unwrap().clear();
        context.clear_sql_logs();
        let mut request = Q::schools_minimal().count_as("schools");
        let (method, local) = match variant {
            0 => {
                request
                    .roll_up_to_school_type()
                    .group_by_platform()
                    .comment("Group visible types");
                ("roll_up_to_school_type", "Group visible types")
            }
            1 => {
                request.roll_up_to_school_type_with(Q::school_types_minimal().group_by_platform());
                ("roll_up_to_school_type_with", "load SchoolType")
            }
            2 => {
                request = request.group_by_school_type_with(
                    Q::school_types_minimal()
                        .group_by_platform()
                        .comment("Group visible types"),
                );
                ("group_by_school_type_with", "Group visible types")
            }
            3 => {
                request = request.group_by_school_type_with_details_from(
                    Q::school_types_minimal().group_by_platform(),
                );
                ("group_by_school_type_with_details_from", "load SchoolType")
            }
            _ => {
                let mut parent = request
                    .roll_up_to_school_type()
                    .count_as("types")
                    .comment("Group visible types");
                parent
                    .roll_up_to_platform()
                    .group_by_name()
                    .comment("Group campus");
                ("roll_up_to_school_type", "Group visible types")
            }
        };
        let rows = request
            .limit(20)
            .comment("Load school statistics")
            .purpose("Verify generated statistical request ancestry")
            .execute_for_rows(context)
            .await?;
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].get("schools").and_then(teaql_core::Value::try_i64),
            Some(5)
        );
        let scopes = capture.0.lock().unwrap();
        assert_eq!(scopes.len(), if variant == 4 { 3 } else { 2 });
        let root = &scopes[0];
        let child = &scopes[1];
        assert_eq!(child.root_request(), root);
        let operation = child.parent_request().unwrap();
        assert_eq!(operation.operation().unwrap().method(), Some(method));
        assert_eq!(
            operation.operation().unwrap().relation(),
            Some("school_type")
        );
        assert_eq!(operation.parent_request(), Some(root));
        let chain = child
            .trace_chain()
            .iter()
            .map(|node| node.comment.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            chain,
            [
                "Verify generated statistical request ancestry".to_owned(),
                "Load school statistics".to_owned(),
                format!("{method}()"),
                local.to_owned()
            ]
        );
        let logs = context.sql_logs();
        assert_eq!(logs.len(), 1, "one actual SQL log per combined execution");
        let log = &logs[0];
        let nodes = log
            .log_context
            .request_tree
            .as_deref()
            .expect("actual inline request tree");
        assert_eq!(nodes.len(), if variant == 4 { 6 } else { 4 });
        let native_node = nodes
            .iter()
            .find(|node| node.method.as_deref() == Some(method))
            .unwrap();
        let local_node = nodes.iter().find(|node| node.detail == local).unwrap();
        assert_eq!(local_node.parent_id, Some(native_node.id));
        assert_eq!(native_node.parent_id, Some(1));
        for (index, node) in nodes.iter().enumerate() {
            assert_eq!(node.id, index);
            assert!(node.parent_id.is_none_or(|parent| parent < index));
        }
        let text = HumanReaderFormatter.format_sql_log(&log.trace_path, log);
        assert!(text.contains(&format!("{method}()")));
        assert!(text.contains(local));
        assert!(text.contains("SQL:"));
        if variant == 4 {
            let campus = &scopes[2];
            assert_eq!(campus.root_request(), root);
            assert_eq!(
                campus.parent_request().unwrap().parent_request(),
                Some(child)
            );
            assert_eq!(
                campus
                    .parent_request()
                    .unwrap()
                    .operation()
                    .unwrap()
                    .method(),
                Some("roll_up_to_platform")
            );
            assert_eq!(campus.comment(), "Group campus");
            let campus_node = nodes
                .iter()
                .find(|node| node.detail == "Group campus")
                .unwrap();
            let campus_operation = &nodes[campus_node.parent_id.unwrap()];
            assert_eq!(
                campus_operation.method.as_deref(),
                Some("roll_up_to_platform")
            );
            assert_eq!(campus_operation.parent_id, Some(local_node.id));
        }
        observations
            .push(serde_json::json!({"method":method,"chain":chain,"requests":scopes.len(),
                "physicalStatements":logs.len(),"requestTree":teaql_core::QueryRequestTraceNode::to_json_value(nodes),"text":text}));
    }
    println!(
        "GENERATED_ROLLUP_TRACE_SCOPES={}",
        serde_json::json!({"passed":true,"generatedQ":true,"runtimeLogsVerified":true,"caseCount":observations.len(),"observations":observations})
    );
    Ok(())
}
