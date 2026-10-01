//! Language-neutral request envelope cases from teaql-conformance #99.
use teaql_core::{InsertCommand, MutationIntent, SelectQuery, TraceKind, TraceNode};
use teaql_data_service::{MutationCommand, MutationRequest, QueryRequest};

fn trace(input: &serde_json::Value) -> Vec<TraceNode> {
    input["trace"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|node| {
            let kind = match node["kind"].as_str().unwrap() {
                "Comment" => TraceKind::Comment,
                "Purpose" => TraceKind::Purpose,
                "AuditReason" => TraceKind::AuditReason,
                "Entity" => TraceKind::Entity,
                "Provider" => TraceKind::Provider,
                "Sql" => TraceKind::Sql,
                kind => panic!("unsupported fixture trace kind {kind}"),
            };
            TraceNode::typed(kind, "Order", None, node["detail"].as_str().unwrap_or(""))
        })
        .collect()
}

#[test]
fn shared_request_intent_vectors() {
    let fixtures: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/request-intent-v1.json")).unwrap();
    assert_eq!(fixtures["contract"], "teaql.request-intent.v1");
    let cases = fixtures["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 20);
    for case in cases {
        let id = case["id"].as_str().unwrap();
        let input = &case["input"];
        let result = match case["kind"].as_str().unwrap() {
            "query" => {
                let mut query = SelectQuery::new("Order");
                query.comment = input["comment"].as_str().map(str::to_owned);
                query.purpose = input["purpose"].as_str().map(str::to_owned);
                query.trace_chain = trace(input);
                QueryRequest::from_query(query).map(|mut request| {
                    if input["logging"] == false {
                        request.capture_execution_metadata = false;
                    }
                    assert_eq!(
                        request.comment(),
                        case["expected"]["comment"].as_str().unwrap(),
                        "{id}"
                    );
                    assert_eq!(
                        request.purpose(),
                        case["expected"]["purpose"].as_str().unwrap(),
                        "{id}"
                    );
                })
            }
            "mutation" => {
                let command = if let Some(children) = input["children"].as_array() {
                    MutationCommand::Batch(
                        children
                            .iter()
                            .map(|child| {
                                MutationCommand::Insert(InsertCommand::new("OrderItem"))
                                    .request(child["comment"].as_str().unwrap())
                                    .unwrap()
                            })
                            .collect(),
                    )
                } else {
                    let mut command = InsertCommand::new("Order");
                    command.trace_chain = trace(input);
                    MutationCommand::Insert(command)
                };
                MutationIntent::from_optional(input["comment"].as_str()).map(|intent| {
                    let request = MutationRequest::with_intent(command, intent);
                    assert_eq!(
                        request.comment(),
                        case["expected"]["comment"].as_str().unwrap(),
                        "{id}"
                    );
                })
            }
            kind => panic!("unsupported fixture kind {kind}"),
        };
        if case.get("error").is_some() {
            let error = result.expect_err(id);
            assert_eq!(
                error.code(),
                case["error"]["code"].as_str().unwrap(),
                "{id}"
            );
            assert_eq!(
                error.field,
                case["error"]["field"].as_str().unwrap(),
                "{id}"
            );
        } else {
            result.expect(id);
        }
    }
}
