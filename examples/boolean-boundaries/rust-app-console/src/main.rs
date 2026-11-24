use purchase_approval_queue_core::{
    request_support::AuditedSave as _, service_runtime, ServiceRuntimeConfig, E, Q,
};
use serde_json::json;
use teaql_core::{Entity as _, Value};
use teaql_runtime::{CheckRule, RuntimeError};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    assert!(args.len() == 3 && matches!(args[2].as_str(), "first" | "restart"));
    let phase = &args[2];
    let context = service_runtime(ServiceRuntimeConfig {
        database_url: args[1].clone(),
    })
    .await?;
    context.ensure_schema().await?;

    if phase == "first" {
        for (reference, status) in [
            ("PO-APPROVED", Value::Bool(true)),
            ("PO-DECLINED", Value::Bool(false)),
            ("PO-AWAITING", Value::Null),
        ] {
            let mut order = Q::purchase_orders()
                .comment("Open a purchase order for review")
                .purpose("Record the department's approval decision")
                .new_entity(&context)?;
            order
                .update_platform_id(1_u64)
                .update_reference(reference)
                .update_approved(status);
            order
                .audit_as("Record the buyer's decision")
                .save(&context)
                .await?;
        }
    }

    let approved = Q::purchase_orders()
        .which_are_approved()
        .limit(4)
        .comment("List approved purchase orders")
        .purpose("Select orders that may be sent to suppliers")
        .execute_for_list(&context)
        .await?;
    let declined = Q::purchase_orders()
        .which_are_not_approved()
        .limit(4)
        .comment("List declined purchase orders")
        .purpose("Keep declined orders out of the supplier queue")
        .execute_for_list(&context)
        .await?;
    let awaiting = Q::purchase_orders()
        .with_approved_is_unknown()
        .limit(4)
        .comment("List orders awaiting review")
        .purpose("Keep an undecided order distinct from a refusal")
        .execute_for_list(&context)
        .await?;
    for (rows, reference, value) in [
        (&approved, "PO-APPROVED", Some(true)),
        (&declined, "PO-DECLINED", Some(false)),
        (&awaiting, "PO-AWAITING", None),
    ] {
        assert_eq!(rows.len(), 1, "{reference} belongs to a single bucket");
        assert!(rows[0].is_field_loaded("approved"));
        let e = E::purchase_order(&rows[0]);
        assert_eq!(e.get_reference().try_eval()?, Some(reference.to_owned()));
        assert_eq!(
            E::purchase_order(&rows[0]).get_approved().try_eval()?,
            value.map(Some)
        );
    }
    let total = Q::purchase_orders()
        .comment("Count the entire purchasing queue")
        .purpose("Reconcile the three approval buckets")
        .execute_for_count(&context)
        .await?;
    assert_eq!(total, 3);

    let mut declined_order = Q::purchase_orders()
        .with_reference_is("PO-DECLINED")
        .limit(1)
        .comment("Read the declined purchase order")
        .purpose("Set up the rejected approval edit")
        .execute_for_one(&context)
        .await?
        .ok_or("The declined purchase order is missing")?;
    declined_order.update_approved(Value::I64(2));
    let refusal = declined_order
        .audit_as("Reject a non-Boolean approval value")
        .save(&context)
        .await
        .expect_err("An integer occupied a Boolean approval field");
    let RuntimeError::Check(results) = refusal else {
        return Err(format!("Unexpected refusal: {refusal}").into());
    };
    assert!(results.iter().any(|error| {
        error.rule == CheckRule::InvalidType && error.location.model_path() == "approved"
    }));
    let retained = Q::purchase_orders()
        .with_reference_is("PO-DECLINED")
        .limit(1)
        .comment("Read the retained approval decision")
        .purpose("Confirm the rejected edit left the order unchanged")
        .execute_for_one(&context)
        .await?
        .ok_or("The declined order disappeared")?;
    assert_eq!(retained.approved(), Some(false));
    assert_eq!(retained.version(), 1);

    let projected = Q::purchase_orders_minimal()
        .with_reference_is("PO-AWAITING")
        .select_reference()
        .limit(1)
        .comment("Read only the reference for an awaiting order")
        .purpose("Preserve NotLoaded separately from undecided")
        .execute_for_one(&context)
        .await?
        .ok_or("The projected order is missing")?;
    assert!(E::purchase_order(&projected)
        .get_approved()
        .try_eval()
        .is_err());
    println!(
        "PURCHASE_APPROVAL={}",
        json!({
            "phase": phase, "approved": 1, "declined": 1, "awaiting": 1,
            "total": total, "invalidApprovalRejected": true, "projectionNotLoaded": true,
            "retainedVersion": retained.version()
        })
    );
    Ok(())
}
