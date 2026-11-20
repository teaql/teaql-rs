use school_enrollment_core::{
    request_support::AuditedSave as _, service_runtime, teaql_core::Entity as _,
    ServiceRuntimeConfig, Q,
};
use std::sync::{Arc, Mutex};
use teaql_runtime::{
    LedgerEntity as _, RawAuditEventKind, RuntimeError, SafeAuditEvent, SafeAuditEventSink,
    UserContext,
};

#[derive(Clone, Default)]
struct EnrollmentAudit(Arc<Mutex<Vec<String>>>);

impl SafeAuditEventSink for EnrollmentAudit {
    fn on_safe_event(&self, _: &UserContext, event: &SafeAuditEvent) -> Result<(), RuntimeError> {
        if matches!(
            event.kind,
            RawAuditEventKind::Created | RawAuditEventKind::Updated
        ) {
            self.0
                .lock()
                .expect("enrollment audit lock")
                .push(event.entity.clone());
        }
        Ok(())
    }
}

impl EnrollmentAudit {
    fn take(&self) -> Vec<String> {
        let mut events = std::mem::take(&mut *self.0.lock().expect("enrollment audit lock"));
        events.sort_unstable();
        events
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    assert!(
        args.len() == 3 && matches!(args[2].as_str(), "install" | "first" | "restart"),
        "Usage: school-enrollment-core-workspace <sqlite-file> <install|first|restart>"
    );
    let mut context = service_runtime(ServiceRuntimeConfig {
        database_url: args[1].clone(),
    })
    .await?;
    let audit = EnrollmentAudit::default();
    context.set_custom_event_sink(audit.clone());
    if args[2] == "install" {
        assert!(audit.take().is_empty());
        println!("ENROLLMENT_INSTALL_READY");
        return Ok(());
    }

    let first = args[2] == "first";
    context.ensure_schema().await?;
    let seeded = audit.take();
    if first {
        assert_eq!(seeded, ["Platform", "SchoolType", "SchoolType"]);
    } else {
        assert!(seeded.is_empty());
    }
    context.ensure_schema().await?;
    assert!(
        audit.take().is_empty(),
        "repeat startup writes must be absent"
    );

    let platform = Q::platforms()
        .limit(2)
        .comment("Find the enrollment platform")
        .purpose("Enroll pupils in the district")
        .execute_for_list(&context)
        .await?;
    assert_eq!(platform.len(), 1);
    assert_eq!((platform[0].id(), platform[0].version()), (1, 1));
    let types = Q::school_types()
        .order_by_id_asc()
        .limit(3)
        .comment("Read the seeded school types")
        .purpose("Select a school type for enrollment")
        .execute_for_list(&context)
        .await?;
    assert_eq!(
        types
            .iter()
            .map(|row| (row.id(), row.version()))
            .collect::<Vec<_>>(),
        [(1001, 1), (1002, 1)]
    );

    if first {
        let mut school = Q::schools()
            .comment("Create Riverside Primary")
            .purpose("Register a school and its first two pupils")
            .new_entity(&context)?;
        school.update_platform_id(platform[0].id());
        school.update_school_type_to_primary();
        school.update_name("Riverside Primary");
        for name in ["Alice", "Bob"] {
            let mut pupil = Q::students()
                .comment("Prepare the pupil record")
                .purpose("Join the enrollment graph")
                .new_entity(&context)?;
            pupil.update_school_id(school.id());
            pupil.update_name(name);
            school.include_pending_mutations_from(&pupil)?;
        }
        school
            .audit_as("Register the school and enroll both pupils")
            .save(&context)
            .await?;
    }
    let enrolled = audit.take();
    if first {
        assert_eq!(enrolled, ["School", "Student", "Student"]);
    } else {
        assert!(
            enrolled.is_empty(),
            "restart must preserve the enrollment graph"
        );
    }
    let schools = Q::schools()
        .with_name_is("Riverside Primary")
        .limit(2)
        .select_student_list()
        .comment("Load the school's roster")
        .purpose("Confirm both enrollments committed together")
        .execute_for_list(&context)
        .await?;
    assert_eq!(schools.len(), 1);
    let school = &schools[0];
    assert_eq!(school.id(), 1);
    assert_eq!(school.version(), 1);
    let relation = school.student_list();
    let roster = relation.value().expect("generated roster relation");
    let mut names: Vec<_> = roster
        .iter()
        .map(|pupil| {
            assert_eq!(pupil.version(), 1);
            pupil.name()
        })
        .collect();
    names.sort_unstable();
    assert_eq!(names, ["Alice", "Bob"]);

    if first {
        let mut pupil = Q::students()
            .comment("Prepare an invalid pupil")
            .purpose("Prevent invalid enrollments")
            .new_entity(&context)?;
        pupil.update_school_id(school.id());
        let rejected = pupil
            .audit_as("Reject an enrollment without the required pupil name")
            .save(&context)
            .await
            .expect_err("missing pupil name");
        let diagnosis = rejected.to_string().to_lowercase();
        assert!(
            diagnosis.contains("required") && diagnosis.contains("name"),
            "{rejected}"
        );
        assert!(
            audit.take().is_empty(),
            "rejection must emit no committed audit"
        );
    }
    println!(
        "ENROLLMENT_RESULT={}",
        serde_json::json!({
            "round": args[2], "bootstrapAudits": seeded.len(), "businessAudits": enrolled.len(),
            "schoolId": school.id(), "students": names, "invalidPupilRejected": first,
        })
    );
    Ok(())
}
