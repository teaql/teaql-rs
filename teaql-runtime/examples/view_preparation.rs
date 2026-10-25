//! Run with `cargo run -p teaql-runtime --example view_preparation`.
//! No listener, document service, key provider or Blob implementation is installed.
use std::sync::Arc;
use teaql_core::Value;
use teaql_runtime::{
    EntityRuntimeState, UserContext, ViewAuthorization, ViewEntity, ViewEntityTarget,
    ViewErrorCode, ViewField, ViewFieldDecision, ViewIdentity, ViewInput, ViewOptions, ViewPath,
    ViewPreparationService,
};

struct Policy;
impl ViewAuthorization for Policy {
    fn identity(&self, _: &UserContext) -> Result<ViewIdentity, ViewErrorCode> {
        ViewIdentity::new("terminal", "reader", "example")
    }
    fn can_read_entity(
        &self,
        _: &UserContext,
        _: &ViewEntityTarget,
        _: &ViewPath,
    ) -> Result<bool, ViewErrorCode> {
        Ok(true)
    }
    fn field(
        &self,
        _: &UserContext,
        _: &ViewEntityTarget,
        field: &str,
        _: &ViewPath,
    ) -> Result<ViewFieldDecision, ViewErrorCode> {
        Ok(if field == "private_note" {
            ViewFieldDecision::Hidden
        } else {
            ViewFieldDecision::value()
        })
    }
}
struct ResultEntity {
    id: Value,
    version: Value,
    title: Value,
    state: EntityRuntimeState,
}
impl ViewEntity for ResultEntity {
    fn target(&self) -> ViewEntityTarget {
        ViewEntityTarget {
            entity: "TerminalResult".into(),
            id: Some(42),
            revision: Some(1),
        }
    }
    fn loaded_fields(&self) -> Vec<ViewField> {
        ["id", "version", "title", "private_note"]
            .into_iter()
            .map(|name| ViewField::new(name, name))
            .collect()
    }
    fn read_field(&self, field: &str) -> Result<ViewInput<'_>, ViewErrorCode> {
        Ok(ViewInput::Value(match field {
            "id" => &self.id,
            "version" => &self.version,
            "title" => &self.title,
            "private_note" => panic!("hidden business getter was called"),
            _ => return Err(ViewErrorCode::ResultUnsupported),
        }))
    }
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut context = UserContext::new();
    ViewPreparationService::new(Arc::new(Policy)).install_into(&mut context)?;
    context.validate_capabilities()?;
    let entity = ResultEntity {
        id: 42_u64.into(),
        version: 1_i64.into(),
        title: "prepared in a terminal".into(),
        state: EntityRuntimeState::default(),
    };
    let options = ViewOptions::read_only();
    assert!(entity.state.current_change_set().is_empty());
    let prepared = context
        .prepare_view(ViewInput::Entity(&entity), &options)
        .await?;
    assert!(entity.state.current_change_set().is_empty());
    assert!(prepared.document_context().is_none());
    assert!(std::ptr::eq(
        context.reuse_prepared_view(&prepared, &options).await?,
        &prepared
    ));
    assert_eq!(entity.title, Value::Text("prepared in a terminal".into()));
    println!("{}", serde_json::to_string(prepared.value())?);
    Ok(())
}
