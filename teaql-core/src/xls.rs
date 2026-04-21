use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct XlsBlock {
    pub page: String,
    pub top: i32,
    pub bottom: i32,
    pub left: i32,
    pub right: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub style_refer_block: Option<Box<XlsBlock>>,
    #[serde(skip_serializing_if = "serde_json::Value::is_null")]
    pub value: serde_json::Value,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub properties: BTreeMap<String, serde_json::Value>,
}

impl XlsBlock {
    pub fn new(
        page: impl Into<String>,
        x: i32,
        y: i32,
        value: impl Into<serde_json::Value>,
    ) -> Self {
        Self {
            page: page.into(),
            top: y,
            bottom: y,
            left: x,
            right: x,
            style_refer_block: None,
            value: value.into(),
            properties: BTreeMap::new(),
        }
    }

    pub fn from_context(
        context: &XlsBlockBuildContext,
        value: impl Into<serde_json::Value>,
    ) -> Self {
        Self::new(context.page.clone(), context.x, context.y, value)
    }

    pub fn region(mut self, left: i32, top: i32, right: i32, bottom: i32) -> Self {
        self.left = left;
        self.top = top;
        self.right = right;
        self.bottom = bottom;
        self
    }

    pub fn span(mut self, width: i32, height: i32) -> Self {
        self.right = self.left + width.saturating_sub(1);
        self.bottom = self.top + height.saturating_sub(1);
        self
    }

    pub fn value(mut self, value: impl Into<serde_json::Value>) -> Self {
        self.value = value.into();
        self
    }

    pub fn add_property(
        mut self,
        name: impl Into<String>,
        value: impl Into<serde_json::Value>,
    ) -> Self {
        self.properties.insert(name.into(), value.into());
        self
    }

    pub fn set_property(&mut self, name: impl Into<String>, value: impl Into<serde_json::Value>) {
        self.properties.insert(name.into(), value.into());
    }

    pub fn style(mut self, style: XlsBlock) -> Self {
        self.style_refer_block = Some(Box::new(style));
        self
    }

    pub fn width(&self) -> i32 {
        self.right - self.left + 1
    }

    pub fn height(&self) -> i32 {
        self.bottom - self.top + 1
    }

    pub fn contains(&self, x: i32, y: i32) -> bool {
        x >= self.left && x <= self.right && y >= self.top && y <= self.bottom
    }

    pub fn to_json_value(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("XlsBlock serialization cannot fail")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct XlsBlockBuildContext {
    pub page: String,
    pub start_x: i32,
    pub x: i32,
    pub y: i32,
}

impl XlsBlockBuildContext {
    pub fn new(page: impl Into<String>, x: i32, y: i32) -> Self {
        let x = x.max(0);
        let y = y.max(0);
        Self {
            page: page.into(),
            start_x: x,
            x,
            y,
        }
    }

    pub fn page(page: impl Into<String>) -> Self {
        Self::new(page, 0, 0)
    }

    pub fn next(&self) -> Self {
        Self {
            page: self.page.clone(),
            start_x: self.start_x,
            x: self.x + 1,
            y: self.y,
        }
    }

    pub fn new_line(&self) -> Self {
        Self {
            page: self.page.clone(),
            start_x: self.start_x,
            x: 0,
            y: self.y + 1,
        }
    }

    pub fn next_line(&self) -> Self {
        Self {
            page: self.page.clone(),
            start_x: self.start_x,
            x: self.start_x,
            y: self.y + 1,
        }
    }

    pub fn to_block(&self, value: impl Into<serde_json::Value>) -> XlsBlock {
        XlsBlock::from_context(self, value)
    }
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct XlsPage {
    pub name: String,
    pub blocks: Vec<XlsBlock>,
}

impl XlsPage {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            blocks: Vec::new(),
        }
    }

    pub fn add_block(mut self, block: XlsBlock) -> Self {
        self.blocks.push(block);
        self
    }

    pub fn push_block(&mut self, block: XlsBlock) {
        self.blocks.push(block);
    }

    pub fn block_at(&self, x: i32, y: i32) -> Option<&XlsBlock> {
        self.blocks.iter().find(|block| block.contains(x, y))
    }
}

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct XlsWorkbook {
    pub pages: Vec<XlsPage>,
}

impl XlsWorkbook {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_page(mut self, page: XlsPage) -> Self {
        self.pages.push(page);
        self
    }

    pub fn push_page(&mut self, page: XlsPage) {
        self.pages.push(page);
    }

    pub fn page(&self, name: &str) -> Option<&XlsPage> {
        self.pages.iter().find(|page| page.name == name)
    }

    pub fn to_json_value(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("XlsWorkbook serialization cannot fail")
    }
}
