//! 外部 MCP 服务的工具，装进这一轮的工具表。
//!
//! MCP 客户端本来就在这个进程里（有端点就能连），但它的工具从来没有进过任何
//! 一张 `ToolRegistry`——于是模型能听懂「有一个外部服务」，却调不到它上面的
//! 任何一只工具。这里补的就是那一步：问一次 `list_tools`，把每一条转成注册表
//! 认得的一只工具，调用原样转回去。
//!
//! 名字带 `mcp__` 前缀：外部服务自己叫 `read_file` 的时候，不带前缀就会把平台
//! 那只替换掉（`register` 同名即替换），而模型会以为它读的是这一轮的树。前缀
//! 让两只同名工具在一张表里都活着，调用记录上也分得清哪一只是外面的。
//!
//! 每轮开头问一次，不是在启动时缓存一份：远端服务的工具表是会变的（进程重启、
//! 插件增减），缓存一份等于把「今天有什么」钉在进程启动的那一刻。问不到时这一
//! 轮没有外部工具，调用方据此如实说「连不上」，而不是留一张昨天的表。

use std::sync::Arc;

use cog_core::{McpClient, Tool, ToolHandler, ToolImplementation};

/// 外部工具名字的前缀。放进函数名要满足上游对工具名的字符集，所以用下划线。
pub const MCP_TOOL_PREFIX: &str = "mcp__";

/// 这一次问到的结果：连上没有、有哪些工具、连不上时为什么。
///
/// 「连上了但一只工具也没有」与「根本没连上」是两句话，界面上要分得开：前者是
/// 那台服务今天什么都没提供，后者是这一次调用根本没到它。
#[derive(Debug, Clone, Default)]
pub struct Listed {
    pub reachable: bool,
    pub tools: Vec<Tool>,
    pub error: Option<String>,
}

impl Listed {
    /// 递到界面与这一轮交付物上的那一格。
    pub fn report(&self) -> serde_json::Value {
        serde_json::json!({
            "reachable": self.reachable,
            "tools": self.tools.iter().map(|tool| tool.name.clone()).collect::<Vec<_>>(),
            "error": self.error,
        })
    }
}

/// 这一次问到的工具，转成注册表认得的形状；问不到时是一张空表并说明原因。
pub async fn tools(client: Arc<dyn McpClient>) -> Listed {
    let listed = match client.list_tools().await {
        Ok(listed) => listed,
        Err(error) => {
            tracing::warn!(
                error = %error,
                "the external MCP server could not be asked for its tools; this turn has none of them"
            );
            return Listed {
                reachable: false,
                tools: Vec::new(),
                error: Some(error.to_string()),
            };
        }
    };
    let tools: Vec<Tool> = listed
        .into_iter()
        .filter_map(|row| {
            let name = row.get("name").and_then(|value| value.as_str())?.trim();
            if name.is_empty() {
                return None;
            }
            let description = row
                .get("description")
                .and_then(|value| value.as_str())
                .unwrap_or("")
                .to_string();
            // 上游没给 schema 时给一份「什么也不收」的：空对象 schema 让一次
            // 无参调用仍然合法，而 `null` 会被一些上游当成缺字段。
            let parameters = row
                .get("inputSchema")
                .filter(|value| value.is_object())
                .cloned()
                .unwrap_or_else(|| serde_json::json!({ "type": "object", "properties": {} }));
            let remote = name.to_string();
            let called = remote.clone();
            let call_client = Arc::clone(&client);
            let handler: ToolHandler = Arc::new(move |arguments| {
                let client = Arc::clone(&call_client);
                let name = called.clone();
                Box::pin(async move { client.call_tool(&name, arguments).await })
            });
            Some(Tool {
                name: registered_name(&remote),
                description,
                parameters,
                implementation: ToolImplementation::Native(handler),
            })
        })
        .collect();
    Listed {
        reachable: true,
        tools,
        error: None,
    }
}

/// 一只外部工具在注册表里的名字。
///
/// 除了前缀，还把上游工具名里不属于 `[A-Za-z0-9_-]` 的字符换成下划线：有的服务
/// 用点或冒号给工具分组，而请求里的函数名只收这几个字符——改名的是调用面，
/// 转回去的仍是上游原名。
pub fn registered_name(remote: &str) -> String {
    let mut name = String::with_capacity(MCP_TOOL_PREFIX.len() + remote.len());
    name.push_str(MCP_TOOL_PREFIX);
    for ch in remote.chars() {
        name.push(if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
            ch
        } else {
            '_'
        });
    }
    name
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use cog_core::SFResult;

    /// 一台只认两支工具的假服务：记事本记下每一次调用，好让判据能核对转回去
    /// 的是上游原名，而不是加了前缀的那个。
    struct Fake {
        listed: Vec<serde_json::Value>,
        calls: std::sync::Mutex<Vec<(String, serde_json::Value)>>,
    }

    impl Fake {
        fn new(listed: Vec<serde_json::Value>) -> Arc<Self> {
            Arc::new(Self {
                listed,
                calls: std::sync::Mutex::new(Vec::new()),
            })
        }
    }

    #[async_trait]
    impl McpClient for Fake {
        async fn list_tools(&self) -> SFResult<Vec<serde_json::Value>> {
            Ok(self.listed.clone())
        }
        async fn call_tool(
            &self,
            name: &str,
            arguments: serde_json::Value,
        ) -> SFResult<serde_json::Value> {
            self.calls
                .lock()
                .unwrap()
                .push((name.to_string(), arguments.clone()));
            Ok(serde_json::json!({ "echo": arguments }))
        }
    }

    /// 两端都通：工具带着名字、说明与 schema 进表，调用转回上游原名。
    #[tokio::test]
    async fn a_listed_tool_is_registered_and_calls_back_under_its_own_name() {
        let fake = Fake::new(vec![serde_json::json!({
            "name": "search.docs",
            "description": "在文档里找",
            "inputSchema": { "type": "object", "properties": { "q": { "type": "string" } } },
        })]);
        let client: Arc<dyn McpClient> = fake.clone();
        let listed = tools(client).await;
        assert!(listed.reachable);
        let tools = listed.tools;
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].name, "mcp__search_docs");
        assert_eq!(tools[0].description, "在文档里找");
        assert_eq!(tools[0].parameters["properties"]["q"]["type"], "string");

        let ToolImplementation::Native(handler) = &tools[0].implementation else {
            panic!("an external tool must be a native handler");
        };
        let answer = handler(serde_json::json!({ "q": "你好" })).await.unwrap();
        assert_eq!(answer["echo"]["q"], "你好");
        // 上游收到的是它自己的名字，不是注册表里的那个。
        assert_eq!(fake.calls.lock().unwrap()[0].0, "search.docs");
    }

    /// 名字不带前缀会把平台自己的那只盖掉：一只外部的 `read_file` 会让模型以为
    /// 它读的是这一轮的树。
    #[test]
    fn an_external_name_cannot_shadow_a_platform_tool() {
        assert_eq!(registered_name("read_file"), "mcp__read_file");
        assert_ne!(registered_name("read_file"), "read_file");
        assert_eq!(registered_name("a.b:c"), "mcp__a_b_c");
    }

    /// 服务连不上时这一轮没有外部工具，但没有一条进来的工具被伪造出来。
    #[tokio::test]
    async fn an_unreachable_server_yields_no_tools() {
        struct Down;
        #[async_trait]
        impl McpClient for Down {
            async fn list_tools(&self) -> SFResult<Vec<serde_json::Value>> {
                Err(cog_core::SFError::Internal("连不上".into()))
            }
            async fn call_tool(
                &self,
                _name: &str,
                _arguments: serde_json::Value,
            ) -> SFResult<serde_json::Value> {
                unreachable!("列不出工具就不会有调用")
            }
        }
        let listed = tools(Arc::new(Down)).await;
        assert!(!listed.reachable);
        assert!(listed.tools.is_empty());
        assert!(listed.error.is_some(), "连不上时要把原因说出来");
    }

    /// 上游没给 schema 时给一份空对象 schema：无参调用仍然合法。
    #[tokio::test]
    async fn a_tool_without_a_schema_still_accepts_an_empty_call() {
        let listed = tools(Fake::new(vec![serde_json::json!({ "name": "ping" })])).await;
        let tools = listed.tools;
        assert_eq!(tools[0].parameters["type"], "object");
        assert!(tools[0].parameters["properties"]
            .as_object()
            .unwrap()
            .is_empty());
    }
}
