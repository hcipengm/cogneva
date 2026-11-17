-- 事实面的正本：一条只追加的事件流。
--
-- 「这一行是『发生了』还是『规定要这样』？」——发生了的进这张表；规定要这样的留在
-- git（values/原则/规格）；由前两者算出的都是投影（各自带水位、重建命令、重建测试）。
--
-- 信封字段每一个都写明「没有它做不了什么」，所以这里逐列注释。缺字段不是"以后再补"：
-- 那些字段推不出来，缺了就有一整类能力（续跑、归属、重放、UI 订阅）做不了。
CREATE TABLE IF NOT EXISTS cogneva_event (
    -- 全局唯一；带时间前缀的 ULID 更好，这里只要求唯一。
    event_id        TEXT        NOT NULL,
    -- **服务端**时钟：唯一的全局时间轴，窗口切分从这里截。
    at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- 闭集里的一个词：llm_call / task_transition / change_measured / change_candidate / …
    kind            TEXT        NOT NULL,
    -- 这条事件说的是谁。
    subject_kind    TEXT        NOT NULL,
    subject_id      TEXT        NOT NULL,
    -- 一件事的 id（一个 goal、一次变更、一次演练）：续跑靠它找断点。
    unit_of_work    TEXT        NOT NULL,
    -- 这件事走到哪一步：planned / generated / evaluated / verified / landed / deployed / nothing
    step            TEXT        NOT NULL,
    -- 这一步的结局：started / ok / failed / aborted / skipped
    outcome         TEXT        NOT NULL,
    -- 同一 step 的第几次尝试（从 1 起）：重试与重复工作靠它分开。
    attempt         INTEGER     NOT NULL DEFAULT 1,
    -- 写者给的去重键：崩溃重发不会让同一件事记两遍。
    idempotency_key TEXT        NOT NULL,
    -- 直接触发它的那条 event_id：因果链。
    causation_id    TEXT,
    -- 一起打包调用的戳：回答「这 N 条是一起打包花的钱」。
    batch_id        TEXT,
    -- 谁（人 / agent:planner / system / eval）：成本与责任归属都按它分组。
    actor           TEXT        NOT NULL,
    -- 哪个进程写的（pod + 镜像 rev + build id）：「值变了还是写者换了」靠它。
    writer          TEXT        NOT NULL,
    -- 租户/工作区边界。
    scope           TEXT        NOT NULL DEFAULT '',
    -- 这个 kind 的载荷格式版本：旧事件重放时按它解释。
    payload_version INTEGER     NOT NULL DEFAULT 1,
    -- 该 kind 自己的字段（第 9.1~9.4 节逐列映射的那些），存 text。
    --
    -- 为什么是 text 而不是 jsonb：正文与哈希相关的那部分要**逐字节**保留，jsonb 会
    -- 重排键、改空白，把它规范化掉（内容寻址与「擦除后仍能证明原字节」都靠这份字节）。
    -- 校验不靠类型，靠下面两条约束：半截 JSON 与不是对象的载荷在写入期就被拒。
    attributes      TEXT        NOT NULL DEFAULT '{}'
                    CONSTRAINT cogneva_event_attributes_is_json CHECK (attributes IS JSON)
                    CONSTRAINT cogneva_event_attributes_is_object
                        CHECK (jsonb_typeof(attributes::jsonb) = 'object'),
    PRIMARY KEY (event_id)
);

-- 同一件事的事件按**顺序身份**读：续跑、因果链都按 `event_id` 走，不按 `at`。
CREATE INDEX IF NOT EXISTS idx_cogneva_event_unit ON cogneva_event(unit_of_work, event_id);
CREATE INDEX IF NOT EXISTS idx_cogneva_event_subject ON cogneva_event(subject_kind, subject_id, event_id);
-- 每 kind 一个部分索引：读某一面时只碰自己那个。
CREATE INDEX IF NOT EXISTS idx_cogneva_event_kind ON cogneva_event(kind, event_id);
-- 时间只作可读与按窗口切分：单独一条索引，不参与排序。
CREATE INDEX IF NOT EXISTS idx_cogneva_event_at ON cogneva_event(at);
-- 幂等：同一写者的同一条事件只能有一条。
CREATE UNIQUE INDEX IF NOT EXISTS idx_cogneva_event_idem ON cogneva_event(writer, idempotency_key);
