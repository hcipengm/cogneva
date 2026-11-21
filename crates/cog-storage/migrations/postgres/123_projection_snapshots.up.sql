-- 投影的水位快照：重建 = 快照 + 增量。
--
-- 一条投影从零重放要读完整条流；流只增不改，那个代价随流长线性增长，而投影的读面
-- 只关心「最近这段」。快照把「到 event_id 为止的状态」存一份，重建从快照之后开始，
-- 于是重建耗时随「快照间隔」变化，不随流长变化——这是 S29 的判据本身。
--
-- 三列说明来源：`projection` 是哪张投影，`event_id` 是水位（重建从这一条之后开始），
-- `state` 是那一刻的折叠结果（JSON 文本，由投影自己解释）。
CREATE TABLE IF NOT EXISTS projection_snapshot (
    projection   TEXT        NOT NULL,
    event_id     TEXT        NOT NULL,
    taken_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    state        TEXT        NOT NULL,
    PRIMARY KEY (projection, event_id),
    CONSTRAINT projection_snapshot_state_is_json CHECK (state IS JSON)
);

CREATE INDEX IF NOT EXISTS idx_projection_snapshot_newest
    ON projection_snapshot (projection, taken_at DESC);
