/**
 * 这个构建是不是在演示模式下跑。
 *
 * 一个常量放一处：入口按它决定建不建真连接，对话面按它决定去不去读历史。
 * 两处各写一遍 `import.meta.env` 时，改一处忘另一处，表现是「连的是演示流，
 * 却在读真接口」——读失败被当成「这段对话没有记录」。
 */
export const USE_MOCK = import.meta.env.VITE_USE_MOCK === 'true';
