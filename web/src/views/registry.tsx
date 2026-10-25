import { AgentsView } from '@/views/AgentsView';
import { AlertsView } from '@/views/AlertsView';
import { AuditView } from '@/views/AuditView';
import { ChangesView } from '@/views/ChangesView';
import { DeliveryView } from '@/views/DeliveryView';
import { DiffView } from '@/views/DiffView';
import { FilesView } from '@/views/FilesView';
import { FlowView } from '@/views/FlowView';
import { GovernanceView } from '@/views/GovernanceView';
import { InboxView } from '@/views/InboxView';
import { ObservabilityView } from '@/views/ObservabilityView';
import { ResourcesView } from '@/views/ResourcesView';
import { SettingsView } from '@/views/SettingsView';
import { SourceControlView } from '@/views/SourceControlView';
import { TasksView } from '@/views/TasksView';
import { WorkspaceView } from '@/views/WorkspaceView';
import { Empty } from '@/ui/primitives';

/**
 * 页签的键 → 组件。
 *
 * 键与 `ActivityId` 同名：页签的身份、左栏的身份、打开的哪一页是同一个词，
 * 才不会出现「点了告警，开出来的是另一面」。
 *
 * 用一支 switch 而不是一张「名字 → 组件」的表：表把渲染点藏进数据里，静态
 * 读的人看不到哪一面真的被画出来了；switch 里每一面都是一个能被搜到的标签。
 */
export function ViewFor({ view }: { view: string }) {
  switch (view) {
    case 'workspace':
      return <WorkspaceView />;
    case 'files':
      return <FilesView />;
    case 'scm':
      return <SourceControlView />;
    case 'diff':
      return <DiffView />;
    case 'inbox':
      return <InboxView />;
    case 'alerts':
      return <AlertsView />;
    case 'tasks':
      return <TasksView />;
    case 'flow':
      return <FlowView />;
    case 'agents':
      return <AgentsView />;
    case 'changes':
      return <ChangesView />;
    case 'governance':
      return <GovernanceView />;
    case 'observability':
      return <ObservabilityView />;
    case 'resources':
      return <ResourcesView />;
    case 'delivery':
      return <DeliveryView />;
    case 'audit':
      return <AuditView />;
    case 'settings':
      return <SettingsView />;
    default:
      return <Empty>没有叫「{view}」的这一页。</Empty>;
  }
}
