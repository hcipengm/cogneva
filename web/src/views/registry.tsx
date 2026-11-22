import { AlertsView } from '@/views/AlertsView';
import { AuditView } from '@/views/AuditView';
import { CapabilitiesView } from '@/views/CapabilitiesView';
import { ChangesView } from '@/views/ChangesView';
import { CommsView } from '@/views/CommsView';
import { DeliveryView } from '@/views/DeliveryView';
import { DiffView } from '@/views/DiffView';
import { EvalView } from '@/views/EvalView';
import { FilesView } from '@/views/FilesView';
import { FlowView } from '@/views/FlowView';
import { GovernanceView } from '@/views/GovernanceView';
import { InboxView } from '@/views/InboxView';
import { ObservabilityView } from '@/views/ObservabilityView';
import { RecordsView } from '@/views/RecordsView';
import { SearchView } from '@/views/SearchView';
import { SnapshotsView } from '@/views/SnapshotsView';
import { WikiView } from '@/views/WikiView';
import { OutputView } from '@/views/OutputView';
import { ProgressView } from '@/views/ProgressView';
import { PeopleView } from '@/views/PeopleView';
import { ProjectsView } from '@/views/ProjectsView';
import { ResourcesView } from '@/views/ResourcesView';
import { SettingsView } from '@/views/SettingsView';
import { SourceControlView } from '@/views/SourceControlView';
import { StorageView } from '@/views/StorageView';
import { TasksView } from '@/views/TasksView';
import { TimelineView } from '@/views/TimelineView';
import { VenuesView } from '@/views/VenuesView';
import { TokenView } from '@/views/TokenView';
import { WorkbenchView } from '@/views/WorkbenchView';
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
      // 工作台是四格与一条状态灯（投入 产出 能力 项目）。会话不在这一面：它的家
      // 是员工页——一段对话与一个智能体的上下文是一对一的，落在「谁」那一面上。
      return <WorkbenchView />;
    case 'people':
      return <PeopleView />;
    case 'venues':
      return <VenuesView />;
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
    case 'projects':
      return <ProjectsView />;
    case 'flow':
      return <FlowView />;
    case 'comms':
      return <CommsView />;
    case 'agents':
      // 智能体与员工合并成一面：读的是同一份名单，分开只会让同一批人两处各认
      // 一次。旧链接（收藏、侧栏交叉链接、状态栏）仍能打开，落到同一个页面。
      return <PeopleView />;
    case 'progress':
      return <ProgressView />;
    case 'changes':
      return <ChangesView />;
    case 'governance':
      return <GovernanceView />;
    case 'capabilities':
      return <CapabilitiesView />;
    case 'output':
      return <OutputView />;
    case 'snapshots':
      return <SnapshotsView />;
    case 'hooks':
      return <RecordsView path="/api/v1/hooks" what="钩子的增删查在，界面上没有编辑面" />;
    case 'plugins':
      return (
        <RecordsView
          path="/api/v1/plugins"
          what="插件列表在；上传与卸载这个构建只校验不装载"
        />
      );
    case 'workspaces':
      return <RecordsView path="/api/v1/workspaces" what="工作区列表与成员在，界面上没有入口" />;
    case 'action-plan':
      return <RecordsView path="/api/v1/action-plan" what="动作计划只在 API 上" />;
    case 'datasets':
      return <RecordsView path="/api/v1/eval/datasets" what="数据集列表与校验在，界面上没有入口" />;
    case 'search':
      return <SearchView />;
    case 'wiki':
      return <WikiView />;
    case 'observability':
      return <ObservabilityView />;
    case 'token':
      return <TokenView />;
    case 'resources':
      return <ResourcesView />;
    case 'storage':
      return <StorageView />;
    case 'eval':
      return <EvalView />;
    case 'timeline':
      return <TimelineView />;
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
