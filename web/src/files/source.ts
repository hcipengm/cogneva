import type { FileSource } from '@/api/client';

/**
 * 文件面的两棵树，以及它们的页签身份。
 *
 * 工作区沿用 `file:<路径>`：那是读者自己的可写目录，一条路径就是它的身份，加前缀
 * 只会让既有的搜索跳转多一处要改的形态。项目面是 `file@project:<路径>`——同一个
 * 相对路径在两棵树里是两个文件，`crates/x/src/lib.rs` 在工作区里指向草稿、在项目
 * 里指向仓库那一版，页签不加来源就会把一个当成另一个。
 */
export const SOURCE_LABEL: Record<FileSource, string> = {
  workspace: '我的工作区',
  project: '项目源码',
};

export const SOURCE_HINT: Record<FileSource, string> = {
  workspace: '自己的可写目录：新建、上传、保存都落在这里',
  project: '这个部署正在演化的那份源码，只读：想看改动就改到工作区，或走变更流程',
};

const PROJECT_PREFIX = 'file@project:';

export function fileTabId(source: FileSource, path: string): string {
  return source === 'project' ? `${PROJECT_PREFIX}${path}` : `file:${path}`;
}

export function parseFileTab(tabId: string): { source: FileSource; path: string } | null {
  if (tabId.startsWith(PROJECT_PREFIX)) {
    return { source: 'project', path: tabId.slice(PROJECT_PREFIX.length) };
  }
  if (tabId.startsWith('file:')) {
    return { source: 'workspace', path: tabId.slice('file:'.length) };
  }
  return null;
}
