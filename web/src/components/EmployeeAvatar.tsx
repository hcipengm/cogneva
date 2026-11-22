// 员工头像：一张按工号确定性画出来的图，不是上传的素材。
//
// 只用 `employeeId` 就能画，因为工号是身份；`name` 只进无障碍文本与悬停提示，
// 改名字不会换脸。

import { avatarFor } from '../ui/avatar';
import { Note } from '../ui/Note';

export function EmployeeAvatar({
  employeeId,
  name,
  size = 32,
}: {
  employeeId: string;
  name?: string;
  size?: number;
}) {
  const face = avatarFor(employeeId, size);
  return (
    // 悬停提示走 `Note` 而不是原生 `title=`：`title` 由浏览器画在页面之外，
    // 选不中、复制不了，令牌面那条门也点名禁止 DOM 上再出现它。头像本身不是
    // 控件，所以 `Note` 就是这一格，不再套一层。
    <Note
      as="span"
      text={name ? `${name}（${employeeId}）` : employeeId}
      className="inline-flex shrink-0 items-center justify-center overflow-hidden rounded-full"
      style={{ width: size, height: size, backgroundColor: face.background }}
    >
      <img
        src={face.src}
        width={size}
        height={size}
        alt={name ? `${name} 的头像` : ''}
        loading="lazy"
        draggable={false}
      />
    </Note>
  );
}
