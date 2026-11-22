// 员工头像：一张按工号确定性画出来的图，不是上传的素材。
//
// 只用 `employeeId` 就能画，因为工号是身份；`name` 只进无障碍文本与悬停提示，
// 改名字不会换脸。

import { avatarFor } from '../ui/avatar';

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
    <span
      className="inline-flex shrink-0 items-center justify-center overflow-hidden rounded-full"
      style={{ width: size, height: size, backgroundColor: face.background }}
      title={name ? `${name}（${employeeId}）` : employeeId}
    >
      <img
        src={face.src}
        width={size}
        height={size}
        alt={name ? `${name} 的头像` : ''}
        loading="lazy"
        draggable={false}
      />
    </span>
  );
}
