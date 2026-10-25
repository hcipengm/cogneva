/**
 * 最小的 ZIP 读取器。
 *
 * docx 与 xlsx 都是 ZIP 装着几份 XML。为这两种格式引一个打包库，就把「读一份
 * 表格」变成了「多一个需要长期跟进的依赖」；而浏览器自己已经会解压（`DecompressionStream`），
 * 缺的只是把中央目录读出来。
 *
 * 只做读、只做这两种文档里会出现的形状：不处理 ZIP64、不处理加密、不处理分卷。
 * 碰到这些直接报错——猜一个大小去解一段错的字节，比说「这份打不开」更坏。
 */

const SIG_EOCD = 0x06054b50;
const SIG_CENTRAL = 0x02014b50;
const SIG_LOCAL = 0x04034b50;

export class ZipError extends Error {}

export interface ZipEntry {
  name: string;
  bytes: Uint8Array;
}

export async function unzip(buffer: ArrayBuffer): Promise<Map<string, Uint8Array>> {
  const view = new DataView(buffer);
  const bytes = new Uint8Array(buffer);

  // 中央目录的尾部记录在文件末尾，后面可能跟着最长 64 KiB 的注释，所以从后往前找签名。
  let eocd = -1;
  const floor = Math.max(0, buffer.byteLength - 0xffff - 22);
  for (let at = buffer.byteLength - 22; at >= floor; at -= 1) {
    if (view.getUint32(at, true) === SIG_EOCD) {
      eocd = at;
      break;
    }
  }
  if (eocd < 0) throw new ZipError('这不是一个 ZIP 容器（没有找到目录尾）');

  const count = view.getUint16(eocd + 10, true);
  const directoryAt = view.getUint32(eocd + 16, true);
  if (directoryAt === 0xffffffff) throw new ZipError('ZIP64 容器暂不支持');

  const files = new Map<string, Uint8Array>();
  let cursor = directoryAt;

  for (let index = 0; index < count; index += 1) {
    if (view.getUint32(cursor, true) !== SIG_CENTRAL) {
      throw new ZipError(`第 ${index + 1} 个目录项签名不对，容器可能被截断`);
    }
    const method = view.getUint16(cursor + 10, true);
    const compressedSize = view.getUint32(cursor + 20, true);
    const uncompressedSize = view.getUint32(cursor + 24, true);
    const nameLength = view.getUint16(cursor + 28, true);
    const extraLength = view.getUint16(cursor + 30, true);
    const commentLength = view.getUint16(cursor + 32, true);
    const localAt = view.getUint32(cursor + 42, true);
    const name = new TextDecoder().decode(bytes.subarray(cursor + 46, cursor + 46 + nameLength));
    cursor += 46 + nameLength + extraLength + commentLength;

    if (compressedSize === 0xffffffff || uncompressedSize === 0xffffffff) {
      throw new ZipError(`ZIP64 容器暂不支持（${name}）`);
    }
    // 目录项没有内容，名字以 / 结尾的是目录。
    if (name.endsWith('/')) continue;

    if (view.getUint32(localAt, true) !== SIG_LOCAL) {
      throw new ZipError(`「${name}」的本地头签名不对`);
    }
    // 本地头里的名字长度与扩展长度可能与目录项不同，必须以本地头为准才算得出数据起点。
    const localNameLength = view.getUint16(localAt + 26, true);
    const localExtraLength = view.getUint16(localAt + 28, true);
    const start = localAt + 30 + localNameLength + localExtraLength;
    const payload = bytes.subarray(start, start + compressedSize);

    if (method === 0) {
      files.set(name, payload);
    } else if (method === 8) {
      files.set(name, await inflateRaw(payload));
    } else {
      throw new ZipError(`「${name}」用了不认识的压缩方式 ${method}`);
    }
  }

  return files;
}

/** 取一份 XML 条目并解码成字符串；缺了就按缺了报，不返回空串。 */
export function textEntry(files: Map<string, Uint8Array>, name: string): string {
  const entry = files.get(name);
  if (!entry) throw new ZipError(`容器里没有「${name}」`);
  return new TextDecoder('utf-8').decode(entry);
}

export function findEntry(files: Map<string, Uint8Array>, test: (name: string) => boolean): string | null {
  for (const name of files.keys()) if (test(name)) return name;
  return null;
}

async function inflateRaw(payload: Uint8Array): Promise<Uint8Array> {
  if (typeof DecompressionStream === 'undefined') {
    throw new ZipError('这个浏览器没有 DecompressionStream，解不开压缩条目');
  }
  // `deflate-raw` 是 ZIP 用的那种裸 deflate（没有 zlib 头）。被拷进新的
  // ArrayBuffer 是为了避开 SharedArrayBuffer / 视图偏移带来的类型限制。
  const copy = payload.slice().buffer;
  const stream = new Blob([copy]).stream().pipeThrough(new DecompressionStream('deflate-raw'));
  const inflated = await new Response(stream).arrayBuffer();
  return new Uint8Array(inflated);
}
