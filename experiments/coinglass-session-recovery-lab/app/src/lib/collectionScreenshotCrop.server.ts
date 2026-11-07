import { deflateSync, inflateSync } from "node:zlib";

const SIGNATURE = Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]);
const CRC = Array.from({ length: 256 }, (_, value) => {
  let result = value;
  for (let bit = 0; bit < 8; bit++)
    result = result & 1 ? 0xedb88320 ^ (result >>> 1) : result >>> 1;
  return result >>> 0;
});
function checksum(bytes: Buffer) {
  let value = 0xffffffff;
  for (const byte of bytes) value = CRC[(value ^ byte) & 255] ^ (value >>> 8);
  return (value ^ 0xffffffff) >>> 0;
}
function chunk(type: string, bytes: Buffer) {
  const data = Buffer.concat([Buffer.from(type), bytes]);
  const header = Buffer.alloc(4),
    tail = Buffer.alloc(4);
  header.writeUInt32BE(bytes.length);
  tail.writeUInt32BE(checksum(data));
  return Buffer.concat([header, data, tail]);
}
const bad = () => new Error("לא ניתן לחתוך את צילום המקור באופן מאומת; התיבה לא שונתה.");
export type ScreenshotCrop = {
  sourceWidth: number;
  sourceHeight: number;
  cropX: number;
  cropY: number;
  width: number;
  height: number;
};

/** Lossless pixel crop of an authenticated source screenshot. No resampling,
 * redraw, font substitution or third-party/native image dependency. */
export function cropSourceScreenshot(bytes: Buffer, box: ScreenshotCrop): Buffer {
  if (
    bytes.length > 8 * 1024 * 1024 ||
    !bytes.subarray(0, 8).equals(SIGNATURE) ||
    [box.sourceWidth, box.sourceHeight, box.cropX, box.cropY, box.width, box.height].some(
      (value) => !Number.isInteger(value),
    ) ||
    box.sourceWidth < 1 ||
    box.sourceWidth > 4096 ||
    box.sourceHeight < 1 ||
    box.sourceHeight > 4000 ||
    box.width < 1 ||
    box.height < 1 ||
    box.cropX < 0 ||
    box.cropY < 0 ||
    box.cropX + box.width > box.sourceWidth ||
    box.cropY + box.height > box.sourceHeight
  )
    throw bad();
  let header: Buffer | undefined;
  const encoded: Buffer[] = [];
  const colorChunks: Buffer[] = [];
  let ended = false;
  for (let offset = 8; offset < bytes.length;) {
    if (offset + 12 > bytes.length) throw bad();
    const length = bytes.readUInt32BE(offset),
      end = offset + 12 + length;
    if (length > 8 * 1024 * 1024 || end > bytes.length) throw bad();
    const type = bytes.subarray(offset + 4, offset + 8).toString("ascii"),
      data = bytes.subarray(offset + 8, offset + 8 + length);
    if (
      checksum(bytes.subarray(offset + 4, offset + 8 + length)) !==
      bytes.readUInt32BE(offset + 8 + length)
    )
      throw bad();
    if (type === "IHDR") {
      if (header || offset !== 8 || length !== 13) throw bad();
      header = Buffer.from(data);
    } else if (type === "IDAT") {
      if (!header) throw bad();
      encoded.push(data);
    } else if (type === "IEND") {
      if (length !== 0 || end !== bytes.length) throw bad();
      ended = true;
    } else if (["gAMA", "cHRM", "sRGB", "iCCP"].includes(type)) {
      if (length > 65536) throw bad();
      colorChunks.push(Buffer.from(bytes.subarray(offset, end)));
    } else if (type[0] === type[0].toUpperCase()) throw bad();
    offset = end;
  }
  if (
    !header ||
    !ended ||
    !encoded.length ||
    header.readUInt32BE(0) !== box.sourceWidth ||
    header.readUInt32BE(4) !== box.sourceHeight ||
    header[8] !== 8 ||
    ![2, 6].includes(header[9]) ||
    header[10] !== 0 ||
    header[11] !== 0 ||
    header[12] !== 0
  )
    throw bad();
  const channels = header[9] === 6 ? 4 : 3,
    stride = box.sourceWidth * channels,
    size = (stride + 1) * box.sourceHeight;
  let raw: Buffer;
  try {
    raw = inflateSync(Buffer.concat(encoded), { maxOutputLength: size });
  } catch {
    throw bad();
  }
  if (raw.length !== size) throw bad();
  const cropped = Buffer.alloc((box.width * channels + 1) * box.height);
  let previous = Buffer.alloc(stride);
  const paeth = (a: number, b: number, c: number) => {
    const p = a + b - c,
      x = Math.abs(p - a),
      y = Math.abs(p - b),
      z = Math.abs(p - c);
    return x <= y && x <= z ? a : y <= z ? b : c;
  };
  for (let y = 0; y < box.sourceHeight; y++) {
    const row = Buffer.alloc(stride),
      start = y * (stride + 1),
      filter = raw[start];
    if (filter > 4) throw bad();
    for (let x = 0; x < stride; x++) {
      const a = x >= channels ? row[x - channels] : 0,
        b = previous[x],
        c = x >= channels ? previous[x - channels] : 0;
      const predictor =
        filter === 0
          ? 0
          : filter === 1
            ? a
            : filter === 2
              ? b
              : filter === 3
                ? Math.floor((a + b) / 2)
                : paeth(a, b, c);
      row[x] = (raw[start + 1 + x] + predictor) & 255;
    }
    if (y >= box.cropY && y < box.cropY + box.height)
      row.copy(
        cropped,
        (y - box.cropY) * (box.width * channels + 1) + 1,
        box.cropX * channels,
        (box.cropX + box.width) * channels,
      );
    previous = row;
  }
  header.writeUInt32BE(box.width, 0);
  header.writeUInt32BE(box.height, 4);
  return Buffer.concat([
    SIGNATURE,
    chunk("IHDR", header),
    ...colorChunks,
    chunk("IDAT", deflateSync(cropped)),
    chunk("IEND", Buffer.alloc(0)),
  ]);
}
