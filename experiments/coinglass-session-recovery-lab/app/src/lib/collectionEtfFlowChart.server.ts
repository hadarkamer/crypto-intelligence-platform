import { deflateSync } from "node:zlib";
import {
  compactMarketDollars,
  validatedEtfCandles,
  type EtfCandle,
} from "./collectionEtfTooltip.ts";

const WIDTH = 760;
const HEIGHT = 340;
const LEFT = 170;
const RIGHT = 36;
const TOP = 20;
const BOTTOM = 284;

/** Only verified daily net USD flows belong on this chart. There is no price,
 * total-assets series or inferred value. One shared linear scale includes zero. */
export function etfFlowChartGeometry(raw: EtfCandle[]) {
  const candles = validatedEtfCandles(raw);
  if (!candles || candles.some((row) => !Number.isFinite(row.inflowMusd * 1_000_000)))
    throw new Error("לא אומתו סכומי דולר ותאריכים לגרף ETF.");
  const nice = (amount: number) => {
    if (!amount) return 0;
    const order = 10 ** Math.floor(Math.log10(amount));
    if (!order) return amount;
    const rounded = [1, 2, 5, 10].find((step) => amount / order <= step)! * order;
    return Number.isFinite(rounded * 1_000_000) ? rounded : amount;
  };
  const low = -nice(Math.max(0, ...candles.map((row) => -row.inflowMusd)));
  const high = nice(Math.max(0, ...candles.map((row) => row.inflowMusd))) || (low ? 0 : 1);
  const y = (amount: number) => TOP + ((high - amount) / (high - low)) * (BOTTOM - TOP);
  const slot = (WIDTH - LEFT - RIGHT) / candles.length;
  return {
    width: WIDTH,
    height: HEIGHT,
    zeroY: y(0),
    bars: candles.map((row, index) => ({
      ...row,
      x: LEFT + slot * (index + 0.5),
      y: y(row.inflowMusd),
      width: slot * 0.6,
    })),
    ticks: Array.from({ length: 5 }, (_, index) => {
      const value = high - ((high - low) * index) / 4;
      return {
        value,
        y: y(value),
        label: compactMarketDollars(value).replace("−", "-"),
      };
    }),
  };
}

// A small numeric bitmap font keeps the server-generated PNG self-contained.
// Labels contain only source dates and the USD axis; no font/network dependency.
const GLYPHS: Record<string, string[]> = {
  "0": ["01110", "10001", "10011", "10101", "11001", "10001", "01110"],
  "1": ["00100", "01100", "00100", "00100", "00100", "00100", "01110"],
  "2": ["01110", "10001", "00001", "00010", "00100", "01000", "11111"],
  "3": ["11110", "00001", "00001", "01110", "00001", "00001", "11110"],
  "4": ["00010", "00110", "01010", "10010", "11111", "00010", "00010"],
  "5": ["11111", "10000", "10000", "11110", "00001", "00001", "11110"],
  "6": ["01110", "10000", "10000", "11110", "10001", "10001", "01110"],
  "7": ["11111", "00001", "00010", "00100", "01000", "01000", "01000"],
  "8": ["01110", "10001", "10001", "01110", "10001", "10001", "01110"],
  "9": ["01110", "10001", "10001", "01111", "00001", "00001", "01110"],
  "+": ["00000", "00100", "00100", "11111", "00100", "00100", "00000"],
  "-": ["00000", "00000", "00000", "11111", "00000", "00000", "00000"],
  ".": ["00000", "00000", "00000", "00000", "00000", "00110", "00110"],
  ",": ["00000", "00000", "00000", "00000", "00110", "00110", "00100"],
  $: ["00100", "01111", "10100", "01110", "00101", "11110", "00100"],
  K: ["10001", "10010", "10100", "11000", "10100", "10010", "10001"],
  M: ["10001", "11011", "10101", "10101", "10001", "10001", "10001"],
  B: ["11110", "10001", "10001", "11110", "10001", "10001", "11110"],
};
const CRC_TABLE = Array.from({ length: 256 }, (_, index) => {
  let crc = index;
  for (let bit = 0; bit < 8; bit++) crc = crc & 1 ? 0xedb88320 ^ (crc >>> 1) : crc >>> 1;
  return crc >>> 0;
});
function chunk(type: string, bytes: Buffer) {
  const data = Buffer.concat([Buffer.from(type), bytes]);
  let crc = 0xffffffff;
  for (const byte of data) crc = CRC_TABLE[(crc ^ byte) & 255] ^ (crc >>> 8);
  const length = Buffer.alloc(4),
    checksum = Buffer.alloc(4);
  length.writeUInt32BE(bytes.length);
  checksum.writeUInt32BE((crc ^ 0xffffffff) >>> 0);
  return Buffer.concat([length, data, checksum]);
}

/** Raster data is already supported by the editor and viewer. SVG/script image
 * permissions remain unchanged, and no paid screenshot provider is needed. */
export function renderEtfFlowChart(raw: EtfCandle[]): string {
  const chart = etfFlowChartGeometry(raw);
  const pixels = Buffer.alloc(WIDTH * HEIGHT * 3);
  const fill = (x: number, y: number, w: number, h: number, color: number[]) => {
    for (let row = Math.max(0, Math.round(y)); row < Math.min(HEIGHT, Math.round(y + h)); row++)
      for (let col = Math.max(0, Math.round(x)); col < Math.min(WIDTH, Math.round(x + w)); col++) {
        const at = (row * WIDTH + col) * 3;
        pixels[at] = color[0];
        pixels[at + 1] = color[1];
        pixels[at + 2] = color[2];
      }
  };
  const label = (text: string, x: number, y: number, align = "left") => {
    const scale = 3,
      size = text.length * 6 * scale - scale;
    let left = align === "right" ? x - size : align === "center" ? x - size / 2 : x;
    for (const char of text) {
      const glyph = GLYPHS[char];
      if (!glyph) throw new Error("תווית גרף ETF אינה נתמכת.");
      glyph.forEach((row, dy) =>
        [...row].forEach((on, dx) => {
          if (on === "1") fill(left + dx * scale, y + dy * scale, scale, scale, [210, 222, 236]);
        }),
      );
      left += 6 * scale;
    }
  };
  fill(0, 0, WIDTH, HEIGHT, [12, 21, 33]);
  for (const tick of chart.ticks) {
    fill(LEFT, tick.y, WIDTH - LEFT - RIGHT, 1, [40, 57, 76]);
    label(tick.label, LEFT - 14, tick.y - 10, "right");
  }
  fill(LEFT, chart.zeroY, WIDTH - LEFT - RIGHT, 2, [112, 132, 155]);
  for (const bar of chart.bars) {
    if (bar.inflowMusd !== 0)
      fill(
        bar.x - bar.width / 2,
        Math.min(bar.y, chart.zeroY),
        bar.width,
        Math.max(1, Math.abs(bar.y - chart.zeroY)),
        bar.inflowMusd > 0 ? [36, 196, 144] : [242, 70, 93],
      );
  }
  const labels = new Set(
    Array.from({ length: Math.min(6, chart.bars.length) }, (_, index) =>
      Math.round((index * (chart.bars.length - 1)) / (Math.min(6, chart.bars.length) - 1)),
    ),
  );
  for (const index of labels) {
    const bar = chart.bars[index];
    label(
      `${Number(bar.date.slice(8))}.${Number(bar.date.slice(5, 7))}`,
      bar.x,
      BOTTOM + 16,
      "center",
    );
  }
  const rows = Buffer.alloc((WIDTH * 3 + 1) * HEIGHT);
  for (let row = 0; row < HEIGHT; row++)
    pixels.copy(rows, row * (WIDTH * 3 + 1) + 1, row * WIDTH * 3, (row + 1) * WIDTH * 3);
  const header = Buffer.alloc(13);
  header.writeUInt32BE(WIDTH, 0);
  header.writeUInt32BE(HEIGHT, 4);
  header[8] = 8;
  header[9] = 2;
  const png = Buffer.concat([
    Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]),
    chunk("IHDR", header),
    chunk("IDAT", deflateSync(rows)),
    chunk("IEND", Buffer.alloc(0)),
  ]);
  return `data:image/png;base64,${png.toString("base64")}`;
}
