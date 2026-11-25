// Renders spec.json into one .docx per project part (A4, matches the original question docs).
const fs = require("fs");
const path = require("path");
const {
  Document, Packer, Paragraph, TextRun, HeadingLevel, Table, TableRow, TableCell, WidthType,
  ShadingType, BorderStyle, ImageRun, AlignmentType, LevelFormat, PageBreak, Footer, PageNumber,
} = require("docx");

const spec = JSON.parse(fs.readFileSync(path.join(__dirname, "spec.json"), "utf8"));
const OUT = path.join(__dirname, "..", "..", "docs", "answers");
fs.mkdirSync(OUT, { recursive: true });

const PAGE_W = 11909, PAGE_H = 16834, MARGIN = 1080;
const CONTENT_W = PAGE_W - 2 * MARGIN;            // 9749 DXA
const NAVY = "1F3864", GREY = "595959";

function pngSize(file) {
  const b = fs.readFileSync(file);
  return { w: b.readUInt32BE(16), h: b.readUInt32BE(20), data: b };
}

const label = (text, color = NAVY) =>
  new Paragraph({ spacing: { before: 140, after: 40 }, keepNext: true,
                  children: [new TextRun({ text, bold: true, size: 18, color })] });

function block(text, fill) {
  let lines = text.replace(/\t/g, "    ").split("\n");
  const longest = Math.max(...lines.map(l => l.length));
  const size = longest <= 95 ? 16 : longest <= 110 ? 14 : 12;     // half-points: 8 / 7 / 6 pt
  const maxChars = 128;
  lines = lines.map(l => (size === 12 && l.length > maxChars ? l.slice(0, maxChars - 1) + "…" : l));
  const border = { style: BorderStyle.SINGLE, size: 4, color: "D0D7E2" };
  return new Table({
    width: { size: CONTENT_W, type: WidthType.DXA }, columnWidths: [CONTENT_W],
    rows: [new TableRow({ children: [new TableCell({
      width: { size: CONTENT_W, type: WidthType.DXA },
      shading: { fill, type: ShadingType.CLEAR, color: "auto" },
      borders: { top: border, bottom: border, left: border, right: border },
      margins: { top: 80, bottom: 80, left: 120, right: 120 },
      children: lines.map(l => new Paragraph({ spacing: { before: 0, after: 0, line: 228 },
        children: [new TextRun({ text: l.length ? l : " ", font: "Courier New", size })] })),
    })] })],
  });
}

function screenshotBox(text) {
  const dash = { style: BorderStyle.DASHED, size: 8, color: "C9A227" };
  return new Table({
    width: { size: CONTENT_W, type: WidthType.DXA }, columnWidths: [CONTENT_W],
    rows: [new TableRow({ children: [new TableCell({
      width: { size: CONTENT_W, type: WidthType.DXA },
      shading: { fill: "FFF8E1", type: ShadingType.CLEAR, color: "auto" },
      borders: { top: dash, bottom: dash, left: dash, right: dash },
      margins: { top: 120, bottom: 120, left: 160, right: 160 },
      children: [
        new Paragraph({ spacing: { after: 60 }, children: [
          new TextRun({ text: "SCREENSHOT — paste here", bold: true, size: 18, color: "8A6D00" })] }),
        new Paragraph({ children: [new TextRun({ text: "Capture: " + text, size: 18, color: "6B5600" })] }),
        new Paragraph({ children: [new TextRun({ text: " ", size: 18 })] }),
        new Paragraph({ children: [new TextRun({ text: " ", size: 18 })] }),
      ],
    })] })],
  });
}

function answerParas(text) {
  return text.split("\n").filter(Boolean).map(line => {
    const bullet = line.startsWith("• ");
    return new Paragraph({
      ...(bullet ? { numbering: { reference: "bullets", level: 0 } } : {}),
      spacing: { after: 80, line: 276 },
      children: [new TextRun({ text: bullet ? line.slice(2) : line, size: 21 })],
    });
  });
}

function section(s, n) {
  const kids = [new Paragraph({ heading: HeadingLevel.HEADING_1, children: [new TextRun(/^\d+\. /.test(s.title) ? s.title : `Q${n}. ${s.title}`)] })];
  kids.push(label("Answer"), ...answerParas(s.answer));
  if (s.code) kids.push(label(s.code_label), block(s.code, "F3F4F6"));
  if (s.output) kids.push(label(s.out_label), block(s.output, "EEF4FB"));
  if (s.image) {
    const img = pngSize(path.join(spec.figures, s.image));
    const wPx = 560, hPx = Math.round(img.h * wPx / img.w);
    kids.push(label("Chart"), new Paragraph({ alignment: AlignmentType.CENTER, children: [
      new ImageRun({ type: "png", data: img.data, transformation: { width: wPx, height: hPx },
                     altText: { title: s.image, description: s.title, name: s.image } })] }));
  }
  if (s.shot) kids.push(new Paragraph({ spacing: { before: 120 }, children: [] }), screenshotBox(s.shot));
  return kids;
}

for (const d of spec.docs) {
  const children = [
    new Paragraph({ heading: HeadingLevel.TITLE, children: [new TextRun(d.title)] }),
    new Paragraph({ spacing: { after: 120 }, children: [new TextRun({ text: d.intro, italics: true, size: 20, color: GREY })] }),
    new Paragraph({ spacing: { after: 200 }, children: [new TextRun({ size: 20, color: GREY,
      text: "Each question shows the answer, the exact code, the real output from the run on this machine, and a box for the screenshot of the code and output." })] }),
  ];
  d.sections.forEach((s, i) => {
    if (i > 0) children.push(new Paragraph({ children: [new PageBreak()] }));
    children.push(...section(s, i + 1));
  });

  const doc = new Document({
    creator: "bank-distributed-ml", title: d.title,
    styles: {
      default: { document: { run: { font: "Arial", size: 21 } } },
      paragraphStyles: [
        { id: "Title", name: "Title", basedOn: "Normal", next: "Normal", quickFormat: true,
          run: { font: "Arial", size: 36, bold: true, color: NAVY },
          paragraph: { spacing: { before: 0, after: 160 }, keepNext: true } },
        { id: "Heading1", name: "Heading 1", basedOn: "Normal", next: "Normal", quickFormat: true,
          run: { font: "Arial", size: 26, bold: true, color: NAVY },
          paragraph: { spacing: { before: 120, after: 120 }, keepNext: true, outlineLevel: 0 } },
      ],
    },
    numbering: { config: [{ reference: "bullets", levels: [{ level: 0, format: LevelFormat.BULLET, text: "•",
      alignment: AlignmentType.LEFT, style: { paragraph: { indent: { left: 540, hanging: 270 } } } }] }] },
    sections: [{
      properties: { page: { size: { width: PAGE_W, height: PAGE_H }, margin: { top: MARGIN, bottom: MARGIN, left: MARGIN, right: MARGIN } } },
      footers: { default: new Footer({ children: [new Paragraph({ alignment: AlignmentType.CENTER, children: [
        new TextRun({ text: d.title + "  ·  page ", size: 16, color: GREY }),
        new TextRun({ children: [PageNumber.CURRENT], size: 16, color: GREY })] })] }) },
      children,
    }],
  });
  Packer.toBuffer(doc).then(buf => {
    const f = path.join(OUT, d.file);
    fs.writeFileSync(f, buf);
    console.log("wrote", f, (buf.length / 1024).toFixed(0) + " KB");
  });
}
