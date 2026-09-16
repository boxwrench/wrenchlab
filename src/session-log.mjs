import fs from 'node:fs'
import { zstdDecompressSync } from 'node:zlib'

const ZSTD_MAGIC = 0xfd2fb528

// Read newline-delimited session logs, including independently decodable zstd
// frames used by the durable session store.
function scanZstdFrames(buffer) {
  const frames = []
  let offset = 0
  while (offset < buffer.length) {
    const start = offset
    if (buffer.length - offset < 4 || buffer.readUInt32LE(offset) !== ZSTD_MAGIC) break
    offset += 4
    if (offset === buffer.length) break
    const descriptor = buffer.readUInt8(offset++)
    if ((descriptor & 24) !== 0) break
    const contentSizeFlag = descriptor >>> 6
    const singleSegment = (descriptor & 32) !== 0
    const checksum = (descriptor & 4) !== 0
    const dictionaryFlag = descriptor & 3
    const dictionaryBytes = dictionaryFlag === 3 ? 4 : dictionaryFlag
    const contentSizeBytes = contentSizeFlag === 0 ? singleSegment ? 1 : 0 : 1 << contentSizeFlag
    const headerBytes = (singleSegment ? 0 : 1) + dictionaryBytes + contentSizeBytes
    if (buffer.length - offset < headerBytes) break
    offset += headerBytes
    let complete = false
    while (offset + 3 <= buffer.length) {
      const block = buffer.readUIntLE(offset, 3)
      offset += 3
      const last = (block & 1) !== 0
      const type = (block >>> 1) & 3
      const size = block >>> 3
      if (type === 3 || offset + (type === 1 ? 1 : size) > buffer.length) break
      offset += type === 1 ? 1 : size
      if (last) {
        if (checksum && offset + 4 > buffer.length) break
        offset += checksum ? 4 : 0
        complete = true
        break
      }
    }
    if (!complete) break
    frames.push({ start, end: offset })
  }
  return frames
}

export function readSessionLog(file) {
  const bytes = fs.readFileSync(file)
  if (!file.endsWith('.jsonl.zstd')) return bytes.toString('utf8')
  return scanZstdFrames(bytes)
    .map(frame => zstdDecompressSync(bytes.subarray(frame.start, frame.end)).toString('utf8'))
    .join('')
}
