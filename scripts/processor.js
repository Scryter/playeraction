/**
 * Player.Action - Thumbnail, Teaser & Adaptive HLS Generator
 * Runs on GitHub Actions (ubuntu-latest) triggered by webhook after a video upload.
 *
 * Flow:
 *  1. Fetch exercises pending processing from the app API
 *  2. Download each MP4 from R2
 *  3. Generate thumb.jpg (frame at 1s) via FFmpeg
 *  4. Generate teaser.mp4 (first 10s, 720px wide) via FFmpeg
 *  5. Detect original resolution with ffprobe
 *  6. Generate multi-bitrate HLS for allowed quality heights (never upscaling)
 *  7. Upload thumb, teaser and all HLS files back to R2
 *  8. Delete the original MP4 from R2 (no longer needed - HLS replaces it)
 *  9. PATCH the app API to mark as processed (hlsProcessed=true)
 *
 * Quality heights are controlled per SaaS tier (videoQualityHeights field).
 * If empty, defaults to [720, 1080]. The processor never upscales beyond
 * the original video resolution.
 */
'use strict'

const { execSync, execFileSync } = require('child_process')
const { S3Client, GetObjectCommand, PutObjectCommand, DeleteObjectCommand } = require('@aws-sdk/client-s3')
const fs   = require('fs')
const path = require('path')
const os   = require('os')

const {
                R2_ACCOUNT_ID,
                R2_ACCESS_KEY_ID,
                R2_SECRET_ACCESS_KEY,
                R2_BUCKET_NAME,
                APP_URL,
                PROCESSOR_API_KEY,
} = process.env

if (!R2_ACCOUNT_ID || !PROCESSOR_API_KEY || !APP_URL) {
                console.error('X Missing required env vars. Check repository secrets.')
                process.exit(1)
}

const s3 = new S3Client({
                region: 'auto',
                endpoint: `https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
                credentials: {
                                            accessKeyId:     R2_ACCESS_KEY_ID,
                                            secretAccessKey: R2_SECRET_ACCESS_KEY,
                },
})

// --- Quality ladder config ----------------------------------------------------
// Defines ffmpeg parameters for each supported height
const QUALITY_CONFIG = {
                1080: { width: 1920, bandwidth: 4000000, bitrate: '4000k', maxrate: '4500k', bufsize: '9000k' },
                720:  { width: 1280, bandwidth: 1500000, bitrate: '1500k', maxrate: '1800k', bufsize: '3600k' },
                480:  { width:  854, bandwidth:  800000, bitrate:  '800k', maxrate: '1000k', bufsize: '2000k' },
                360:  { width:  640, bandwidth:  400000, bitrate:  '400k', maxrate:  '500k', bufsize: '1000k' },
}

const DEFAULT_HEIGHTS = [720, 1080]

// --- API helpers --------------------------------------------------------------

async function fetchPending() {
                const res = await fetch(`${APP_URL}/api/admin/video-processor`, {
                                            headers: { 'x-processor-key': PROCESSOR_API_KEY },
                })
                if (!res.ok) throw new Error(`Failed to fetch pending: ${res.status}`)
                const data = await res.json()
                return data.pending || []
}

async function updateProgress(id, progress) {
                try {
                                            await fetch(`${APP_URL}/api/admin/video-processor`, {
                                                                                        method: 'PATCH',
                                                                                        headers: {
                                                                                                                                                        'Content-Type': 'application/json',
                                                                                                                                                        'x-processor-key': PROCESSOR_API_KEY,
                                                                                                },
                                                                                        body: JSON.stringify({ id, processingProgress: progress }),
                                            })
                } catch {
                                            // Progress update failure is non-fatal - continue processing
                }
}

async function markDone({ id, originalVideoKey }) {
                const res = await fetch(`${APP_URL}/api/admin/video-processor`, {
                                            method: 'PATCH',
                                            headers: {
                                                                                        'Content-Type': 'application/json',
                                                                                        'x-processor-key': PROCESSOR_API_KEY,
                                            },
                                            body: JSON.stringify({ id, hlsProcessed: true, originalVideoKey, processingProgress: 100 }),
                })
                if (!res.ok) {
                                            const err = await res.json().catch(() => ({}))
                                            throw new Error(`PATCH failed for ${id}: ${err.error || res.status}`)
                }
}

// --- R2 helpers ---------------------------------------------------------------
async function downloadFromR2(key, destPath) {
                const { Body } = await s3.send(new GetObjectCommand({ Bucket: R2_BUCKET_NAME, Key: key }))
                await new Promise((resolve, reject) => {
                                            const out = fs.createWriteStream(destPath)
                                            Body.pipe(out)
                                            out.on('finish', resolve)
                                            out.on('error', reject)
                })
}

async function uploadToR2(key, filePath, contentType) {
                await s3.send(new PutObjectCommand({
                                            Bucket:      R2_BUCKET_NAME,
                                            Key:         key,
                                            Body:        fs.readFileSync(filePath),
                                            ContentType: contentType,
                }))
}

async function deleteFromR2(key) {
                await s3.send(new DeleteObjectCommand({ Bucket: R2_BUCKET_NAME, Key: key }))
}

// --- HLS master playlist builder ----------------------------------------------

function buildMasterPlaylist(variants) {
                const lines = ['#EXTM3U', '#EXT-X-VERSION:3', '']
                for (const v of variants) {
                                            lines.push(`#EXT-X-STREAM-INF:BANDWIDTH=${v.bandwidth},RESOLUTION=${v.resolution}`)
                                            lines.push(v.path)
                }
                return lines.join('\n')
}

// --- Detect original resolution -----------------------------------------------

function probeHeight(mp4Path) {
                try {
                                            const out = execSync(
                                                                                        `ffprobe -v error -select_streams v:0 -show_entries stream=height -of csv=p=0 "${mp4Path}"`,
                                                    { encoding: 'utf8' }
                                                                        ).trim()
                                            const h = parseInt(out)
                                            return Number.isFinite(h) && h > 0 ? h : 9999
                } catch {
                                            return 9999 // assume no cap if ffprobe fails
                }
}

// --- Core processor -----------------------------------------------------------
async function processExercise(exercise) {
                const { id, name, originalVideoKey, videoQualityHeights } = exercise
                const mp4Key = originalVideoKey

    if (!mp4Key) {
                            console.warn(`  ! Skipping "${name}" - no originalVideoKey`)
                            return
    }

    // Determine requested heights from tier config (default: [720, 1080])
    const requestedHeights = Array.isArray(videoQualityHeights) && videoQualityHeights.length > 0
                            ? videoQualityHeights.filter(h => QUALITY_CONFIG[h])
                                : DEFAULT_HEIGHTS

    const folder     = mp4Key.slice(0, mp4Key.lastIndexOf('/') + 1)
                const thumbKey   = `${folder}thumb.jpg`
                const teaserKey  = `${folder}teaser.mp4`
                const hlsFolder  = `${folder}hls/`

    const tmpDir     = fs.mkdtempSync(path.join(os.tmpdir(), `processor-${id}-`))
                const mp4Path    = path.join(tmpDir, 'input.mp4')
                const thumbPath  = path.join(tmpDir, 'thumb.jpg')
                const teaserPath = path.join(tmpDir, 'teaser.mp4')

    try {
                            // 1. Download original MP4
                    await updateProgress(id, 5)
                            console.log(`  v  Downloading ${mp4Key}...`)
                            await downloadFromR2(mp4Key, mp4Path)

                    // 2. Generate thumbnail (frame at 1 second)
                    await updateProgress(id, 10)
                            console.log(`  camera Generating thumbnail...`)
                            execSync([
                                                                    'ffmpeg', '-y', '-i', `"${mp4Path}"`,
                                                                    '-ss', '00:00:01', '-frames:v', '1', '-update', '1', '-q:v', '2',
                                                                    `"${thumbPath}"`,
                                                                ].join(' '), { stdio: 'inherit' })
                        // 3. Generate teaser (first 10s, 720px wide, compressed)
                    await updateProgress(id, 20)
                            console.log(`  movie Generating teaser (10s preview)...`)
                            execSync([
                                                                    'ffmpeg', '-y', '-i', `"${mp4Path}"`,
                                                                    '-t', '10',
                                                                    '-vf', 'scale=720:-2',
                                                                    '-c:v', 'libx264', '-c:a', 'aac',
                                                                    '-preset', 'fast', '-crf', '28',
                                                                    '-movflags', '+faststart',
                                                                    `"${teaserPath}"`,
                                                                ].join(' '), { stdio: 'inherit' })

                    // 4. Detect original resolution - never upscale
                    await updateProgress(id, 28)
                            const originalHeight = probeHeight(mp4Path)
                            console.log(`  zoom Original resolution: ${originalHeight}p`)

                    // Filter: only generate heights <= original
                        let targetHeights = requestedHeights.filter(h => h <= originalHeight)
                            if (targetHeights.length === 0) {
                                                                    // Original is smaller than all requested heights - use lowest requested
                                            targetHeights = [Math.min(...requestedHeights)]
                                                                    console.warn(`  ! All requested heights exceed original (${originalHeight}p). Using ${targetHeights[0]}p only.`)
                            }
                            console.log(`  list Generating HLS variants: ${targetHeights.map(h => `${h}p`).join(' + ')}`)

                    // 5. Generate HLS for each target height
                        await updateProgress(id, 35)
                            for (const height of targetHeights) {
                                                                    const cfg = QUALITY_CONFIG[height]
                                                                    const hlsDir = path.join(tmpDir, 'hls', `${height}p`)
                                                                    fs.mkdirSync(hlsDir, { recursive: true })
                                                                    console.log(`  signal Generating HLS ${height}p...`)
                                                                    execFileSync('ffmpeg', [
                                                                                                                            '-y', '-i', mp4Path,
                                                                                                                            '-profile:v', 'baseline', '-level', '3.0',
                                                                                                                            '-vf', `scale='min(${cfg.width},iw)':trunc(ow/a/2)*2`,
                                                                                                                            '-c:v', 'libx264', '-c:a', 'aac',
                                                                                                                            '-b:v', cfg.bitrate, '-maxrate', cfg.maxrate, '-bufsize', cfg.bufsize,
                                                                                                                            '-hls_time', '10',
                                                                                                                            '-hls_playlist_type', 'vod',
                                                                                                                            '-hls_segment_filename', path.join(hlsDir, 'seg%03d.ts'),
                                                                                                                            path.join(hlsDir, 'playlist.m3u8'),
                                                                                                            ], { stdio: 'inherit' })
                            }
                        // 6. Upload thumb + teaser
                    await updateProgress(id, 75)
                            if (fs.existsSync(thumbPath)) {
                                                                    await uploadToR2(thumbKey, thumbPath, 'image/jpeg')
                                                                    console.log(`  image  Thumbnail -> ${thumbKey}`)
                            }
                            if (fs.existsSync(teaserPath)) {
                                                                    await uploadToR2(teaserKey, teaserPath, 'video/mp4')
                                                                    console.log(`  video  Teaser -> ${teaserKey}`)
                            }

                    // 7. Upload all HLS files and multi-bitrate master playlist
                    await updateProgress(id, 85)
                            const variants = []

                                                    for (const height of targetHeights) {
                                                                                            const cfg = QUALITY_CONFIG[height]
                                                                                            const dir = path.join(tmpDir, 'hls', `${height}p`)
                                                                                            if (!fs.existsSync(dir)) continue
                                                                                            const files = fs.readdirSync(dir)
                                                                                            for (const file of files) {
                                                                                                                                                    const contentType = file.endsWith('.m3u8')
                                                                                                                                                        ? 'application/vnd.apple.mpegurl'
                                                                                                                                                                                                                : 'video/MP2T'
                                                                                                                                                    await uploadToR2(`${hlsFolder}${height}p/${file}`, path.join(dir, file), contentType)
                                                                                                    }
                                                                                            variants.push({ bandwidth: cfg.bandwidth, resolution: `${cfg.width}x${height}`, path: `${height}p/playlist.m3u8` })
                                                                                            const tsCount = files.filter(f => f.endsWith('.ts')).length
                                                                                            console.log(`  cube HLS ${height}p -> ${hlsFolder}${height}p/ (${tsCount} segments)`)
                                                    }

                    const masterContent = buildMasterPlaylist(variants)
                            const masterTmp = path.join(tmpDir, 'master.m3u8')
                            fs.writeFileSync(masterTmp, masterContent)
                            await uploadToR2(`${hlsFolder}master.m3u8`, masterTmp, 'application/vnd.apple.mpegurl')
                            console.log(`  list Master playlist -> ${hlsFolder}master.m3u8`)

                    // 8. Mark as processed in DB
                    await markDone({ id, originalVideoKey: mp4Key })

                    // 9. Delete original MP4 from R2 - HLS variants replace it
                    await deleteFromR2(mp4Key)
                            console.log(`  trash  Original MP4 deleted -> ${mp4Key}`)

                    console.log(`  check Done - "${name}"`)

    } finally {
                            fs.rmSync(tmpDir, { recursive: true, force: true })
    }
}

// --- Main ---------------------------------------------------------------------

async function main() {
                const pending = await fetchPending()
                const heights = DEFAULT_HEIGHTS.join('+')
                console.log(`movie  Player.Action - Adaptive HLS Generator (tier-aware)\n`)

    if (pending.length === 0) {
                            console.log('check No pending exercises.')
                            return
    }

    console.log(`list ${pending.length} exercise(s) to process:\n`)

    let ok = 0, fail = 0

    for (const ex of pending) {
                            const tierHeights = (Array.isArray(ex.videoQualityHeights) && ex.videoQualityHeights.length > 0)
                                            ? ex.videoQualityHeights.join('+')
                                                                    : heights
                            console.log(`\nrefresh "${ex.name}" (${ex.id}) [tier: ${tierHeights}p]`)
                            try {
                                                                    await processExercise(ex)
                                                                    ok++
                            } catch (err) {
                                                                    console.error(`  X ${err.message}`)
                                                                    fail++
                            }
    }

    console.log(`\n----------------------------`)
                console.log(`check ${ok} processed  X ${fail} failed`)
}

main().catch(err => { console.error('Fatal:', err); process.exit(1) })
