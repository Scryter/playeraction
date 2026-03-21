/**
 * Player.Action - Thumbnail, Teaser & Adaptive HLS Generator
 * Runs on GitHub Actions (ubuntu-latest) triggered by webhook after a video upload.
 *
 * Flow:
 *  1. Fetch exercises pending processing from the app API
 *  2. Download each MP4 from R2
 *  3. Generate thumb.jpg (frame at 1s) via FFmpeg
 *  4. Generate teaser.mp4 (first 10s, 720px wide) via FFmpeg
 *  5. Generate multi-bitrate HLS (1080p + 720p) with a master playlist for adaptive streaming
 *  6. Upload thumb, teaser and all HLS files back to R2
 *  7. Delete the original MP4 from R2 (no longer needed - HLS replaces it)
 *  8. PATCH the app API to mark as processed (hlsProcessed=true)
 *
 * Storage impact (per video):
 *  Before: original.mp4 (100%) + HLS 1080p (~80%) = ~180%
 *  After:  HLS 1080p (~80%) + HLS 720p (~35%) + assets = ~117% -> ~35% savings
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

// --- Core processor -----------------------------------------------------------

async function processExercise(exercise) {
        const { id, name, originalVideoKey } = exercise
        const mp4Key = originalVideoKey

    if (!mp4Key) {
                console.warn(`  ! Skipping "${name}" - no originalVideoKey`)
                return
    }

    const folder     = mp4Key.slice(0, mp4Key.lastIndexOf('/') + 1)
        const thumbKey   = `${folder}thumb.jpg`
        const teaserKey  = `${folder}teaser.mp4`
        const hlsFolder  = `${folder}hls/`

    const tmpDir     = fs.mkdtempSync(path.join(os.tmpdir(), `processor-${id}-`))
        const mp4Path    = path.join(tmpDir, 'input.mp4')
        const thumbPath  = path.join(tmpDir, 'thumb.jpg')
        const teaserPath = path.join(tmpDir, 'teaser.mp4')
        const hls1080Dir = path.join(tmpDir, 'hls', '1080p')
        const hls720Dir  = path.join(tmpDir, 'hls', '720p')

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

            // 4. Generate HLS 1080p - target 4 Mbps
            await updateProgress(id, 35)
                console.log(`  signal Generating HLS 1080p...`)
                fs.mkdirSync(hls1080Dir, { recursive: true })
                execFileSync('ffmpeg', [
                                '-y', '-i', mp4Path,
                                '-profile:v', 'baseline', '-level', '3.0',
                                '-vf', "scale='min(1920,iw)':trunc(ow/a/2)*2",
                                '-c:v', 'libx264', '-c:a', 'aac',
                                '-b:v', '4000k', '-maxrate', '4500k', '-bufsize', '9000k',
                                '-hls_time', '10',
                                '-hls_playlist_type', 'vod',
                                '-hls_segment_filename', path.join(hls1080Dir, 'seg%03d.ts'),
                                path.join(hls1080Dir, 'playlist.m3u8'),
                            ], { stdio: 'inherit' })

            // 5. Generate HLS 720p - target 1.5 Mbps (~35% of original size)
            await updateProgress(id, 60)
                console.log(`  signal Generating HLS 720p...`)
                fs.mkdirSync(hls720Dir, { recursive: true })
                execFileSync('ffmpeg', [
                                '-y', '-i', mp4Path,
                                '-profile:v', 'baseline', '-level', '3.0',
                                '-vf', "scale='min(1280,iw)':trunc(ow/a/2)*2",
                                '-c:v', 'libx264', '-c:a', 'aac',
                                '-b:v', '1500k', '-maxrate', '1800k', '-bufsize', '3600k',
                                '-hls_time', '10',
                                '-hls_playlist_type', 'vod',
                                '-hls_segment_filename', path.join(hls720Dir, 'seg%03d.ts'),
                                path.join(hls720Dir, 'playlist.m3u8'),
                            ], { stdio: 'inherit' })

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

                            for (const [label, dir, bandwidth, resolution] of [
                                            ['1080p', hls1080Dir, 4000000, '1920x1080'],
                                            ['720p',  hls720Dir,  1500000, '1280x720'],
                                        ]) {
                                            if (!fs.existsSync(dir)) continue
                                            const files = fs.readdirSync(dir)
                                            for (const file of files) {
                                                                const contentType = file.endsWith('.m3u8')
                                                                    ? 'application/vnd.apple.mpegurl'
                                                                                        : 'video/MP2T'
                                                                await uploadToR2(`${hlsFolder}${label}/${file}`, path.join(dir, file), contentType)
                                            }
                                            variants.push({ bandwidth, resolution, path: `${label}/playlist.m3u8` })
                                            const tsCount = files.filter(f => f.endsWith('.ts')).length
                                            console.log(`  cube HLS ${label} -> ${hlsFolder}${label}/ (${tsCount} segments)`)
                            }

            const masterContent = buildMasterPlaylist(variants)
                const masterTmp = path.join(tmpDir, 'master.m3u8')
                fs.writeFileSync(masterTmp, masterContent)
                await uploadToR2(`${hlsFolder}master.m3u8`, masterTmp, 'application/vnd.apple.mpegurl')
                console.log(`  list Master playlist -> ${hlsFolder}master.m3u8`)

            // 8. Mark as processed in DB
            await markDone({ id, originalVideoKey: mp4Key })

            // 9. Delete original MP4 from R2 - HLS variants replace it, saving ~35% storage
            await deleteFromR2(mp4Key)
                console.log(`  trash  Original MP4 deleted -> ${mp4Key}`)

            console.log(`  check Done - "${name}"`)

    } finally {
                fs.rmSync(tmpDir, { recursive: true, force: true })
    }
}

// --- Main ---------------------------------------------------------------------

async function main() {
        console.log('movie  Player.Action - Adaptive HLS Generator (1080p + 720p)\n')

    const pending = await fetchPending()

    if (pending.length === 0) {
                console.log('check No pending exercises.')
                return
    }

    console.log(`list ${pending.length} exercise(s) to process:\n`)

    let ok = 0, fail = 0

    for (const ex of pending) {
                console.log(`\nrefresh "${ex.name}" (${ex.id})`)
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
