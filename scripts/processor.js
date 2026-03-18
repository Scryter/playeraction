/**
 * Player.Action — Thumbnail, Teaser & HLS Generator
 * Runs on GitHub Actions (ubuntu-latest) triggered by webhook after a video upload.
 *
 * Flow:
 *  1. Fetch exercises pending processing from the app API
 *  2. Download each MP4 from R2
 *  3. Generate thumb.jpg (frame at 1s) via FFmpeg
 *  4. Generate teaser.mp4 (first 10s, 720px wide) via FFmpeg
 *  5. Generate HLS playlist + segments via FFmpeg
 *  6. Upload thumb, teaser and all HLS files back to R2 (co-located with original MP4)
 *  7. PATCH the app API to mark as processed (hlsProcessed=true)
 */
'use strict'

const { execSync } = require('child_process')
const { S3Client, GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3')
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
    console.error('❌ Missing required env vars. Check repository secrets.')
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

// ─── API helpers ──────────────────────────────────────────────────────────────

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
        // Progress update failure is non-fatal — continue processing
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

// ─── R2 helpers ───────────────────────────────────────────────────────────────

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

// ─── Core processor ───────────────────────────────────────────────────────────

async function processExercise(exercise) {
    const { id, name, originalVideoKey } = exercise
    const mp4Key = originalVideoKey

    if (!mp4Key) {
        console.warn(`  ⚠ Skipping "${name}" — no originalVideoKey`)
        return
    }

    // Derive folder prefix from the MP4 key (strip filename)
    // e.g. "exercises/trainer/uploadId/video.mp4" → "exercises/trainer/uploadId/"
    const folder    = mp4Key.slice(0, mp4Key.lastIndexOf('/') + 1)
    const thumbKey  = `${folder}thumb.jpg`
    const teaserKey = `${folder}teaser.mp4`
    const hlsFolder = `${folder}hls/`

    const tmpDir    = fs.mkdtempSync(path.join(os.tmpdir(), `processor-${id}-`))
    const mp4Path   = path.join(tmpDir, 'input.mp4')
    const thumbPath = path.join(tmpDir, 'thumb.jpg')
    const teaserPth = path.join(tmpDir, 'teaser.mp4')
    const hlsDir    = path.join(tmpDir, 'hls')

    try {
        // 1. Download original MP4
        await updateProgress(id, 5)
        console.log(`  ⬇  Downloading ${mp4Key}…`)
        await downloadFromR2(mp4Key, mp4Path)

        // 2. Generate thumbnail (frame at 1 second)
        await updateProgress(id, 20)
        console.log(`  📸 Generating thumbnail…`)
        execSync([
            'ffmpeg', '-y', '-i', `"${mp4Path}"`,
            '-ss', '00:00:01', '-frames:v', '1', '-update', '1', '-q:v', '2',
            `"${thumbPath}"`,
        ].join(' '), { stdio: 'inherit' })

        // 3. Generate teaser (first 10s, 720px wide, compressed)
        await updateProgress(id, 40)
        console.log(`  🎬 Generating teaser (10s preview)…`)
        execSync([
            'ffmpeg', '-y', '-i', `"${mp4Path}"`,
            '-t', '10',
            '-vf', 'scale=720:-2',
            '-c:v', 'libx264', '-c:a', 'aac',
            '-preset', 'fast', '-crf', '23',
            '-movflags', '+faststart',
            `"${teaserPth}"`,
        ].join(' '), { stdio: 'inherit' })

        // 4. Generate HLS playlist + segments (adaptive streaming)
        await updateProgress(id, 60)
        console.log(`  📡 Generating HLS playlist + segments…`)
        fs.mkdirSync(hlsDir, { recursive: true })
        execSync([
            'ffmpeg', '-y', '-i', `"${mp4Path}"`,
            '-profile:v', 'baseline', '-level', '3.0',
            '-c:v', 'libx264', '-c:a', 'aac',
            '-hls_time', '10',
            '-hls_playlist_type', 'vod',
            '-hls_segment_filename', `"${path.join(hlsDir, 'segment%03d.ts')}"`,
            `"${path.join(hlsDir, 'master.m3u8')}"`,
        ].join(' '), { stdio: 'inherit' })

        // 5. Upload thumb + teaser to R2
        await updateProgress(id, 80)
        if (fs.existsSync(thumbPath)) {
            await uploadToR2(thumbKey, thumbPath, 'image/jpeg')
            console.log(`  🖼  Thumbnail → ${thumbKey}`)
        }
        if (fs.existsSync(teaserPth)) {
            await uploadToR2(teaserKey, teaserPth, 'video/mp4')
            console.log(`  🎞  Teaser → ${teaserKey}`)
        }

        // 6. Upload all HLS files (master.m3u8 + segments)
        if (fs.existsSync(hlsDir)) {
            const hlsFiles = fs.readdirSync(hlsDir)
            for (const file of hlsFiles) {
                const filePath = path.join(hlsDir, file)
                const r2Key = `${hlsFolder}${file}`
                const contentType = file.endsWith('.m3u8')
                    ? 'application/vnd.apple.mpegurl'
                    : 'video/MP2T'
                await uploadToR2(r2Key, filePath, contentType)
            }
            const m3u8Count = hlsFiles.filter(f => f.endsWith('.m3u8')).length
            const tsCount = hlsFiles.filter(f => f.endsWith('.ts')).length
            console.log(`  📦 HLS → ${hlsFolder} (${m3u8Count} playlist + ${tsCount} segments)`)
        }

        // 7. Mark as processed in DB
        await markDone({ id, originalVideoKey: mp4Key })
        console.log(`  ✅ Done — "${name}"`)

    } finally {
        fs.rmSync(tmpDir, { recursive: true, force: true })
    }
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
    console.log('🎬  Player.Action — Thumbnail, Teaser & HLS Generator\n')

    const pending = await fetchPending()

    if (pending.length === 0) {
        console.log('✅ No pending exercises.')
        return
    }

    console.log(`📋 ${pending.length} exercise(s) to process:\n`)

    let ok = 0, fail = 0

    for (const ex of pending) {
        console.log(`\n🔄 "${ex.name}" (${ex.id})`)
        try {
            await processExercise(ex)
            ok++
        } catch (err) {
            console.error(`  ❌ ${err.message}`)
            fail++
        }
    }

    console.log(`\n────────────────────────────`)
    console.log(`✅ ${ok} processed  ❌ ${fail} failed`)
}

main().catch(err => { console.error('Fatal:', err); process.exit(1) })
