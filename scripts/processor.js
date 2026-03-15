/**
 * Player.Action — Thumbnail & Teaser Generator
 * Runs on GitHub Actions (ubuntu-latest) triggered by webhook after a video upload.
 *
 * Flow:
 *  1. Fetch exercises pending thumbnail generation from the app API
 *  2. Download each MP4 from R2
 *  3. Generate thumb.jpg (frame at 1s) and teaser.mp4 (first 10s, 400px wide) via FFmpeg
 *  4. Upload thumb + teaser back to R2 (co-located with original MP4)
 *  5. PATCH the app API to mark as processed
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

async function markDone({ id, originalVideoKey }) {
    const res = await fetch(`${APP_URL}/api/admin/video-processor`, {
        method: 'PATCH',
        headers: {
            'Content-Type': 'application/json',
            'x-processor-key': PROCESSOR_API_KEY,
        },
        // Mark as processed — videoUrl stays as R2 MP4, thumb/teaser now available at known paths
        body: JSON.stringify({ id, hlsProcessed: true, originalVideoKey }),
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

    const tmpDir    = fs.mkdtempSync(path.join(os.tmpdir(), `processor-${id}-`))
    const mp4Path   = path.join(tmpDir, 'input.mp4')
    const thumbPath = path.join(tmpDir, 'thumb.jpg')
    const teaserPth = path.join(tmpDir, 'teaser.mp4')

    try {
        // 1. Download original MP4
        console.log(`  ⬇  Downloading ${mp4Key}…`)
        await downloadFromR2(mp4Key, mp4Path)

        // 2. Generate thumbnail (frame at 1 second)
        console.log(`  📸 Generating thumbnail…`)
        execSync([
            'ffmpeg', '-y', '-i', `"${mp4Path}"`,
            '-ss', '00:00:01', '-frames:v', '1', '-q:v', '2',
            `"${thumbPath}"`,
        ].join(' '), { stdio: 'inherit' })

        // 3. Generate teaser (first 10s, 400px wide, compressed)
        console.log(`  🎬 Generating teaser (10s preview)…`)
        execSync([
            'ffmpeg', '-y', '-i', `"${mp4Path}"`,
            '-t', '10',
            '-vf', 'scale=400:-2',
            '-c:v', 'libx264', '-c:a', 'aac',
            '-preset', 'fast', '-crf', '28',
            '-movflags', '+faststart',
            `"${teaserPth}"`,
        ].join(' '), { stdio: 'inherit' })

        // 4. Upload thumb + teaser to R2
        if (fs.existsSync(thumbPath)) {
            await uploadToR2(thumbKey, thumbPath, 'image/jpeg')
            console.log(`  🖼  Thumbnail → ${thumbKey}`)
        }
        if (fs.existsSync(teaserPth)) {
            await uploadToR2(teaserKey, teaserPth, 'video/mp4')
            console.log(`  🎞  Teaser → ${teaserKey}`)
        }

        // 5. Mark as processed in DB
        await markDone({ id, originalVideoKey: mp4Key })
        console.log(`  ✅ Done — "${name}"`)

    } finally {
        fs.rmSync(tmpDir, { recursive: true, force: true })
    }
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
    console.log('🖼  Player.Action — Thumbnail & Teaser Generator\n')

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
