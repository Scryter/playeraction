/**
 * Player.Action — HLS Video Processor
 * Runs on GitHub Actions (ubuntu-latest) which has FFmpeg available.
 *
 * Flow:
 *  1. Fetch pending exercises from the app API (MP4 in R2, not yet HLS)
 *  2. Download each MP4 from R2
 *  3. Convert to HLS + generate thumbnail with FFmpeg
 *  4. Upload all HLS segments, manifest and thumbnail back to R2
 *  5. PATCH the app API to mark exercise as HLS processed
 *  6. Clean up temp files
 */
'use strict'

const { execSync }  = require('child_process')
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

async function markDone({ id, hlsVideoUrl, hlsManifestKey, originalVideoKey }) {
    const res = await fetch(`${APP_URL}/api/admin/video-processor`, {
        method: 'PATCH',
        headers: {
            'Content-Type': 'application/json',
            'x-processor-key': PROCESSOR_API_KEY,
        },
        body: JSON.stringify({ id, hlsVideoUrl, hlsManifestKey, originalVideoKey }),
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
    const { id, name, videoPublicId, originalVideoKey } = exercise
    const mp4Key = originalVideoKey || videoPublicId

    if (!mp4Key) {
        console.warn(`  ⚠ Skipping "${name}" — no MP4 key`)
        return
    }

    const tmpDir  = fs.mkdtempSync(path.join(os.tmpdir(), `hls-${id}-`))
    const mp4Path = path.join(tmpDir, 'input.mp4')

    try {
        // 1. Download MP4
        console.log(`  ⬇  Downloading ${mp4Key}…`)
        await downloadFromR2(mp4Key, mp4Path)

        // 2. Convert MP4 → HLS
        console.log(`  🎬 Converting to HLS…`)
        execSync([
            'ffmpeg', '-y',
            '-i', mp4Path,
            '-c:v', 'libx264', '-c:a', 'aac',
            '-hls_time', '4', '-hls_list_size', '0',
            '-hls_segment_filename', path.join(tmpDir, 'seg_%03d.ts'),
            '-preset', 'fast', '-crf', '26',
            path.join(tmpDir, 'master.m3u8'),
        ].join(' '), { stdio: 'inherit' })

        // 3. Generate thumbnail at 1s
        console.log(`  📸 Generating thumbnail…`)
        const thumbPath = path.join(tmpDir, 'thumb.jpg')
        execSync([
            'ffmpeg', '-y', '-i', mp4Path,
            '-ss', '00:00:01', '-frames:v', '1', '-q:v', '2',
            thumbPath,
        ].join(' '), { stdio: 'inherit' })

        // 4. Upload to R2
        const folder    = mp4Key.includes('/') ? mp4Key.slice(0, mp4Key.lastIndexOf('/') + 1) : ''
        const hlsPrefix = `${folder}hls/${id}/`
        const manifest  = fs.readFileSync(path.join(tmpDir, 'master.m3u8'), 'utf-8')
        const segments  = [...manifest.matchAll(/seg_\d+\.ts/g)].map(m => m[0])

        console.log(`  ⬆  Uploading ${segments.length} segments + manifest…`)
        for (const seg of segments) {
            await uploadToR2(`${hlsPrefix}${seg}`, path.join(tmpDir, seg), 'video/MP2T')
        }
        const manifestKey = `${hlsPrefix}master.m3u8`
        await uploadToR2(manifestKey, path.join(tmpDir, 'master.m3u8'), 'application/x-mpegURL')

        if (fs.existsSync(thumbPath)) {
            await uploadToR2(`${hlsPrefix}thumb.jpg`, thumbPath, 'image/jpeg')
            console.log(`  🖼  Thumbnail uploaded`)
        }

        // 5. Mark done in DB
        await markDone({
            id,
            hlsVideoUrl:    `/api/videos/${manifestKey}`,
            hlsManifestKey: manifestKey,
            originalVideoKey: mp4Key,
        })

        console.log(`  ✅ Done — "${name}"`)
    } finally {
        fs.rmSync(tmpDir, { recursive: true, force: true })
    }
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function main() {
    console.log('🎬 Player.Action — HLS Processor\n')

    const pending = await fetchPending()

    if (pending.length === 0) {
        console.log('✅ No pending exercises.')
        return
    }

    console.log(`📋 ${pending.length} exercise(s) to convert:\n`)

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
    console.log(`✅ ${ok} converted  ❌ ${fail} failed`)
}

main().catch(err => { console.error('Fatal:', err); process.exit(1) })
