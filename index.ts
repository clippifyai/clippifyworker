import express from 'express';
import Bull from 'bull';
import { createClient } from '@supabase/supabase-js';
import { AssemblyAI } from 'assemblyai';
import OpenAI from 'openai';
import YTDlpWrap from 'yt-dlp-wrap';

const app = express();
app.use(express.json());

// Environment variables
const REDIS_URL = process.env.REDIS_URL || 'redis://localhost:6379';
const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const ASSEMBLYAI_API_KEY = process.env.ASSEMBLYAI_API_KEY!;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY!;

// Initialize clients
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
const assemblyClient = new AssemblyAI({ apiKey: ASSEMBLYAI_API_KEY });
const openai = new OpenAI({ apiKey: OPENAI_API_KEY });
const ytDlp = new YTDlpWrap();

// Create queue
const videoQueue = new Bull('video-processing', REDIS_URL, {
  defaultJobOptions: {
    attempts: 3,
    backoff: { type: 'exponential', delay: 2000 },
    removeOnComplete: 100,
    removeOnFail: 100,
  },
});

// Process video jobs
videoQueue.process(async (job) => {
  const { videoId } = job.data;
  console.log(`[v0] Processing video ${videoId}`);

  try {
    // Get video from database
    const { data: video, error: videoError } = await supabase
      .from('videos')
      .select('*')
      .eq('id', videoId)
      .single();

    if (videoError || !video) {
      throw new Error(`Video not found: ${videoId}`);
    }

    // Update status to processing
    await supabase
      .from('videos')
      .update({ status: 'processing' })
      .eq('id', videoId);

    console.log(`[v0] Extracting audio URL from YouTube: ${video.source_url}`);

    // Get audio URL from YouTube
    const videoInfo = await ytDlp.getVideoInfo(video.source_url);
    const audioFormat = videoInfo.formats?.find(
      (f: any) => f.acodec && f.acodec !== 'none' && !f.vcodec
    );

    if (!audioFormat?.url) {
      throw new Error('Could not extract audio URL from YouTube video');
    }

    console.log(`[v0] Transcribing audio with AssemblyAI`);

    // Transcribe audio
    const transcript = await assemblyClient.transcripts.transcribe({
      audio_url: audioFormat.url,
    });

    if (transcript.status === 'error') {
      throw new Error(`Transcription failed: ${transcript.error}`);
    }

    console.log(`[v0] Analyzing transcript for viral moments`);

    // Analyze with GPT-4o
    const completion = await openai.chat.completions.create({
      model: 'gpt-4o',
      messages: [
        {
          role: 'system',
          content: `You are a viral video expert. Analyze transcripts and identify 30 viral moments.
          
For each moment, provide:
- Start time (seconds)
- End time (seconds)  
- Viral hook (what makes it engaging)
- Viral score (0-100)
- Platform scores (TikTok, Instagram, YouTube)
- Caption text
- Hashtags

Focus on: Strong hooks, emotional triggers, curiosity gaps, pattern interrupts, shocking statements.

Return as JSON array with this structure:
[{
  "start_time": 45,
  "end_time": 73,
  "hook": "Opens with shocking question",
  "viral_score": 87,
  "tiktok_score": 92,
  "instagram_score": 85,
  "youtube_score": 83,
  "caption": "Exact transcript text",
  "hashtags": ["#viral", "#trending"]
}]`,
        },
        {
          role: 'user',
          content: `Transcript:\n${transcript.text}\n\nFind 30 viral moments.`,
        },
      ],
      response_format: { type: 'json_object' },
    });

    const analysis = JSON.parse(completion.choices[0].message.content || '{}');
    const moments = analysis.clips || analysis.moments || [];

    console.log(`[v0] Creating ${moments.length} clips in database`);

    // Create clips in database
    const clips = moments.map((moment: any) => ({
      video_id: videoId,
      user_id: video.user_id,
      start_time: moment.start_time,
      end_time: moment.end_time,
      duration: moment.end_time - moment.start_time,
      viral_hook: moment.hook,
      viral_score: moment.viral_score,
      tiktok_score: moment.tiktok_score || moment.viral_score,
      instagram_score: moment.instagram_score || moment.viral_score,
      youtube_score: moment.youtube_score || moment.viral_score,
      caption_text: moment.caption,
      hashtags: moment.hashtags,
      status: 'ready',
    }));

    const { error: clipsError } = await supabase
      .from('video_clips')
      .insert(clips);

    if (clipsError) {
      throw new Error(`Failed to create clips: ${clipsError.message}`);
    }

    // Update video status
    await supabase
      .from('videos')
      .update({ status: 'completed', clips_generated: clips.length })
      .eq('id', videoId);

    console.log(`[v0] ✅ Successfully created ${clips.length} clips for video ${videoId}`);

    return { success: true, clipsCreated: clips.length };
  } catch (error: any) {
    console.error(`[v0] ❌ Error processing video ${videoId}:`, error);

    // Update video status to failed
    await supabase
      .from('videos')
      .update({ status: 'failed', error_message: error.message })
      .eq('id', videoId);

    throw error;
  }
});

// API endpoint to submit jobs
app.post('/api/process-video', async (req, res) => {
  try {
    const { videoId } = req.body;

    if (!videoId) {
      return res.status(400).json({ error: 'videoId is required' });
    }

    console.log(`[v0] Adding video ${videoId} to queue`);

    const job = await videoQueue.add({ videoId });

    res.json({
      success: true,
      jobId: job.id,
      message: 'Video queued for processing',
    });
  } catch (error: any) {
    console.error('[v0] Error queuing video:', error);
    res.status(500).json({ error: error.message });
  }
});

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// Queue dashboard
app.get('/admin/queues', async (req, res) => {
  const jobCounts = await videoQueue.getJobCounts();
  const waiting = await videoQueue.getWaiting();
  const active = await videoQueue.getActive();
  const completed = await videoQueue.getCompleted();
  const failed = await videoQueue.getFailed();

  res.json({
    counts: jobCounts,
    waiting: waiting.map((j) => ({ id: j.id, data: j.data })),
    active: active.map((j) => ({ id: j.id, data: j.data })),
    completed: completed.slice(0, 10).map((j) => ({ id: j.id, data: j.data })),
    failed: failed.slice(0, 10).map((j) => ({ id: j.id, data: j.data, failedReason: j.failedReason })),
  });
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
  console.log(`[v0] Worker server running on port ${PORT}`);
  console.log(`[v0] Health check: http://localhost:${PORT}/health`);
  console.log(`[v0] Queue dashboard: http://localhost:${PORT}/admin/queues`);
});
