package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"micro-bl/internal/dto/cache"
	"micro-bl/internal/model"
	"micro-bl/pkg/config"
	"micro-bl/pkg/kafkax"
	"micro-bl/pkg/otelx"
	"micro-bl/pkg/storagex"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// transcodeConsumeLoop 消费 video.transcode，把每条消息交给 transcodeOne。
//
// 与其余消费者一致：kafka-go Reader 自动处理重连，ctx 取消后退出并关闭 Reader。
//
// 为什么转码失败也提交 offset：
//   - ffmpeg 失败通常是输入文件损坏 / 编码不支持等不可恢复原因，重试只会反复消耗 CPU
//   - 失败信息已写入 videos.fail_reason，由用户决定是否重新上传
//
// JSON 解析失败同样由 kafkax.Consume 记录并提交，避免毒消息阻塞分区。
func transcodeConsumeLoop(ctx context.Context, kafkaCfg *config.KafkaConfig, db *gorm.DB, storageInstance storagex.Storage, ffmpegCfg config.FFmpegConfig, timeout time.Duration, logger *zap.Logger) {
	// 业务自定义指标（本项目唯一保留项）：转码成功率
	// PromQL: sum(rate(video_transcode_total{result="success"}[5m]))
	//       / sum(rate(video_transcode_total[5m]))
	// 此处创建晚于 otelx.Init，拿到的是生效的 MeterProvider（未启用时为 noop）
	transcodeTotal, _ := otelx.Meter("worker-svc").Int64Counter("video_transcode",
		metric.WithDescription("Transcode jobs total, grouped by result (success|failed)"))
	reader, err := kafkax.NewReader(kafkaCfg, kafkax.TopicVideoTranscode, "worker-transcode")
	if err != nil {
		logger.Error("kafka reader 初始化失败", zap.Error(err))
		return
	}
	defer reader.Close()

	kafkax.Consume(ctx, reader, logger, func(ctx context.Context, _, value []byte) error {
		var m cache.TranscodeMsg
		if err := json.Unmarshal(value, &m); err != nil {
			return err
		}

		// 手动埋点：ctx 已由 kafkax.Consume 从消息头续接上传链路的 trace，
		// 这里的 span 会成为 video.upload 的子 span，能看到"排队 → 转码"耗时。
		ctx, span := otelx.Tracer("worker-svc").Start(ctx, "worker.transcode",
			trace.WithAttributes(attribute.Int("video_id", int(m.VideoID))))
		defer span.End()

		// transcodeOne 内部已处理失败场景（写入 status=failed），
		// 返回 error 只代表不可恢复的内部错误；仍然提交 offset 避免重试循环
		ok, err := transcodeOne(ctx, m, db, storageInstance, ffmpegCfg, timeout, logger)
		// 转码成功率指标：与 GinMiddleware 同一取舍，独立 ctx 防进程关闭时统计丢失
		result := "failed"
		if ok {
			result = "success"
		}
		transcodeTotal.Add(context.Background(), 1, metric.WithAttributes(attribute.String("result", result)))
		// span 与指标共用同一套 result 语义：业务失败在 HTTP 层不可见（消费端无 5xx），
		// 必须打到 span 上，尾采的 string_attribute 策略（transcode.result=failed）才捞得到
		span.SetAttributes(attribute.String("transcode.result", result))
		if !ok {
			span.SetStatus(codes.Error, "transcode failed")
		}
		if err != nil {
			logger.Warn("transcode one returned error (still committing to avoid retry loop)",
				zap.Uint("video_id", m.VideoID),
				zap.Error(err))
		}
		return nil
	})
}

// transcodeOne 处理单条转码消息：下载源文件 → ffmpeg 转码 → 截帧生成封面 → 上传 → 更新 DB。
//
// 流程：
//  1. 标记 status=transcoding，让前端轮询能看到"正在转码"
//  2. 从对象存储下载 SourceKey 到本机临时目录（worker 唯一允许使用本地磁盘的场景）
//  3. 调用 ffmpeg -i {source} -c:v libx264 -preset medium -crf 23 -c:a aac -y {output}.mp4
//     使用 CommandContext 绑定超时，避免 ffmpeg 卡死拖垮 worker
//  4. 失败：截取 stderr 末尾 2000 字符作为 fail_reason，写入 status=failed，清理源对象并返回 nil（Ack）
//  5. 成功：用 ffprobe 解析时长
//  6. 若 SourceCoverKey 为空：调用 ffmpeg -ss {duration/2} -i {source} -frames:v 1 截帧生成 JPG
//  7. 通过 storagex.Put 上传 video 和 cover 到 video/{videoID}.mp4 / cover/{videoID}.jpg
//  8. UPDATE videos SET video_url, cover_url, duration_sec, status='published' WHERE id=?
//
// 返回值约定：
//   - (true, nil)：转码成功，status=published 已写库
//   - (false, nil)：业务失败（status=failed 已写库、不可重试），流程已收尾
//   - (false, error)：不可恢复的内部错误（如发布后 DB 更新失败）；调用方仍会 Ack
//
// ok 供调用方打转码成功率指标 video_transcode_total。
func transcodeOne(ctx context.Context, msg cache.TranscodeMsg, db *gorm.DB, storageInstance storagex.Storage, ffmpegCfg config.FFmpegConfig, timeout time.Duration, logger *zap.Logger) (bool, error) {
	// 关键路径日志统一带 trace_id/span_id：转码失败后可在 Tempo 按 video_id 找到链路，
	// 再通过 trace_id 检索这段 ffmpeg / 上传 / DB 的日志。
	logger = otelx.Logger(logger, ctx)

	// 0. 幂等保护：Kafka 是 at-least-once，"回写 published → 崩溃 → 未提交 offset"
	//    或生产端缓冲重放都会让同一消息再次投递。此时源对象已在上次成功时清理，
	//    若不加判断继续跑，下载失败会把已发布的视频改写成 failed。
	var current model.Video
	if err := db.WithContext(ctx).Select("status").
		Where("id = ?", msg.VideoID).First(&current).Error; err == nil &&
		current.Status == "published" {
		logger.Info("transcode: duplicate delivery, video already published, skip",
			zap.Uint("video_id", msg.VideoID))
		return true, nil
	}

	// 1. 标记 status=transcoding，让前端轮询能看到"正在转码"
	//    使用裸 db.Model 而不是 VideoRepo.UpdateTranscodeResult，因为 worker 进程
	//    没有 Repos 容器（Repos 是 API 进程的依赖聚合）
	if err := db.WithContext(ctx).Model(&model.Video{}).
		Where("id = ?", msg.VideoID).
		Update("status", "transcoding").Error; err != nil {
		logger.Warn("transcode: failed to mark transcoding",
			zap.Uint("video_id", msg.VideoID),
			zap.Error(err))
		// 继续转码：标记失败不应阻止后续步骤
	}

	// 2. 本机临时目录：下载源文件 + 存放 ffmpeg 产物，函数结束时整体清理
	tmpDir, err := os.MkdirTemp("", "micro-bl-transcode-*")
	if err != nil {
		failReason := truncateErr(fmt.Sprintf("create temp dir failed: %v", err))
		_ = db.WithContext(ctx).Model(&model.Video{}).
			Where("id = ?", msg.VideoID).
			Updates(map[string]interface{}{"status": "failed", "fail_reason": failReason})
		cleanupObjects(ctx, storageInstance, logger, msg.VideoID, msg.SourceKey, msg.SourceCoverKey)
		return false, nil
	}
	defer os.RemoveAll(tmpDir)

	// 源视频 key 为空说明消息体异常（旧版本消息无 SourceKey），直接失败
	if msg.SourceKey == "" {
		failReason := truncateErr("transcode: empty source key")
		_ = db.WithContext(ctx).Model(&model.Video{}).
			Where("id = ?", msg.VideoID).
			Updates(map[string]interface{}{"status": "failed", "fail_reason": failReason})
		return false, nil
	}

	// 3. 下载源视频到临时文件
	inputPath := filepath.Join(tmpDir, "source"+filepath.Ext(msg.SourceKey))
	if err := downloadToFile(ctx, storageInstance, msg.SourceKey, inputPath); err != nil {
		failReason := truncateErr(fmt.Sprintf("download source failed: %v", err))
		logger.Warn("transcode: download source failed",
			zap.Uint("video_id", msg.VideoID),
			zap.String("source_key", msg.SourceKey),
			zap.Error(err))
		_ = db.WithContext(ctx).Model(&model.Video{}).
			Where("id = ?", msg.VideoID).
			Updates(map[string]interface{}{"status": "failed", "fail_reason": failReason})
		cleanupObjects(ctx, storageInstance, logger, msg.VideoID, msg.SourceKey)
		return false, nil
	}

	// 4. 准备输出文件
	outputPath := filepath.Join(tmpDir, fmt.Sprintf("%d.mp4", msg.VideoID))

	// 5. 调用 ffmpeg 转码
	//    超时由 APP_TRANSCODE_TIMEOUT（秒）控制，默认 30 分钟，足够覆盖 1GB 视频
	//    （5Mbps ≈ 30 分钟）在主流 CPU 上的重编码耗时；ffmpeg 用 -y 覆盖输出，避免交互提示阻塞。
	if timeout <= 0 {
		timeout = 30 * time.Minute
	}
	transcodeCtx, transcodeCancel := context.WithTimeout(ctx, timeout)
	defer transcodeCancel()

	ffmpegBin := ffmpegCfg.BinaryPath
	if ffmpegBin == "" {
		ffmpegBin = "ffmpeg"
	}
	outputCodec := ffmpegCfg.OutputCodec
	if outputCodec == "" {
		outputCodec = "libx264"
	}
	preset := ffmpegCfg.Preset
	if preset == "" {
		preset = "medium"
	}
	crf := ffmpegCfg.CRF
	if crf == 0 {
		crf = 23
	}

	// ffmpeg -i {input} -c:v libx264 -preset medium -crf 23 -maxrate 2M
	//   -bufsize 4M -c:a aac -movflags +faststart -y {output}
	// 关键优化（修复 Web 播放卡顿）：
	//   - -movflags +faststart：把 moov 索引原子前置到文件头，
	//     支持浏览器边下边播 / 任意拖拽，否则 moov 在文件末尾会导致整段下载完才播放。
	//   - -maxrate/-bufsize：限制峰值码率，避免对已经压过的源重编码后
	//     文件比源还大（如 28.9M 源转出 45M），导致流式播放带宽不足而缓冲卡顿。
	cmd := exec.CommandContext(transcodeCtx, ffmpegBin,
		"-i", inputPath,
		"-c:v", outputCodec,
		"-preset", preset,
		"-crf", strconv.Itoa(crf),
		"-maxrate", "2M",
		"-bufsize", "4M",
		"-c:a", "aac",
		"-movflags", "+faststart",
		"-y", outputPath,
	)
	// 捕获 stderr 用于失败时输出 fail_reason
	// ffmpeg 进度信息全部走 stderr，stdout 留空
	var stderrBuf strings.Builder
	cmd.Stderr = &stderrBuf
	// stdin 显式设为 nil 避免从控制台读取
	cmd.Stdin = nil

	if err := cmd.Run(); err != nil {
		// 转码失败：截取 stderr 末尾 2000 字符作为 fail_reason
		failReason := truncateErr(fmt.Sprintf("ffmpeg exit: %v, stderr: %s",
			err, stderrBuf.String()))
		logger.Warn("transcode: ffmpeg failed",
			zap.Uint("video_id", msg.VideoID),
			zap.String("fail_reason", failReason))
		_ = db.WithContext(ctx).Model(&model.Video{}).
			Where("id = ?", msg.VideoID).
			Updates(map[string]interface{}{"status": "failed", "fail_reason": failReason})
		cleanupObjects(ctx, storageInstance, logger, msg.VideoID, msg.SourceKey, msg.SourceCoverKey)
		return false, nil
	}

	// 6. 解析视频时长：使用 ffprobe（默认在 PATH 中）
	//    ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 {output}
	durationCtx, durationCancel := context.WithTimeout(ctx, 30*time.Second)
	defer durationCancel()
	probeCmd := exec.CommandContext(durationCtx, "ffprobe",
		"-v", "error",
		"-show_entries", "format=duration",
		"-of", "default=noprint_wrappers=1:nokey=1",
		outputPath,
	)
	probeOut, err := probeCmd.Output()
	if err != nil {
		// ffprobe 失败：不算致命，duration 留 0
		logger.Warn("transcode: ffprobe failed, duration will be 0",
			zap.Uint("video_id", msg.VideoID),
			zap.Error(err))
	}
	duration := 0.0
	if err == nil {
		// 输出格式：浮点数字符串 + 换行，如 "12.345000\n"
		durStr := strings.TrimSpace(string(probeOut))
		if durStr != "" {
			if d, perr := strconv.ParseFloat(durStr, 64); perr == nil {
				duration = d
			}
		}
	}

	// 7. 准备封面文件
	//    若 SourceCoverKey 已指定（用户上传时自带），先下载到本地复用
	//    否则调用 ffmpeg -ss {duration/2} -i {source} -frames:v 1 截帧生成 JPG
	var coverLocalPath string
	if msg.SourceCoverKey != "" {
		coverLocalPath = filepath.Join(tmpDir, "cover"+filepath.Ext(msg.SourceCoverKey))
		if err := downloadToFile(ctx, storageInstance, msg.SourceCoverKey, coverLocalPath); err != nil {
			logger.Warn("transcode: download source cover failed, will extract frame",
				zap.Uint("video_id", msg.VideoID),
				zap.String("cover_key", msg.SourceCoverKey),
				zap.Error(err))
			coverLocalPath = ""
		}
	}
	if coverLocalPath == "" {
		coverLocalPath = filepath.Join(tmpDir, fmt.Sprintf("%d_cover.jpg", msg.VideoID))
		// 取视频中间时刻截帧，避免开头黑屏
		ssTime := duration / 2
		coverCtx, coverCancel := context.WithTimeout(ctx, 30*time.Second)
		coverCmd := exec.CommandContext(coverCtx, ffmpegBin,
			"-ss", strconv.FormatFloat(ssTime, 'f', 3, 64),
			"-i", inputPath,
			"-frames:v", "1",
			"-y", coverLocalPath,
		)
		var coverStderr strings.Builder
		coverCmd.Stderr = &coverStderr
		if err := coverCmd.Run(); err != nil {
			// 截帧失败：不致命，cover_url 留空，视频仍可发布
			logger.Warn("transcode: cover extract failed, cover_url will be empty",
				zap.Uint("video_id", msg.VideoID),
				zap.String("stderr", coverStderr.String()),
				zap.Error(err))
			coverLocalPath = ""
		}
		coverCancel()
	}

	// 8. 上传视频文件到对象存储（DB 只保存 key）
	videoFile, err := os.Open(outputPath)
	if err != nil {
		// 致命：转码好的文件打不开，标记失败
		failReason := truncateErr(fmt.Sprintf("open transcoded video failed: %v", err))
		_ = db.WithContext(ctx).Model(&model.Video{}).
			Where("id = ?", msg.VideoID).
			Updates(map[string]interface{}{"status": "failed", "fail_reason": failReason})
		cleanupObjects(ctx, storageInstance, logger, msg.VideoID, msg.SourceKey, msg.SourceCoverKey)
		return false, nil
	}
	videoKey := fmt.Sprintf("video/%d.mp4", msg.VideoID)
	var videoSize int64
	if fi, statErr := videoFile.Stat(); statErr == nil {
		videoSize = fi.Size()
	}
	err = storageInstance.Put(ctx, videoKey, videoFile, videoSize)
	_ = videoFile.Close()
	if err != nil {
		failReason := truncateErr(fmt.Sprintf("upload video to storage failed: %v", err))
		_ = db.WithContext(ctx).Model(&model.Video{}).
			Where("id = ?", msg.VideoID).
			Updates(map[string]interface{}{"status": "failed", "fail_reason": failReason})
		cleanupObjects(ctx, storageInstance, logger, msg.VideoID, msg.SourceKey, msg.SourceCoverKey)
		return false, nil
	}

	// 9. 上传封面文件到对象存储（若有）
	var coverKey string
	if coverLocalPath != "" {
		coverFile, err := os.Open(coverLocalPath)
		if err == nil {
			coverKey = fmt.Sprintf("cover/%d.jpg", msg.VideoID)
			var coverSize int64
			if fi, statErr := coverFile.Stat(); statErr == nil {
				coverSize = fi.Size()
			}
			err = storageInstance.Put(ctx, coverKey, coverFile, coverSize)
			_ = coverFile.Close()
		}
		if err != nil {
			// 封面上传失败：不致命，cover_url 留空，视频仍可发布
			logger.Warn("transcode: cover upload failed, cover_url will be empty",
				zap.Uint("video_id", msg.VideoID),
				zap.Error(err))
			_ = storageInstance.Delete(ctx, coverKey)
			coverKey = ""
		}
	}

	// 10. 更新 DB：写入 video_url / cover_url（对象 key）/ duration_sec / status=published，
	//     并按 model.Video 约定清空 source_key/source_cover_key（源对象随后即被删除）
	//     使用 Updates(map[...]) 确保零值字段也被写入
	if err := db.WithContext(ctx).Model(&model.Video{}).
		Where("id = ?", msg.VideoID).
		Updates(map[string]interface{}{
			"play_url":         videoKey,
			"cover_url":        coverKey,
			"duration_sec":     duration,
			"status":           "published",
			"source_key":       "",
			"source_cover_key": "",
		}).Error; err != nil {
		logger.Error("transcode: failed to update video status to published",
			zap.Uint("video_id", msg.VideoID),
			zap.Error(err))
		// 清理已上传的产物，保持 DB 与存储一致
		cleanupObjects(ctx, storageInstance, logger, msg.VideoID, videoKey, coverKey)
		return false, err
	}

	// 11. 转码成功后清理源对象（video/src/{id}.* 与 cover/src/{id}.*）
	//     清理失败仅记日志，不影响业务流程；本机临时文件由 defer 清理。
	cleanupObjects(ctx, storageInstance, logger, msg.VideoID, msg.SourceKey, msg.SourceCoverKey)

	logger.Info("transcode: success",
		zap.Uint("video_id", msg.VideoID),
		zap.Float64("duration", duration),
		zap.String("video_key", videoKey),
		zap.String("cover_key", coverKey))
	return true, nil
}

// downloadToFile 从对象存储下载 key 到本地 dest 路径。
func downloadToFile(ctx context.Context, storageInstance storagex.Storage, key, dest string) error {
	f, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer f.Close()
	return storagex.Download(ctx, storageInstance, key, f)
}

// cleanupObjects 删除对象 key（空 key 跳过），删除失败只记日志。
func cleanupObjects(ctx context.Context, storageInstance storagex.Storage, logger *zap.Logger, videoID uint, keys ...string) {
	for _, key := range keys {
		if key == "" {
			continue
		}
		if err := storageInstance.Delete(ctx, key); err != nil {
			logger.Warn("transcode: cleanup object failed",
				zap.Uint("video_id", videoID),
				zap.String("key", key),
				zap.Error(err))
		}
	}
}

// truncateErr 把 fail_reason 截断到 2000 字符以内，匹配 model.Video.FailReason 的 gorm size:2000。
// 截取末尾部分（最近发生的错误更相关）。
func truncateErr(s string) string {
	const max = 2000
	if len(s) <= max {
		return s
	}
	// 保留末尾 max 字符：最近的 ffmpeg 错误信息通常最有用
	return "..." + s[len(s)-max+3:]
}
