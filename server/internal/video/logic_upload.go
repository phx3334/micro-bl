package video

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"mime/multipart"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"micro-bl/internal/dto/cache"
	"micro-bl/internal/dto/request"
	"micro-bl/internal/model"
	"micro-bl/pkg/apperr"
	"micro-bl/pkg/httpx"
	"micro-bl/pkg/otelx"
	"micro-bl/pkg/storagex"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// allowedVideoExt 视频上传扩展名白名单（小写形式）。
var allowedVideoExt = map[string]bool{
	".mp4": true,
	".mov": true,
	".avi": true,
	".mkv": true,
	".flv": true,
}

// allowedCoverExt 封面上传扩展名白名单（小写形式）。
var allowedCoverExt = map[string]bool{
	".jpg":  true,
	".jpeg": true,
	".png":  true,
	".gif":  true,
	".webp": true,
}

// VideoUploadLogic 视频投稿上传业务编排层
type VideoUploadLogic struct {
	deps    *LogicDeps
	storage storagex.Storage
	// uploadTotal 业务指标：上传成败三分类（success|client_error|server_error）
	uploadTotal metric.Int64Counter
}

// NewVideoUploadLogic 构造一个 VideoUploadLogic 实例。
func NewVideoUploadLogic(deps *LogicDeps, storage storagex.Storage) *VideoUploadLogic {
	// 构造发生在服务 Prepare 阶段，晚于 otelx.Init，拿到的是生效的 MeterProvider
	counter, _ := otelx.Meter("video-svc").Int64Counter("video_upload",
		metric.WithDescription("Video upload attempts by result (success|client_error|server_error)"))
	return &VideoUploadLogic{deps: deps, storage: storage, uploadTotal: counter}
}

// Upload 接收视频投稿：源文件写入 MinIO → 落库（status=transcoding）→ 发布转码任务。
//
// 流程：
//  1. 校验大小/扩展名
//  2. 生成与数据库无关的对象 key，先上传源文件与可选封面
//  3. 插入 status=transcoding 记录（携带源 key，供 worker 下载与失败重试）
//  4. 投递 video.transcode
//
// 上传或落库失败都会清理已上传对象，避免悬挂对象。
func (l *VideoUploadLogic) Upload(ctx context.Context, userID string, file *multipart.FileHeader, cover *multipart.FileHeader, meta request.VideoUploadReq) (id uint, retErr error) {
	// 手动埋点：上传是视频异步链路的起点，traceparent 会随转码消息投递给 worker，
	// 从而把"HTTP 上传 → Kafka → worker 转码"串成一条 trace。
	ctx, span := otelx.Tracer("video-svc").Start(ctx, "video.upload")
	defer span.End()
	span.SetAttributes(attribute.String("user_id", userID))
	if file != nil {
		span.SetAttributes(attribute.Int64("file_size", file.Size))
	}

	// 业务指标与 HTTP 状态码共用同一套分类（apperr）：
	//   业务错误 → result=client_error + HTTP 200；系统错误 → result=server_error + HTTP 500。
	// 未显式分类的错误在 defer 里按系统错误兜底，避免基础设施故障被记成用户行为。
	result := "success"
	defer func() {
		if retErr != nil {
			if apperr.IsSystem(retErr) {
				result = "server_error"
			} else if result == "success" {
				result = "client_error"
			}
		}
		l.uploadTotal.Add(context.Background(), 1,
			metric.WithAttributes(attribute.String("result", result)))
		span.SetAttributes(attribute.String("upload.result", result))
		// 只有服务端故障给 span 打 Error：客户端拒绝是正常业务行为，
		// 既不该被尾采 error 策略捞走，也不该拉低链路错误率
		if result == "server_error" {
			span.SetStatus(codes.Error, "upload failed: server side")
		}
	}()

	// 1. 校验视频文件大小与扩展名（用户行为 → 业务错误，HTTP 200）
	if file.Size > l.deps.Config.Upload.MaxFileSize {
		return 0, apperr.Business("文件超过大小限制")
	}
	ext := strings.ToLower(filepath.Ext(file.Filename))
	if !allowedVideoExt[ext] {
		return 0, apperr.Business("不支持的视频格式")
	}

	// 封面扩展名先校验，避免上传后再失败
	var coverExt string
	if cover != nil {
		coverExt = strings.ToLower(filepath.Ext(cover.Filename))
		if !allowedCoverExt[coverExt] {
			return 0, apperr.Business("不支持的封面格式")
		}
	}

	// 2. tags → JSON
	var tagsJSON string
	if len(meta.Tags) > 0 {
		body, err := json.Marshal(meta.Tags)
		if err != nil {
			return 0, apperr.Internal("序列化标签失败", err)
		}
		tagsJSON = string(body)
	}

	// 3. 先上传对象（key 由随机串生成，不依赖自增 ID）
	// 用 objID 而非 id：id 已是具名返回值（上传指标分类用），不可遮蔽
	objID := randomObjectID()
	sourceKey := fmt.Sprintf("video/src/%s%s", objID, ext)
	if err := l.putUpload(ctx, sourceKey, file); err != nil {
		return 0, apperr.Storage("保存视频文件失败", err)
	}

	var coverKey string
	if cover != nil {
		coverKey = fmt.Sprintf("cover/src/%s%s", objID, coverExt)
		if err := l.putUpload(ctx, coverKey, cover); err != nil {
			_ = l.storage.Delete(ctx, sourceKey)
			return 0, apperr.Storage("保存封面文件失败", err)
		}
	}

	// 4. 落库：源 key 持久化，便于 worker 下载与人工重发
	video := &model.Video{
		Status:         "transcoding",
		AuthorID:       userID,
		Title:          meta.Title,
		Description:    meta.Description,
		Zone:           meta.Zone,
		TagsJSON:       tagsJSON,
		SourceKey:      sourceKey,
		SourceCoverKey: coverKey,
		// PlayURL/CoverURL 留空，由 worker 转码完成后回写对象 key
	}
	if err := l.deps.VideoRepo.Create(ctx, video); err != nil {
		_ = l.storage.Delete(ctx, sourceKey)
		if coverKey != "" {
			_ = l.storage.Delete(ctx, coverKey)
		}
		return 0, apperr.DB("写入视频记录失败", err)
	}

	// 5. 发布转码消息
	// PublishBuffer 内部会入缓冲并自动重放，这里拿到的 err 只是"未能直发"的信号，
	// 不构成致命错误；记录中已保存源 key，可人工重发。
	msg := cache.TranscodeMsg{
		VideoID:        video.ID,
		SourceKey:      sourceKey,
		SourceCoverKey: coverKey,
		UserID:         userID,
	}
	if err := l.deps.TranscodePublisher.Publish(ctx, msg); err != nil {
		if l.deps.Logger != nil {
			// 关键路径日志带 trace_id，便于从 Tempo 直接跳到对应日志
			otelx.Logger(l.deps.Logger, ctx).Warn("transcode publish buffered or failed",
				zap.Uint("video_id", video.ID),
				zap.Error(err))
		}
	}

	return video.ID, nil
}

// GetTranscodeStatus 查询视频转码状态，供前端轮询。
func (l *VideoUploadLogic) GetTranscodeStatus(ctx context.Context, userID string, videoID uint) (httpx.VideoStatusResp, error) {
	video, err := l.deps.VideoRepo.FindByID(ctx, videoID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return httpx.VideoStatusResp{}, apperr.Business("视频不存在")
		}
		return httpx.VideoStatusResp{}, apperr.DB("查询视频失败", err)
	}

	if video.AuthorID != userID {
		return httpx.VideoStatusResp{}, apperr.Business("无权限查看")
	}

	// DB 存的是对象 key，响应时转换成可访问地址
	videoURL, _ := l.storage.URL(ctx, video.PlayURL)
	coverURL, _ := l.storage.URL(ctx, video.CoverURL)

	return httpx.VideoStatusResp{
		Status:     video.Status,
		FailReason: video.FailReason,
		VideoURL:   videoURL,
		CoverURL:   coverURL,
	}, nil
}

// putUpload 打开 multipart 文件并写入对象存储。
// 读盘与写 MinIO 都属于系统侧失败，交由调用方按 apperr.Storage 包装。
func (l *VideoUploadLogic) putUpload(ctx context.Context, key string, file *multipart.FileHeader) error {
	src, err := file.Open()
	if err != nil {
		return fmt.Errorf("open upload: %w", err)
	}
	defer src.Close()
	return l.storage.Put(ctx, key, src, file.Size)
}

// randomObjectID 生成 16 字节随机 hex（32 字符），作为对象 key 的唯一后缀。
func randomObjectID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand 失败属于系统级异常，退回纳秒时间戳保证 key 仍可用
		return strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	return hex.EncodeToString(b)
}
