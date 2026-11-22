package interaction

import (
	"context"

	"fake_tiktok/internal/model"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ---------------------------------------------------------------------------
// VideoCommentRepo —— 视频评论
// ---------------------------------------------------------------------------

// VideoCommentRepo 视频评论表的 MySQL 数据存储
type VideoCommentRepo struct {
	db *gorm.DB
}

func NewVideoCommentRepo(db *gorm.DB) *VideoCommentRepo {
	return &VideoCommentRepo{db: db}
}

// Create 插入一条视频评论
func (r *VideoCommentRepo) Create(ctx context.Context, c *model.Comment) error {
	ctx, cancel := withTimeout(ctx)
	defer cancel()
	return r.db.WithContext(ctx).Create(c).Error
}

// FindByVideoID 分页查询指定视频的顶级评论（Level=1）
func (r *VideoCommentRepo) FindByVideoID(ctx context.Context, videoID uint, page, pageSize int) ([]model.Comment, int64, error) {
	ctx, cancel := withTimeout(ctx)
	defer cancel()

	var total int64
	if err := r.db.WithContext(ctx).Model(&model.Comment{}).
		Where("video_id = ? AND level = ?", videoID, 1).
		Count(&total).Error; err != nil {
		return nil, 0, err
	}

	var comments []model.Comment
	err := r.db.WithContext(ctx).
		Where("video_id = ? AND level = ?", videoID, 1).
		Order("like_count DESC, created_at DESC").
		Limit(pageSize).Offset((page - 1) * pageSize).
		Find(&comments).Error
	return comments, total, err
}

// FindReplies 分页查询指定父评论的回复
func (r *VideoCommentRepo) FindReplies(ctx context.Context, parentID uint64, page, pageSize int) ([]model.Comment, int64, error) {
	ctx, cancel := withTimeout(ctx)
	defer cancel()

	var total int64
	if err := r.db.WithContext(ctx).Model(&model.Comment{}).
		Where("parent_id = ?", parentID).
		Count(&total).Error; err != nil {
		return nil, 0, err
	}

	var comments []model.Comment
	err := r.db.WithContext(ctx).
		Where("parent_id = ?", parentID).
		Order("created_at ASC").
		Limit(pageSize).Offset((page - 1) * pageSize).
		Find(&comments).Error
	return comments, total, err
}

// FindRepliesByParentIDs 批量查询多个父评论的子回复（用于递归查楼中楼）
func (r *VideoCommentRepo) FindRepliesByParentIDs(ctx context.Context, parentIDs []uint64, limit int) ([]model.Comment, int64, error) {
	if len(parentIDs) == 0 {
		return nil, 0, nil
	}
	ctx, cancel := withTimeout(ctx)
	defer cancel()

	var total int64
	if err := r.db.WithContext(ctx).Model(&model.Comment{}).
		Where("parent_id IN ?", parentIDs).
		Count(&total).Error; err != nil {
		return nil, 0, err
	}

	var comments []model.Comment
	err := r.db.WithContext(ctx).
		Where("parent_id IN ?", parentIDs).
		Order("created_at ASC").
		Limit(limit).
		Find(&comments).Error
	return comments, total, err
}

// FindByID 根据 ID 查询单条评论
func (r *VideoCommentRepo) FindByID(ctx context.Context, id uint64) (*model.Comment, error) {
	ctx, cancel := withTimeout(ctx)
	defer cancel()

	var c model.Comment
	if err := r.db.WithContext(ctx).Where("id = ?", id).First(&c).Error; err != nil {
		return nil, err
	}
	return &c, nil
}

// DeleteCascade 硬删除一条评论及其全部后代回复（连同评论点赞记录），
// 返回实际删除的评论条数。
//
// 只删父评论会让子回复变成孤儿（父 ID 指向不存在的行，列表里再也查不到），
// 评论计数也会与实际可见评论数不符。BFS 逐层收集后代后在同一事务里删除，
// 深度上限 10 层防止异常环形数据把循环卡死。
func (r *VideoCommentRepo) DeleteCascade(ctx context.Context, id uint64) (int64, error) {
	ctx, cancel := withTimeout(ctx)
	defer cancel()

	ids := []uint64{id}
	frontier := []uint64{id}
	for depth := 0; depth < 10 && len(frontier) > 0; depth++ {
		var children []uint64
		if err := r.db.WithContext(ctx).Model(&model.Comment{}).
			Where("parent_id IN ?", frontier).
			Pluck("id", &children).Error; err != nil {
			return 0, err
		}
		if len(children) == 0 {
			break
		}
		ids = append(ids, children...)
		frontier = children
	}

	var deleted int64
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 先删点赞记录：评论没了但 like 行还在，会留下永远查不到的孤儿数据
		if err := tx.Where("comment_id IN ?", ids).Delete(&model.CommentLike{}).Error; err != nil {
			return err
		}
		res := tx.Where("id IN ?", ids).Delete(&model.Comment{})
		if res.Error != nil {
			return res.Error
		}
		deleted = res.RowsAffected
		return nil
	})
	if err != nil {
		return 0, err
	}
	return deleted, nil
}

// IncrementLikeCount 原子更新评论点赞数（delta 可正可负）
func (r *VideoCommentRepo) IncrementLikeCount(ctx context.Context, id uint64, delta int) error {
	ctx, cancel := withTimeout(ctx)
	defer cancel()
	return r.db.WithContext(ctx).Model(&model.Comment{}).
		Where("id = ?", id).
		UpdateColumn("like_count", gorm.Expr("like_count + ?", delta)).Error
}

// CountReplies 统计指定父评论的回复数
func (r *VideoCommentRepo) CountReplies(ctx context.Context, parentID uint64) (int64, error) {
	ctx, cancel := withTimeout(ctx)
	defer cancel()

	var count int64
	err := r.db.WithContext(ctx).Model(&model.Comment{}).
		Where("parent_id = ?", parentID).
		Count(&count).Error
	return count, err
}

// ---------------------------------------------------------------------------
// VideoCommentLikeRepo —— 视频评论点赞
// ---------------------------------------------------------------------------

type VideoCommentLikeRepo struct {
	db *gorm.DB
}

func NewVideoCommentLikeRepo(db *gorm.DB) *VideoCommentLikeRepo {
	return &VideoCommentLikeRepo{db: db}
}

// CreateLike 尝试插入点赞记录。created=true 表示首次新增（用于幂等判断）。
//
// 用 INSERT ... ON DUPLICATE KEY DO NOTHING 而不是 FirstOrCreate：
// 后者是"先查后插"，并发重复点赞时两个请求都查不到、都去插入，
// 其中一个撞唯一索引直接报错（用户看到"点赞失败"）。OnConflict 把竞态
// 交给数据库裁决，冲突时 RowsAffected=0，与"已点过"语义天然一致。
func (r *VideoCommentLikeRepo) CreateLike(ctx context.Context, userID string, commentID uint64) (bool, error) {
	ctx, cancel := withTimeout(ctx)
	defer cancel()

	like := model.CommentLike{
		UserID:    userID,
		CommentID: commentID,
	}
	result := r.db.WithContext(ctx).
		Clauses(clause.OnConflict{DoNothing: true}).
		Create(&like)
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

// DeleteLike 删除点赞记录。deleted=true 表示本次真正删掉了一行。
// 用 DELETE 的 RowsAffected 做原子判重：并发重复取消时只有一拟能拿到 true，
// 避免"先查后删"竞态导致调用方重复递减 like_count。
func (r *VideoCommentLikeRepo) DeleteLike(ctx context.Context, userID string, commentID uint64) (bool, error) {
	ctx, cancel := withTimeout(ctx)
	defer cancel()

	result := r.db.WithContext(ctx).
		Where("user_id = ? AND comment_id = ?", userID, commentID).
		Delete(&model.CommentLike{})
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

func (r *VideoCommentLikeRepo) ExistsLike(ctx context.Context, userID string, commentID uint64) (bool, error) {
	ctx, cancel := withTimeout(ctx)
	defer cancel()

	var count int64
	err := r.db.WithContext(ctx).Model(&model.CommentLike{}).
		Where("user_id = ? AND comment_id = ?", userID, commentID).
		Count(&count).Error
	if err != nil {
		return false, err
	}
	return count > 0, nil
}
