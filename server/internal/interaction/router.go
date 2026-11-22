package interaction

import "github.com/gin-gonic/gin"

// RegisterRoutes 注册 interaction-svc 的 HTTP 路由。
//
// 统一前缀约定：所有路由都挂在 /api/v1/interaction 下。
// 关注/取关已迁到 user-svc（/api/v1/user/follow），本服务不再注册。
func RegisterRoutes(publicGroup, privateGroup *gin.RouterGroup, h *HandlerGroup) {
	// 私有路由：需要登录才能访问的操作
	interactionRouter := privateGroup.Group("interaction")
	interactionVideoRouter := interactionRouter.Group("video")
	{
		interactionVideoRouter.POST("like", h.InteractionHandler.LikeVideo)
		interactionVideoRouter.POST("unlike", h.InteractionHandler.UnlikeVideo)
	}

	// 评论列表是公开接口：视频详情页是公开路由，未登录用户也要能看评论，
	// 否则匿名访问会被 401 踢去登录页。写操作（创建/点赞/删除）仍需登录。
	commentPublic := publicGroup.Group("interaction/comment")
	{
		commentPublic.GET("list", h.CommentHandler.ListComments)
		commentPublic.GET("replies", h.CommentHandler.ListReplies)
	}
	commentPrivate := interactionRouter.Group("comment")
	{
		commentPrivate.POST("create", h.CommentHandler.CreateComment)
		commentPrivate.POST("like", h.CommentHandler.LikeComment)
		commentPrivate.POST("unlike", h.CommentHandler.UnlikeComment)
		commentPrivate.POST("delete", h.CommentHandler.DeleteComment)
	}
}
