package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"vilog-victorialogs/internal/util"
)

func IPWhitelist(matcher *util.IPMatcher, trustProxyHeaders bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		if matcher == nil || matcher.Empty() {
			c.Next()
			return
		}

		clientIP := util.ExtractClientIP(c.Request, trustProxyHeaders)
		if !matcher.Contains(clientIP) {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{
				"error": gin.H{
					"code":    "forbidden",
					"message": "client IP is not allowed",
				},
			})
			return
		}

		c.Next()
	}
}
