package middleware

import (
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"vilog-victorialogs/internal/util"
)

type tokenBucket struct {
	tokens float64
	last   time.Time
}

func RateLimit(rate float64, burst int, trustProxyHeaders bool) gin.HandlerFunc {
	if rate <= 0 || burst <= 0 {
		return func(c *gin.Context) {
			c.Next()
		}
	}

	var mu sync.Mutex
	buckets := make(map[string]*tokenBucket)

	return func(c *gin.Context) {
		clientIP := util.ExtractClientIP(c.Request, trustProxyHeaders)
		key := clientIP.String()
		if key == "<nil>" || key == "" {
			key = "unknown"
		}

		now := time.Now()

		mu.Lock()
		bucket, ok := buckets[key]
		if !ok {
			bucket = &tokenBucket{
				tokens: float64(burst),
				last:   now,
			}
			buckets[key] = bucket
		}

		elapsed := now.Sub(bucket.last).Seconds()
		bucket.tokens += elapsed * rate
		if bucket.tokens > float64(burst) {
			bucket.tokens = float64(burst)
		}
		bucket.last = now

		allowed := bucket.tokens >= 1
		if allowed {
			bucket.tokens -= 1
		}
		mu.Unlock()

		if !allowed {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error": gin.H{
					"code":    "rate_limited",
					"message": "rate limit exceeded",
				},
			})
			return
		}

		c.Next()
	}
}
