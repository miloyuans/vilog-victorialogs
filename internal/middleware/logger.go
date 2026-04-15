package middleware

import (
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

func ZapLogger(logger *zap.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		rawQuery := c.Request.URL.RawQuery

		c.Next()

		fields := []zap.Field{
			zap.String("method", c.Request.Method),
			zap.String("path", path),
			zap.Int("status", c.Writer.Status()),
			zap.Duration("latency", time.Since(start)),
			zap.String("client_ip", c.ClientIP()),
			zap.Int("bytes_written", c.Writer.Size()),
		}

		if rawQuery != "" {
			fields = append(fields, zap.String("query", rawQuery))
		}

		if requestID := GetRequestID(c); requestID != "" {
			fields = append(fields, zap.String("request_id", requestID))
		}

		if len(c.Errors) > 0 {
			fields = append(fields, zap.String("errors", c.Errors.String()))
		}

		switch status := c.Writer.Status(); {
		case status >= 500:
			logger.Error("http request completed", fields...)
		case status >= 400:
			logger.Warn("http request completed", fields...)
		default:
			logger.Info("http request completed", fields...)
		}
	}
}
