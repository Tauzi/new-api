package middleware

import (
	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/service"
	"github.com/gin-gonic/gin"
)

// BodyStorageCleanup releases request-body and downloaded-file resources when
// the handler chain completes, including aborted and recovered requests.
func BodyStorageCleanup() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			common.CleanupBodyStorage(c)
			service.CleanupFileSources(c)
		}()
		c.Next()
	}
}
