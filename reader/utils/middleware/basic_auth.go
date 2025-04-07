package middleware

import (
	"encoding/base64"
	"net/http"
	"strings"
)

func BasicAuthMiddleware(login, pass string) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			auth := r.Header.Get("Authorization")
			if auth == "" {
				w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}

			authParts := strings.SplitN(auth, " ", 2)
			if len(authParts) != 2 || authParts[0] != "Basic" {
				http.Error(w, "Invalid authorization header", http.StatusBadRequest)
				return
			}

			payload, _ := base64.StdEncoding.DecodeString(authParts[1])
			pair := strings.SplitN(string(payload), ":", 2)

			if len(pair) != 2 || pair[0] != login ||
				pair[1] != pass {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
