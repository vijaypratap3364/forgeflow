package api

import (
	"net/http"
	"strings"
)

type responseObserver struct {
	http.ResponseWriter
	status int
}

func (writer *responseObserver) WriteHeader(status int) {
	if writer.status != 0 {
		return
	}
	writer.status = status
	writer.ResponseWriter.WriteHeader(status)
}

func (writer *responseObserver) Write(body []byte) (int, error) {
	if writer.status == 0 {
		writer.WriteHeader(http.StatusOK)
	}
	return writer.ResponseWriter.Write(body)
}

func (writer *responseObserver) statusCode() int {
	if writer.status == 0 {
		return http.StatusOK
	}
	return writer.status
}

type flushingResponseObserver struct {
	*responseObserver
	flusher http.Flusher
}

func (writer *flushingResponseObserver) Flush() {
	if writer.status == 0 {
		writer.WriteHeader(http.StatusOK)
	}
	writer.flusher.Flush()
}

func observeResponse(writer http.ResponseWriter) (*responseObserver, http.ResponseWriter) {
	observer := &responseObserver{ResponseWriter: writer}
	if flusher, ok := writer.(http.Flusher); ok {
		return observer, &flushingResponseObserver{responseObserver: observer, flusher: flusher}
	}
	return observer, observer
}

func normalizedRoute(route string) string {
	if route == "" {
		return "unmatched"
	}
	return route
}

func routeValue(pattern, path, name string) string {
	patternParts := strings.Split(strings.Trim(pattern, "/"), "/")
	pathParts := strings.Split(strings.Trim(path, "/"), "/")
	if len(patternParts) != len(pathParts) {
		return ""
	}
	wanted := "{" + name + "}"
	for index, part := range patternParts {
		if part == wanted {
			return pathParts[index]
		}
	}
	return ""
}
