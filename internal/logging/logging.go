// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package logging configures the Logrus logging library.
package logging

import (
	"context"
	"strings"

	stackdriver "github.com/TV4/logrus-stackdriver-formatter"
	"github.com/sirupsen/logrus"
	"go.elastic.co/apm/module/apmlogrus"
	"open-match.dev/open-match/internal/config"
)

// ConfigureLogging sets up open match logrus instance using the logging section of the matchmaker_config.json
//  - log line format (text[default] or json)
//  - min log level to include (debug, info [default], warn, error, fatal, panic)
func ConfigureLogging(cfg config.View) {
	logrus.SetFormatter(newFormatter(cfg.GetString("logging.format"), cfg.GetString("logging.timestampFormat")))
	level := toLevel(cfg.GetString("logging.level"))
	logrus.SetLevel(level)
	if isDebugLevel(level) {
		logrus.Warn("Trace logging level configured. Not recommended for production!")
	}
}

func newFormatter(formatter string, timestampFormatter string) logrus.Formatter {
	switch strings.ToLower(formatter) {
	case "stackdriver":
		return stackdriver.NewFormatter()
	case "json":
		return &logrus.JSONFormatter{}
	}

	textFormatter := &logrus.TextFormatter{}
	if timestampFormatter != "" {
		textFormatter.TimestampFormat = timestampFormatter
		textFormatter.FullTimestamp = true
	}
	return textFormatter
}

func toLevel(level string) logrus.Level {
	switch strings.ToLower(level) {
	case "trace":
		return logrus.TraceLevel
	case "debug":
		return logrus.DebugLevel
	case "warn":
		fallthrough
	case "warning":
		return logrus.WarnLevel
	case "error":
		return logrus.ErrorLevel
	case "fatal":
		return logrus.FatalLevel
	case "panic":
		return logrus.PanicLevel
	}
	return logrus.InfoLevel
}

// IsDebugEnabled returns true if the logging level is debug or more granular.
func IsDebugEnabled(cfg config.View) bool {
	return IsDebugLevel(cfg.GetString("logging.level"))
}

// IsDebugLevel returns true if the logging level is debug or more granular.
func IsDebugLevel(level string) bool {
	return isDebugLevel(toLevel(level))
}

func isDebugLevel(level logrus.Level) bool {
	switch level {
	case logrus.TraceLevel:
		fallthrough
	case logrus.DebugLevel:
		return true
	}
	return false
}

// TraceContext returns zap.Fields containing the trace context
// of the transaction and span contained in ctx, if any.
func TraceContext(ctx context.Context) logrus.Fields {
	return apmlogrus.TraceContext(ctx)
}
