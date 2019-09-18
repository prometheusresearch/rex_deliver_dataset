/*
	RexRegistry Dataset Delivery
    Copyright (C) 2019 Prometheus Research, LLC

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

package rexdeliverdataset

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func AbsPath(path string) (string, error) {
	if strings.HasPrefix(path, "~") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		path = home + path[1:]
	}

	return filepath.Abs(filepath.Clean(path))
}

const (
	b   = float64(1)
	kib = 1024 * b
	mib = 1024 * kib
	gib = 1024 * mib
	tib = 1024 * gib
)

func FormatBytes(size float64) string {
	var value float64
	suffix := ""

	switch {
	case size >= tib:
		suffix = "TiB"
		value = size / tib
	case size >= gib:
		suffix = "GiB"
		value = size / gib
	case size >= mib:
		suffix = "MiB"
		value = size / mib
	case size >= kib:
		suffix = "KiB"
		value = size / kib
	default:
		suffix = "B"
		value = size
	}

	return fmt.Sprintf(
		"%s%s",
		strings.TrimRight(
			strings.TrimRight(
				fmt.Sprintf("%.2f", value),
				"0",
			),
			".",
		),
		suffix,
	)
}

func TimeAsISO8601(t time.Time) string {
	return t.Format("2006-01-02T15:04:05Z0700")
}
