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
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	val "github.com/prometheusresearch/rex_deliver_dataset/validation"

	// Load in the validators we want available
	_ "github.com/prometheusresearch/rex_deliver_dataset/validation/omop52csv"
)

var (
	commonStorageProperties = []string{
		"kind",
		"container",
	}

	implStorageProperties = map[string][]string{
		"s3": {
			"access_key",
			"secret_key",
			"region",
		},
		"gcs": {
			"credentials_json",
		},
		"local": {
			"path",
		},
	}
)

type Configuration struct {
	ConfigurationPath string
	SourcePath        string
	ExecutionTime     time.Time
	Storage           map[string]string
	DatasetType       string `yaml:"dataset_type"`
}

func NewConfiguration() Configuration {
	return Configuration{
		ExecutionTime: time.Now().UTC(),
		Storage:       make(map[string]string),
	}
}

func checkStorageProps(storage map[string]string) error {
	for _, property := range commonStorageProperties {
		value := storage[property]
		if value == "" {
			return fmt.Errorf("storage requires %s property", property)
		}
	}

	neededProps := implStorageProperties[storage["kind"]]
	if neededProps != nil {
		for _, property := range neededProps {
			value := storage[property]
			if value == "" {
				return fmt.Errorf(
					"storage requires %s property when kind=%s",
					property,
					storage["kind"],
				)
			}
		}
	} else {
		var kinds []string
		for k := range implStorageProperties {
			kinds = append(kinds, k)
		}
		sort.Strings(kinds)
		return fmt.Errorf(
			"storage.kind must be one of: %s",
			strings.Join(kinds, ", "),
		)
	}

	if storage["path"] != "" {
		sep := string(os.PathSeparator)
		if !strings.HasPrefix(storage["path"], sep) {
			return fmt.Errorf(
				"storage.path must be an absolute path (starting with a %s)",
				sep,
			)
		}
	}

	return nil
}

func (config Configuration) Validate() error {
	err := checkStorageProps(config.Storage)
	if err != nil {
		return err
	}

	allTypes := val.GetAvailableTypes()
	if config.DatasetType == "" {
		return fmt.Errorf(
			"dataset_type must be one of: %s",
			strings.Join(allTypes, ", "),
		)
	}
	found := false
	for _, dsType := range allTypes {
		if config.DatasetType == dsType {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf(
			"dataset_type must be one of: %s",
			strings.Join(allTypes, ", "),
		)
	}

	return nil
}

func ReadConfig(configPath string) (Configuration, error) {
	cfg := NewConfiguration()

	path, err := AbsPath(configPath)
	if err != nil {
		return cfg, err
	}
	cfg.ConfigurationPath = path

	file, err := os.Open(cfg.ConfigurationPath)
	if err != nil {
		return cfg, err
	}
	defer file.Close()

	content, err := ioutil.ReadAll(file)
	if err != nil {
		return cfg, err
	}

	err = yaml.Unmarshal(content, &cfg)
	if err != nil {
		return cfg, err
	}

	return cfg, cfg.Validate()
}