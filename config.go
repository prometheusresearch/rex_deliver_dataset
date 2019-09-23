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
	"path"
	"runtime"
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
		"gs": {
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

func checkStorageKindProps(storage map[string]string) error {
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

	return nil
}

func checkStorageProps(storage map[string]string) error {
	for _, property := range commonStorageProperties {
		value := storage[property]
		if value == "" {
			return fmt.Errorf("storage requires %s property", property)
		}
	}

	if runtime.GOOS == "windows" && storage["kind"] == "local" {
		return fmt.Errorf(
			"storage.kind cannot be local on Windows systems",
		)
	}

	err := checkStorageKindProps(storage)
	if err != nil {
		return err
	}

	if storage["path"] != "" {
		if !path.IsAbs(storage["path"]) {
			return fmt.Errorf("storage.path must be an absolute path")
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

	cfgPath, err := AbsPath(configPath)
	if err != nil {
		return cfg, err
	}
	cfg.ConfigurationPath = cfgPath

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
