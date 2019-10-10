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

package main

//revive:disable:unhandled-error

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"gopkg.in/alecthomas/kingpin.v2"

	rdd "github.com/prometheusresearch/rex_deliver_dataset"
	val "github.com/prometheusresearch/rex_deliver_dataset/validation"
)

var (
	version = "DEVELOPMENT"
)

type Arguments struct {
	ConfigPath          string
	FilePath            string
	ValidationErrorPath string
	ValidateOnly        bool
}

func parseArguments() (Arguments, error) {
	app := kingpin.New(
		filepath.Base(os.Args[0]),
		fmt.Sprintf(
			"RexRegistry Dataset Delivery v%s\n\n"+
				"Delivers a set of files to a cloud storage container for"+
				" intake into a RexRegistry system.",
			version,
		),
	)

	configPath := app.Flag(
		"config",
		"Path to the configuration file to use. This is required.",
	).Short('c').OverrideDefaultFromEnvar("RDD_CONFIG").Required().String()

	validationErrors := app.Flag(
		"validation-errors",
		"If provided, validation errors will be written as a CSV to the file"+
			" name specified instead of outputting the errors to the console.",
	).Short('e').OverrideDefaultFromEnvar("RDD_VALIDATION_ERRORS").String()

	validateOnly := app.Flag(
		"validate-only",
		"Only execute dataset validation procedures; will not upload any"+
			" files.",
	).Short('v').Bool()

	filePath := app.Arg(
		"path",
		"Path to the directory containing the files to deliver.",
	).Required().String()

	app.Version(version)
	app.HelpFlag.Short('h')
	_, err := app.Parse(os.Args[1:])

	return Arguments{
		ConfigPath:          *configPath,
		FilePath:            *filePath,
		ValidationErrorPath: *validationErrors,
		ValidateOnly:        *validateOnly,
	}, err
}

func getConfig(args Arguments) (rdd.Configuration, error) {
	config, err := rdd.ReadConfig(args.ConfigPath)
	if err != nil {
		return config, err
	}

	config.SourcePath, err = rdd.AbsPath(args.FilePath)
	if err != nil {
		return config, err
	}

	return config, nil
}

func sayHello(config rdd.Configuration) {
	fmt.Printf("RexRegistry Dataset Delivery v%s\n", version)
	fmt.Printf(
		"  Execution Time: %s\n",
		rdd.TimeAsISO8601(config.ExecutionTime),
	)
	fmt.Printf("  Configuration: %s\n", config.ConfigurationPath)
	fmt.Printf("  Source Path: %s\n", config.SourcePath)
	fmt.Printf(
		"  Target: %s://%s\n",
		config.Storage["kind"],
		config.Storage["container"],
	)
	fmt.Printf("  Dataset Type: %s\n", config.DatasetType)
}

func getFiles(config rdd.Configuration) ([]rdd.File, error) {
	files, err := rdd.CatalogDirectory(config.SourcePath)
	if err != nil {
		return nil, err
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("No files found")
	}

	return files, err
}

func validateFiles(
	config rdd.Configuration,
	files []rdd.File,
) val.ErrorCollection {
	fmt.Printf("Validating Files...")
	validator := val.NewValidator(config.DatasetType)

	fileNames := make([]string, 0, len(files))
	for _, file := range files {
		fileNames = append(fileNames, file.Name)
	}

	errors := validator(config.SourcePath, fileNames)
	if errors.HasErrors() {
		fmt.Printf(" FAILED\n")
	} else {
		fmt.Printf(" SUCCESS\n")
	}
	return errors
}

func showValidationErrors(errors val.ErrorCollection) {
	files := errors.GetFiles()
	sort.Strings(files)

	for _, file := range files {
		fmt.Printf("  %s:\n", file)

		fileErrors := errors.Errors[file]
		for _, err := range fileErrors {
			fmt.Printf("    %s\n", err.String())
		}
	}
}

func writeValidationErrors(errors val.ErrorCollection, fileName string) error {
	filePath, err := rdd.AbsPath(fileName)
	if err != nil {
		return err
	}

	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	record := []string{
		"file",
		"record",
		"column",
		"error",
	}
	err = writer.Write(record)
	if err != nil {
		return err
	}

	files := errors.GetFiles()
	sort.Strings(files)
	fmt.Printf("  Failed Files: %s\n", strings.Join(files, ", "))

	for _, inputFile := range files {
		record[0] = inputFile
		fileErrors := errors.Errors[inputFile]
		for _, fileErr := range fileErrors {
			if fileErr.Record != 0 {
				record[1] = fmt.Sprintf("%d", fileErr.Record)
			} else {
				record[1] = ""
			}
			record[2] = fileErr.Column
			record[3] = fileErr.Message

			err := writer.Write(record)
			if err != nil {
				return err
			}
		}
	}

	fmt.Printf("  Full Error Report Saved to: %s\n", filePath)

	return nil
}

func uploadFiles(config rdd.Configuration, files []rdd.File) error {
	uploader, err := rdd.NewUploader(config)
	if err != nil {
		return err
	}

	fmt.Printf("Uploading Files...\n")
	var numFiles int
	var totalBytes int64
	for idx := range files {
		fmt.Printf(
			"  %s : %s",
			files[idx].Name,
			rdd.FormatBytes(float64(files[idx].Size)),
		)
		start := time.Now()

		err = uploader.UploadFile(&files[idx])
		if err != nil {
			fmt.Printf(" ..failure!\n")
			return err
		}

		elapsed := time.Now().Sub(start).Truncate(time.Microsecond)
		speed := float64(files[idx].Size) / elapsed.Seconds()
		fmt.Printf(" : %s : %s/s\n", elapsed, rdd.FormatBytes(speed))

		numFiles++
		totalBytes += files[idx].Size
	}

	fmt.Printf("Uploading Manifest...\n")
	manifest := rdd.CreateManifest(config, files)
	manifest.Generator = fmt.Sprintf("rex_deliver_dataset/%s", version)
	content, err := manifest.ToJSON()
	if err != nil {
		return err
	}
	err = uploader.UploadContent("MANIFEST.json", content)
	if err != nil {
		return err
	}

	fmt.Printf(
		"Complete! %d Files (%s) Uploaded to: %s\n",
		numFiles,
		rdd.FormatBytes(float64(totalBytes)),
		uploader.GetURL(),
	)
	return nil
}

func main() {
	args, err := parseArguments()
	kingpin.FatalIfError(err, "Invalid arguments")

	config, err := getConfig(args)
	kingpin.FatalIfError(err, "Could not read configuration")

	sayHello(config)

	files, err := getFiles(config)
	kingpin.FatalIfError(err, "Could not identify files to upload")

	errors := validateFiles(config, files)
	if errors.HasErrors() {
		if args.ValidationErrorPath != "" {
			err := writeValidationErrors(errors, args.ValidationErrorPath)
			kingpin.FatalIfError(
				err,
				"Could not write to validation error file",
			)
		} else {
			showValidationErrors(errors)
		}
		kingpin.Fatalf("Files did not satisfy validation rules")
	}

	if !args.ValidateOnly {
		err = uploadFiles(config, files)
		kingpin.FatalIfError(err, "Could not complete upload")
	}
}
