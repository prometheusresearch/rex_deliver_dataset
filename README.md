# RexRegistry Dataset Delivery

[![Test Status](https://github.com/prometheusresearch/rex_deliver_dataset/workflows/Test/badge.svg)](https://github.com/prometheusresearch/rex_deliver_dataset/actions)
[![Go Report Card](https://goreportcard.com/badge/github.com/prometheusresearch/rex_deliver_dataset)](https://goreportcard.com/report/github.com/prometheusresearch/rex_deliver_dataset)


## Overview

`rex_deliver_dataset` is a simple command-line utility for uploading file-based
datasets into a [RexRegistry](https://www.prometheusresearch.com/) system. It
will validate the format of the files in your dataset and then upload them to
the system with all the required metadata. It automates all the steps necessary
to satisfy the [requirements for delivering files to
RexRegistry](doc/dataset_delivery_requirements.md).


## Installation

You can install `rex_deliver_dataset` onto your system in a variety of ways:

* Download from our [GitHub
  Releases](https://github.com/prometheusresearch/rex_deliver_dataset/releases).
  Every release will be available here with pre-built binaries for all the
  platforms we support. Unless you have a very specific reason not to, you
  should always download the most recent version.

* Homebrew. We provide a custom
  [tap](https://github.com/prometheusresearch/homebrew-public) that allows
  [Homebrew](https://brew.sh) users to easily install this utility on MacOS.

  ```
  $ brew tap prometheusresearch/public
  $ brew install rex_deliver_dataset
  ```

* Scoop. We provide a custom
  [bucket](https://github.com/prometheusresearch/scoop-public) that allows
  [Scoop](https://scoop.sh) users to easily install this utility on Windows.

  ```
  PS> scoop bucket add prometheusresearch https://github.com/prometheusresearch/scoop-public.git
  PS> scoop install rex_deliver_dataset
  ```

* Compile from
  [source](https://github.com/prometheusresearch/rex_deliver_dataset). If
  you're comfortable building [Go](https://golang.org) projects, you're welcome
  to retrieve the source and build it yourself.

  ```
  $ git clone https://github.com/prometheusresearch/rex_deliver_dataset
  $ cd rex_deliver_dataset
  $ make init
  $ make build
  ```


## Usage

Once installed, you can use this utility by executing the `rex_deliver_dataset`
command on the commandline of your system. It requires two parameters: one
parameter specifies the location of the [configuration file](#configuration) to
use, the other specifies the directory that contains the files you wish to
deliver. For example:

    $ rex_deliver_dataset --config=my_config_file.yaml /path/to/my/files

When executed, `rex_deliver_dataset` will validate the structure and format of
the files in your dataset according to the `dataset_type` specified in your
configuration, and if everything is valid, it will then upload those files to
the cloud storage container described in the `storage` section of your
configuration.

For more information about other parameters you can use, run
``rex_deliver_dataset --help``.


## Configuration

`rex_deliver_dataset` requires a configuration file in order to do its job.
This configuration file is a [YAML](https://yaml.org)-formatted file that
specifies a few properties. An example configuration is as follows:

```yaml
dataset_type: omop-5.2-csv

storage:
  kind: s3
  container: my-bucket-name
  access_key: SOME_ACCESS_KEY
  secret_key: YOUR_SUPER_SECRET_KEY
  region: us-east-1
```

The properties that this configuration file supports are:

### dataset_type

The `dataset_type` property tells the tool what kind of dataset you're trying
to upload. Using this information, it will perform a series of validations on
your files to ensure they are properly formatted. This property currently
allows the following values:

* `omop-5.2-csv` for CSV-formatted files representing OMOP CDM v5.2 tables
  ([specifications](doc/omop_52_csv.md))

### storage

The `storage` property tells the tool where to upload the dataset to. This
property consists of several child properties that contain the details of the
location:

#### kind

The `kind` property specifies type type of cloud storage container that is
being used. It is required, and currently allows the following values:

* `s3` for [Amazon S3](https://aws.amazon.com/s3)
* `gs` for [Google Cloud Storage](https://cloud.google.com/storage)

#### container

The `container` property specifies the name of the container that should be
used. It is required.

#### access_key

The `access_key` property specifies the Access Key that will be used to access
the S3 bucket that will be used. It is required when using a `kind` of `s3`.

#### secret_key

The `secret_key` property specifies the Secret Key that will be used to access
the S3 bucket that will be used. It is required when using a `kind` of `s3`.

#### region

The `region` property specifies the AWS region that the S3 bucket is hosted in.
It is required when using a `kind` of `s3`.

#### credentials_json

The `credentials_json` property specifies the path to the JSON file containing
the GCS Application Credentials that will be used to access the Google Cloud
Storage container. It is required when using a `kind` of `gs`.


## Support

If you require technical support in using this tool to submit datasets to a
RexRegistry system, please get in contact directly with your Registry
coordinator. Do not submit an issue in this GitHub Project.

If you believe you've found a bug with this utility, or would like to request
a new feature, feel free to submit a new issue in this [GitHub
Project](https://github.com/prometheusresearch/rex_deliver_dataset/issues).


## Contributing

You can contribute to this project by forking it, making your changes, and then
sending a Pull Request back to this project. To get started:

1. Clone the repository.
2. Run `make init`. This will retrieve all the dependencies of the project.
3. Make your changes (please include tests!).
4. Test your changes with `make test`.


## License

`rex_deliver_dataset` is published under the terms of the [GNU Affero General
Public License v3.0](https://www.gnu.org/licenses/agpl-3.0.en.html).

