# RexRegistry File Delivery Requirements

RexRegistry uses cloud storage platforms (AWS S3, Google Cloud Storage, etc) as
a file exchange mechanism with external systems. After you have coordinated
with Prometheus Research to establish the specifics of which cloud storage
platform will be used as part of your exchange with the RexRegistry system, you
can follow these requirements to deliver file-based datasets to it:

* No files should be placed at the root of the storage container. If any are,
  they will be ignored.
* Each dataset must be placed in its own subdirectory of the root of the
  storage container.
* The names of these subdirectories can be any name allowed by the cloud
  storage platform.
* All files that make up the dataset must be placed in the same directory.
* Each dataset must include a [Dataset Manifest file](manifest.md).
  * The Manifest must be the **last** file placed into the directory. When the
    Manifest is uploaded into a directory, it acts as a signal to RexRegistry
    that the entirety of the dataset has been uploaded and is ready for
    processing.
* Once a Manifest has been uploaded to the storage container for a dataset, all
  files in that dataset should be considered read only. Do not update any
  files, as those updates will be ignored by RexRegistry.
  * If you need to update records that were delivered in previous datasets,
    then you should upload a new dataset that uses the same record identifiers
    as those that were used in the previous datasets.

