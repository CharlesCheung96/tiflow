// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package common

const (
	// DefaultFileMode is the default mode when operation files
	DefaultFileMode = 0o644
	// DefaultDirMode is the default mode when operation dir
	DefaultDirMode = 0o755

	// TmpEXT is the file ext of log file before safely wrote to disk
	TmpEXT = ".tmp"
	// LogEXT is the file ext of log file after safely wrote to disk
	LogEXT = ".log"
	// MetaEXT is the meta file ext of meta file after safely wrote to disk
	MetaEXT = ".meta"
	// MetaTmpEXT is the meta file ext of meta file before safely wrote to disk
	MetaTmpEXT = ".mtmp"
	// SortLogEXT is the sorted log file ext of log file after safely wrote to disk
	SortLogEXT = ".sort"

	// MinSectorSize is minimum sector size used when flushing log so that log can safely
	// distinguish between torn writes and ordinary data corruption.
	MinSectorSize = 512
)

const (
	// RedoMetaFileType is the default file type of meta file
	RedoMetaFileType = "meta"
	// RedoRowLogFileType is the default file type of row log file
	RedoRowLogFileType = "row"
	// RedoDDLLogFileType is the default file type of ddl log file
	RedoDDLLogFileType = "ddl"
)

// FileTypeConfig Specifies redo file type config.
type FileTypeConfig struct {
	// Whether emitting redo meta or not.
	EmitMeta bool
	// Whether emitting row events or not.
	EmitRowEvents bool
	// Whether emitting DDL events or not.
	EmitDDLEvents bool
}

// ConsistentLevelType is the level of redo log consistent level.
type ConsistentLevelType string

const (
	// ConsistentLevelNone no consistent guarantee.
	ConsistentLevelNone ConsistentLevelType = "none"
	// ConsistentLevelEventual eventual consistent.
	ConsistentLevelEventual ConsistentLevelType = "eventual"
)

// IsValidConsistentLevel checks whether a given consistent level is valid
func IsValidConsistentLevel(level string) bool {
	switch ConsistentLevelType(level) {
	case ConsistentLevelNone, ConsistentLevelEventual:
		return true
	default:
		return false
	}
}

// IsConsistentEnabled returns whether the consistent feature is enabled.
func IsConsistentEnabled(level string) bool {
	return IsValidConsistentLevel(level) && ConsistentLevelType(level) != ConsistentLevelNone
}

// ConsistentStorage is the type of consistent storage.
type ConsistentStorage string

const (
	// ConsistentStorageBlackhole is a blackhole storage, which will discard all data.
	ConsistentStorageBlackhole ConsistentStorage = "blackhole"
	// ConsistentStorageLocal is a local storage, which will store data in local disk.
	ConsistentStorageLocal ConsistentStorage = "local"
	// ConsistentStorageNFS is a NFS storage, which will store data in NFS.
	ConsistentStorageNFS ConsistentStorage = "nfs"

	// ConsistentStorageS3 is a S3 storage, which will store data in S3.
	ConsistentStorageS3 ConsistentStorage = "s3"
)

// IsValidConsistentStorage checks whether a give consistent storage is valid.
func IsValidConsistentStorage(scheme string) bool {
	switch ConsistentStorage(scheme) {
	case ConsistentStorageBlackhole,
		ConsistentStorageLocal, ConsistentStorageNFS,
		ConsistentStorageS3:
		return true
	default:
		return false
	}
}

// IsExternalStorage returns whether external storage, such as s3 and gcs, is used.
func IsExternalStorage(storage string) bool {
	switch ConsistentStorage(storage) {
	case ConsistentStorageS3:
		return true
	default:
		return false
	}
}
