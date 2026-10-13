// Package milvusbackup contains the schema-bearing Go types for the
// "milvus-backup" BackupClass. Each struct here is converted to an OpenAPI
// v3 schema by `provider-sdk generate` and inlined into the generated
// BackupClass manifest.
//
// +k8s:openapi-gen=true
package milvusbackup

// MilvusBackupParameters describes the parameters accepted by Backup CRs that
// target this class (spec.parameters).
type MilvusBackupParameters struct {
	// Collections limits the backup to a comma-separated list of collections
	// in milvus-backup filter syntax: "coll", "db.coll" or "db.*". Empty backs
	// up every collection.
	// +optional
	Collections string `json:"collections,omitempty"`
	// IncludeRBAC also backs up users, roles and grants.
	// +optional
	IncludeRBAC bool `json:"includeRBAC,omitempty"`
}

// MilvusRestoreParameters describes the parameters accepted by Restore CRs
// that target this class (spec.parameters).
type MilvusRestoreParameters struct {
	// Collections limits the restore to a comma-separated list of collections
	// in milvus-backup filter syntax. Empty restores every collection.
	// +optional
	Collections string `json:"collections,omitempty"`
	// CollectionSuffix restores each collection next to the existing one under
	// its name plus this suffix, instead of replacing it.
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9_]*$`
	// +optional
	CollectionSuffix string `json:"collectionSuffix,omitempty"`
	// KeepExistingCollections fails the restore of a collection that already
	// exists instead of dropping and replacing it.
	// +optional
	KeepExistingCollections bool `json:"keepExistingCollections,omitempty"`
	// IncludeRBAC also restores users, roles and grants when the backup has
	// them.
	// +optional
	IncludeRBAC bool `json:"includeRBAC,omitempty"`
}
