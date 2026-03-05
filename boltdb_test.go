package heartwood

import (
	"github.com/boltdb/bolt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
)

func tempDB(t *testing.T) *bolt.DB {
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := bolt.Open(path, 0600, nil)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func TestSetupBoltDB(t *testing.T) {
	db := tempDB(t)

	err := SetupBoltDB(db)
	assert.NoError(t, err)

	// Verify bucket exists
	err = db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(BucketName))
		assert.NotNil(t, bucket)
		return nil
	})
	assert.NoError(t, err)

	// Second call should not error
	err = SetupBoltDB(db)
	assert.NoError(t, err)
}

func TestBatchCheckEventsExist(t *testing.T) {
	cases := []struct {
		name        string
		setupEvents []EventBlob
		checkIDs    []string
		expectedMap map[string]bool
		skipSetup   bool
	}{
		{
			name:        "empty ids",
			setupEvents: []EventBlob{},
			checkIDs:    []string{},
			expectedMap: map[string]bool{},
		},
		{
			name:        "no events exist",
			setupEvents: []EventBlob{},
			checkIDs:    []string{"id1", "id2"},
			expectedMap: map[string]bool{"id1": false, "id2": false},
		},
		{
			name: "some exist",
			setupEvents: []EventBlob{
				{ID: []byte("id1"), JSON: []byte(`{"test":1}`)},
			},
			checkIDs: []string{"id1", "id2"},
			expectedMap: map[string]bool{
				"id1": true,
				"id2": false,
			},
		},
		{
			name: "all exist",
			setupEvents: []EventBlob{
				{ID: []byte("id1"), JSON: []byte(`{"test":1}`)},
				{ID: []byte("id2"), JSON: []byte(`{"test":2}`)},
			},
			checkIDs: []string{"id1", "id2"},
			expectedMap: map[string]bool{
				"id1": true,
				"id2": true,
			},
		},
		{
			name:        "bucket doesn't exist",
			setupEvents: []EventBlob{},
			checkIDs:    []string{"id1"},
			expectedMap: map[string]bool{}, // outputs empty map
			skipSetup:   true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := tempDB(t)

			if !tc.skipSetup {
				err := SetupBoltDB(db)
				require.NoError(t, err)

				if len(tc.setupEvents) > 0 {
					err = BatchWriteEvents(db, tc.setupEvents)
					require.NoError(t, err)
				}
			}

			result := BatchCheckEventsExist(db, tc.checkIDs)
			assert.Equal(t, tc.expectedMap, result)
		})
	}
}

func TestBatchWriteEvents(t *testing.T) {
	cases := []struct {
		name         string
		events       []EventBlob
		verifyID     string
		expectedJSON string
	}{
		{
			name:   "empty slice",
			events: []EventBlob{},
		},
		{
			name: "single event",
			events: []EventBlob{
				{ID: []byte("id1"), JSON: []byte(`{"test":1}`)},
			},
			verifyID:     "id1",
			expectedJSON: `{"test":1}`,
		},
		{
			name: "multiple events",
			events: []EventBlob{
				{ID: []byte("id1"), JSON: []byte(`{"test":1}`)},
				{ID: []byte("id2"), JSON: []byte(`{"test":2}`)},
				{ID: []byte("id3"), JSON: []byte(`{"test":3}`)},
			},
			verifyID:     "id2",
			expectedJSON: `{"test":2}`,
		},
		{
			name: "duplicate id overwrites",
			events: []EventBlob{
				{ID: []byte("id1"), JSON: []byte(`{"test":1}`)},
				{ID: []byte("id1"), JSON: []byte(`{"test":2}`)},
			},
			verifyID:     "id1",
			expectedJSON: `{"test":2}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := tempDB(t)

			err := SetupBoltDB(db)
			require.NoError(t, err)

			err = BatchWriteEvents(db, tc.events)
			assert.NoError(t, err)

			if tc.verifyID != "" {
				err = db.View(func(tx *bolt.Tx) error {
					bucket := tx.Bucket([]byte(BucketName))
					val := bucket.Get([]byte(tc.verifyID))
					assert.Equal(t, tc.expectedJSON, string(val))
					return nil
				})
				assert.NoError(t, err)
			}
		})
	}
}

func TestWriteThenCheck(t *testing.T) {
	db := tempDB(t)

	err := SetupBoltDB(db)
	require.NoError(t, err)

	events := []EventBlob{
		{ID: []byte("id1"), JSON: []byte(`{"test":1}`)},
		{ID: []byte("id2"), JSON: []byte(`{"test":2}`)},
		{ID: []byte("id3"), JSON: []byte(`{"test":3}`)},
	}

	err = BatchWriteEvents(db, events)
	require.NoError(t, err)

	checkIDs := []string{"id1", "id2", "id3"}
	result := BatchCheckEventsExist(db, checkIDs)

	for _, id := range checkIDs {
		assert.True(t, result[id], "expected %s to exist", id)
	}
}
