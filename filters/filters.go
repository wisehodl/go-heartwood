package filters

import (
	"encoding/json"
	"fmt"
	roots "git.wisehodl.dev/jay/go-roots/filters"
	"strings"
)

type HeartwoodFilter struct {
	Root  roots.Filter
	Graph []GraphFilter
}

type GraphFilter struct {
	IDs        []string
	Authors    []string
	Kinds      []json.RawMessage
	Since      json.RawMessage
	Until      json.RawMessage
	Limit      *int
	Tags       roots.TagFilters
	Distance   *Distance
	Graph      []GraphFilter
	Extensions roots.FilterExtensions
}

type Distance struct {
	Min int
	Max int
}

func MarshalJSON(f HeartwoodFilter) ([]byte, error) {
	legacyFilter := f.Root

	if f.Graph != nil {
		graphArray, err := marshalGraphArray(f.Graph)
		if err != nil {
			return nil, fmt.Errorf("error marshalling graph field: %w", err)
		}

		graphField, err := json.Marshal(graphArray)
		if err != nil {
			return nil, fmt.Errorf("error marshalling graph field: %w", err)
		}

		if legacyFilter.Extensions == nil {
			legacyFilter.Extensions = make(roots.FilterExtensions)
		}

		legacyFilter.Extensions["graph"] = graphField
	}

	return roots.MarshalJSON(legacyFilter)
}

func UnmarshalJSON(data []byte, f *HeartwoodFilter) error {
	var rootsFilter roots.Filter
	if err := roots.UnmarshalJSON(data, &rootsFilter); err != nil {
		return err
	}

	if rootsFilter.Extensions["graph"] != nil {
		graphArray, err := unmarshalGraphArray(rootsFilter.Extensions["graph"])
		if err != nil {
			return fmt.Errorf("error unmarshalling graph extension field: %w", err)
		}
		f.Graph = graphArray
		delete(rootsFilter.Extensions, "graph")
	}

	f.Root = rootsFilter

	return nil
}

func MarshalGraphJSON(f GraphFilter) ([]byte, error) {
	outputMap := make(map[string]interface{})

	// Add standard fields
	if f.IDs != nil {
		outputMap["ids"] = f.IDs
	}
	if f.Authors != nil {
		outputMap["authors"] = f.Authors
	}
	if f.Kinds != nil {
		outputMap["kinds"] = f.Kinds
	}
	if f.Since != nil {
		outputMap["since"] = f.Since
	}
	if f.Until != nil {
		outputMap["until"] = f.Until
	}
	if f.Limit != nil {
		outputMap["limit"] = *f.Limit
	}

	// Add distance field
	if f.Distance != nil {
		distanceMap := make(map[string]interface{})
		distanceMap["max"] = f.Distance.Max
		distanceMap["min"] = f.Distance.Min
		outputMap["distance"] = distanceMap
	}

	// Add nested graph field
	if f.Graph != nil {
		graphArray, err := marshalGraphArray(f.Graph)
		if err != nil {
			return nil, fmt.Errorf("error in nested graph: %w", err)
		}
		outputMap["graph"] = graphArray
	}

	// Add tags
	for key, values := range f.Tags {
		outputMap["#"+key] = values
	}

	// Merge extensions
	for key, raw := range f.Extensions {
		// Disallow standard keys in extensions
		if key == "ids" ||
			key == "authors" ||
			key == "kinds" ||
			key == "since" ||
			key == "until" ||
			key == "limit" ||
			key == "distance" ||
			key == "graph" {
			continue
		}

		// Disallow tag keys in extensions
		if strings.HasPrefix(key, "#") {
			continue
		}

		var extValue interface{}
		if err := json.Unmarshal(raw, &extValue); err != nil {
			return nil, err
		}
		outputMap[key] = extValue
	}

	return json.Marshal(outputMap)
}

func UnmarshalGraphJSON(data []byte, f *GraphFilter) error {
	// Decode into raw map
	raw := make(map[string]json.RawMessage)
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	// Extract standard fields
	if v, ok := raw["ids"]; ok {
		if err := json.Unmarshal(v, &f.IDs); err != nil {
			return err
		}
		delete(raw, "ids")
	}

	if v, ok := raw["authors"]; ok {
		if err := json.Unmarshal(v, &f.Authors); err != nil {
			return err
		}
		delete(raw, "authors")
	}

	if v, ok := raw["kinds"]; ok {
		if err := json.Unmarshal(v, &f.Kinds); err != nil {
			return err
		}
		delete(raw, "kinds")
	}

	if v, ok := raw["since"]; ok {
		var val json.RawMessage
		if err := json.Unmarshal(v, &val); err != nil {
			return err
		}
		f.Since = val
		delete(raw, "since")
	}

	if v, ok := raw["until"]; ok {
		var val json.RawMessage
		if err := json.Unmarshal(v, &val); err != nil {
			return err
		}
		f.Until = val
		delete(raw, "until")
	}

	if v, ok := raw["limit"]; ok {
		if len(v) == 4 && string(v) == "null" {
			f.Limit = nil
		} else {
			var val int
			if err := json.Unmarshal(v, &val); err != nil {
				return err
			}
			f.Limit = &val
		}
		delete(raw, "limit")
	}

	// Extract distance field
	if v, ok := raw["distance"]; ok {
		if err := json.Unmarshal(v, &f.Distance); err != nil {
			return err
		}
		delete(raw, "distance")
	}

	// Extract nested graph field
	if v, ok := raw["graph"]; ok {
		graphArray, err := unmarshalGraphArray(v)
		if err != nil {
			return fmt.Errorf("error unmarshalling nested graph filter: %w", err)
		}
		f.Graph = graphArray
		delete(raw, "graph")
	}

	// Extract tag fields
	for key := range raw {
		if strings.HasPrefix(key, "#") {
			// Leave Tags as `nil` unless tag fields exist
			if f.Tags == nil {
				f.Tags = make(roots.TagFilters)
			}
			tagKey := key[1:]
			var tagValues []string
			if err := json.Unmarshal(raw[key], &tagValues); err != nil {
				return err
			}
			f.Tags[tagKey] = tagValues
			delete(raw, key)
		}
	}

	// Place remaining fields in extensions
	if len(raw) > 0 {
		f.Extensions = raw
	}

	return nil
}

func marshalGraphArray(filters []GraphFilter) ([]json.RawMessage, error) {
	result := []json.RawMessage{}
	for _, f := range filters {
		b, err := MarshalGraphJSON(f)
		if err != nil {
			return nil, err
		}
		result = append(result, b)
	}
	return result, nil
}

func unmarshalGraphArray(raws json.RawMessage) ([]GraphFilter, error) {
	var rawArray []json.RawMessage
	if err := json.Unmarshal(raws, &rawArray); err != nil {
		return nil, err
	}
	var result []GraphFilter
	for _, raw := range rawArray {
		var f GraphFilter
		if err := UnmarshalGraphJSON(raw, &f); err != nil {
			return nil, err
		}
		result = append(result, f)
	}
	return result, nil
}
